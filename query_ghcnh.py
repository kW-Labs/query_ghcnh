#!/usr/bin/env python3
"""
Query NOAA GHCNh hourly parquet files (served over HTTPS) for a given station and date range using DuckDB.

This clean version includes:
- Correct NOAA path: .../hourly/access/by-year/{year}/parquet/GHCNh_{STATION}_{year}.parquet
- Multi-year selection via --years or inferred from --start/--end
- Optional --probe to skip missing yearly files (HTTP HEAD)
- Station selection by metadata from a CSV or the official station list URL
  (name/state/substring, proximity/bbox; choose best match or list candidates)
- Direct ID filters: --wmo-id, --icao
- Robust timestamp inference (TIMESTAMP column OR date+hour OR year/month/day/hour)
- CSV export, schema print, SQL preview
- Listing/inspection commands work without --start/--end

Official station list (default for --station-file):
https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/doc/ghcnh-station-list.csv
"""
import argparse
import datetime as dt
import sys
from typing import Iterable, List, Tuple, Set, Dict, Optional

try:
    import duckdb  # type: ignore
except Exception as e:
    print("This script requires the 'duckdb' package. Install with: pip install duckdb", file=sys.stderr)
    raise

import csv
import io
from pathlib import Path

try:
    from urllib.request import Request, urlopen  # stdlib
except Exception:
    urlopen = None  # type: ignore

DEFAULT_BASE = "https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/by-year"
DEFAULT_STATION_CSV = "https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/doc/ghcnh-station-list.csv"

CANDIDATE_TS_COLS = [
    "timestamp", "time", "datetime", "date_time", "obs_time", "observation_time",
    "valid", "DATE"
]

STATION_ID_CAND_COLS = [
    "ghcn_id", "ghcnh_id", "station", "station_id", "id", "ghcn"
]

STATION_META_PRINT_ORDER = [
    "name","station_name","station","id","ghcn_id","ghcnh_id","state","wmo_id","icao",
    "latitude","lat","longitude","lon","elevation","elev","begin","end"
]

# ----------------- small utils -----------------
def str_ci(s: Optional[str]) -> str:
    return (s or "").strip().casefold()

def try_float(s: Optional[str]) -> Optional[float]:
    if s is None or s == "":
        return None
    try:
        return float(s)
    except Exception:
        return None

def haversine_km(lat1, lon1, lat2, lon2) -> float:
    from math import radians, sin, cos, asin, sqrt
    R = 6371.0088
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    return R * c

def fuzzy_ratio(a: str, b: str) -> float:
    import difflib
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()

# ----------------- years/urls -----------------
def parse_years_arg(arg: str) -> List[int]:
    years: Set[int] = set()
    for part in arg.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            ya, yb = int(a), int(b)
            if yb < ya:
                ya, yb = yb, ya
            years.update(range(ya, yb + 1))
        else:
            years.add(int(part))
    out = sorted(years)
    if not out:
        raise ValueError("No valid years parsed from --years")
    return out

def years_from_window(start: dt.datetime, end: dt.datetime) -> List[int]:
    return list(range(start.year, end.year + 1))

def build_urls(station: str, years: Iterable[int], base: str = DEFAULT_BASE) -> List[str]:
    # Correct path includes /parquet/
    return [f"{base}/{y}/parquet/GHCNh_{station}_{y}.parquet" for y in years]

def parquet_list_sql(urls: List[str]) -> str:
    quoted = ", ".join([f"'{u}'" for u in urls])
    return f"[{quoted}]"

def head_ok(url: str, timeout: float = 10.0) -> bool:
    if urlopen is None:
        return True  # best-effort
    try:
        req = Request(url, method="HEAD")
        with urlopen(req, timeout=timeout) as resp:
            code = getattr(resp, "status", 200)
            return 200 <= code < 400
    except Exception:
        return False

# ----------------- schema/timestamp -----------------
def detect_timestamp_expr(con: "duckdb.DuckDBPyConnection", urls: List[str]) -> Tuple[str, str]:
    plist = parquet_list_sql(urls)
    desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet({plist}) LIMIT 0").fetchdf()
    cols = {row['column_name']: row['column_type'] for _, row in desc.iterrows()}
    lower_map = {name.lower(): (name, coltype) for name, coltype in cols.items()}

    for cand in CANDIDATE_TS_COLS:
        if cand.lower() in lower_map:
            real, coltype = lower_map[cand.lower()]
            if "TIMESTAMP" in coltype.upper() or "DATETIME" in coltype.upper():
                return real, f"direct:{real} ({coltype})"

    if "date" in lower_map and "hour" in lower_map:
        date_col, date_type = lower_map["date"]
        hour_col, hour_type = lower_map["hour"]
        if "DATE" in date_type.upper():
            ts_expr = f"(CAST({date_col} AS TIMESTAMP) + {hour_col} * INTERVAL 1 HOUR)"
            return ts_expr, f"date+hour:{date_col}+{hour_col}"

    needed = ["year", "month", "day", "hour"]
    if all(k in lower_map for k in needed):
        y, _ = lower_map["year"]
        m, _ = lower_map["month"]
        d, _ = lower_map["day"]
        h, _ = lower_map["hour"]
        ts_expr = f"make_timestamp({y}, {m}, {d}, {h}, 0, 0)"
        return ts_expr, f"ymdh:{y},{m},{d},{h}"

    if "date" in lower_map:
        date_col, date_type = lower_map["date"]
        if "TIMESTAMP" in date_type.upper():
            return date_col, f"direct:{date_col} ({date_type})"

    for cand in ["timestamp", "time", "datetime", "date_time", "obs_time", "observation_time", "valid"]:
        if cand in lower_map:
            real, coltype = lower_map[cand]
            return f"CAST({real} AS TIMESTAMP)", f"cast:{real} ({coltype})"

    raise RuntimeError(
        "Could not infer a timestamp expression. Use --ts-expr (e.g. make_timestamp(year,month,day,hour,0,0))."
    )

# ----------------- station CSV helpers -----------------
def read_station_csv(path_or_url: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    try:
        if path_or_url.lower().startswith("http://") or path_or_url.lower().startswith("https://"):
            if urlopen is None:
                raise RuntimeError("urllib not available to fetch remote station CSV")
            req = Request(path_or_url, method="GET")
            with urlopen(req, timeout=30) as resp:
                text = resp.read().decode("utf-8", errors="replace")
            f = io.StringIO(text)
            reader = csv.DictReader(f)
        else:
            p = Path(path_or_url)
            if not p.exists():
                return rows
            reader = csv.DictReader(p.open(newline="", encoding="utf-8"))
        for r in reader:
            norm = { (k or "").strip().lower(): (v or "").strip() for k, v in r.items() }
            rows.append(norm)
    except Exception as e:
        print(f"Failed to read station CSV {path_or_url}: {e}", file=sys.stderr)
    return rows

def station_headers(station_file: str) -> List[str]:
    rows = read_station_csv(station_file)
    return list(rows[0].keys()) if rows else []

def best_station_id_column(headers_lower: List[str]) -> Optional[str]:
    for cand in STATION_ID_CAND_COLS:
        if cand in headers_lower:
            return cand
    for h in headers_lower:
        if "station" in h or (h.endswith("_id") or h == "id"):
            return h
    return None

def lookup_station_meta(station: str, station_file: Optional[str]) -> Optional[Dict[str, str]]:
    if not station_file:
        return None
    rows = read_station_csv(station_file)
    if not rows:
        return None
    headers = list(rows[0].keys())
    id_col = best_station_id_column(headers)
    if not id_col:
        return None
    target = station.strip().lower()
    for row in rows:
        if row.get(id_col, "").strip().lower() == target:
            return row
    return None

def print_station_meta(meta: Dict[str, str]) -> None:
    print("== Station Info ==", file=sys.stderr)
    printed = set()
    for k in STATION_META_PRINT_ORDER:
        if k in meta and meta[k]:
            print(f"{k.title()}: {meta[k]}", file=sys.stderr)
            printed.add(k)
    extra = 0
    for k, v in meta.items():
        if k in printed or not v:
            continue
        print(f"{k}: {v}", file=sys.stderr)
        extra += 1
        if extra >= 10:
            break
    print("==================", file=sys.stderr)

def collect_station_candidates(station_file: str) -> list:
    return read_station_csv(station_file)

def select_stations_from_csv(station_file: str,
                             name: Optional[str] = None,
                             city: Optional[str] = None,
                             state: Optional[str] = None,
                             country: Optional[str] = None,
                             contains: Optional[str] = None,
                             bbox: Optional[List[float]] = None,
                             near: Optional[List[float]] = None,
                             radius_km: Optional[float] = None,
                             top: int = 1) -> list:
    rows = collect_station_candidates(station_file)
    if not rows:
        return []

    headers = list(rows[0].keys())
    id_col = best_station_id_column(headers) or "id"

    name_ci = str_ci(name)
    city_ci = str_ci(city)
    state_ci = str_ci(state)
    country_ci = str_ci(country)
    contains_ci = str_ci(contains)

    text_like = [k for k in headers if any(tok in k for tok in ["name","station","city","state","country","location","site"])]
    if not text_like:
        text_like = headers

    filtered = []
    for r in rows:
        blob = " ".join([str_ci(r.get(k)) for k in text_like])

        if city_ci:
            if "city" in r and r.get("city"):
                if city_ci not in str_ci(r.get("city")):
                    continue
            else:
                if city_ci not in blob:
                    continue
        if state_ci:
            if "state" in r and r.get("state"):
                if state_ci not in str_ci(r.get("state")):
                    continue
            else:
                if state_ci not in blob:
                    continue
        if country_ci:
            if "country" in r and r.get("country"):
                if country_ci not in str_ci(r.get("country")):
                    continue
            else:
                if country_ci not in blob:
                    continue

        if bbox is not None:
            if len(bbox) != 4:
                continue
            min_lat, min_lon, max_lat, max_lon = bbox
            lat = try_float(r.get("latitude") or r.get("lat"))
            lon = try_float(r.get("longitude") or r.get("lon"))
            if lat is None or lon is None:
                continue
            if not (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
                continue

        if contains_ci and contains_ci not in blob:
            continue

        filtered.append(r)

    if not filtered:
        return []

    scored = []
    target_lat = target_lon = None
    if near is not None and len(near) == 2:
        target_lat, target_lon = float(near[0]), float(near[1])

    for r in filtered:
        lat = try_float(r.get("latitude") or r.get("lat"))
        lon = try_float(r.get("longitude") or r.get("lon"))
        name_val = r.get("name") or r.get("station_name") or r.get("station") or ""
        name_score = 0.0
        if name_ci:
            if name_ci in str_ci(name_val):
                name_score = 1.0
            else:
                name_score = fuzzy_ratio(str_ci(name_val), name_ci)

        dist_km = None
        if target_lat is not None and target_lon is not None and lat is not None and lon is not None:
            dist_km = haversine_km(target_lat, target_lon, lat, lon)

        if radius_km is not None and dist_km is not None and dist_km > radius_km:
            continue

        score = 0.0
        if dist_km is not None:
            score += max(0.0, 1.0 - (dist_km / 1000.0))
        score += name_score

        scored.append((score, dist_km if dist_km is not None else float("inf"), name_score, r))

    if not scored:
        return []

    scored.sort(key=lambda t: (-t[0], t[1], -t[2]))
    return [r for (_, _, _, r) in scored[:max(1, top)]]

# ----------------- main -----------------
def main():
    ap = argparse.ArgumentParser(description="Query NOAA GHCNh hourly parquet files with DuckDB (multi-year + station selection).")
    ap.add_argument("--station", required=False, help="GHCN station ID (e.g., USW00023188). Optional if using station selectors below.")
    ap.add_argument("--start", required=False, help="Start datetime (YYYY-MM-DD or ISO-8601)")
    ap.add_argument("--end", required=False, help="End datetime (YYYY-MM-DD or ISO-8601)")
    ap.add_argument("--years", default=None, help="Explicit years to read (e.g. '2018-2020' or '2017,2019,2021')")
    ap.add_argument("--probe", action="store_true", help="HEAD-check and skip missing yearly files (404s)")
    ap.add_argument("--base-url", default=DEFAULT_BASE, help="Base HTTPS path for by-year parquet directory")
    ap.add_argument("--columns", default="*", help="Comma-separated list of columns to select (default '*'). Include 'station_id' to inject the selected station ID as a column.")
    ap.add_argument("--limit", type=int, default=None, help="Optional LIMIT")
    ap.add_argument("--ts-expr", default=None, help="Override timestamp expression (DuckDB SQL)")
    ap.add_argument("--output", default=None, help="Write results to CSV at this path")
    ap.add_argument("--show-sql", action="store_true", help="Print the generated SQL before running")
    ap.add_argument("--station-file", default=DEFAULT_STATION_CSV, help="Path or URL to station CSV (defaults to the official NOAA list)")
    ap.add_argument("--print-schema", action="store_true", help="Print the inferred schema from parquet before querying")

    # Station selection filters
    ap.add_argument("--name", default=None, help="Station name (exact/substring match, case-insensitive)")
    ap.add_argument("--city", default=None, help="City (case-insensitive)")
    ap.add_argument("--state", default=None, help="State/region code (case-insensitive)")
    ap.add_argument("--country", default=None, help="Country code (e.g., US)")
    ap.add_argument("--contains", default=None, help="Substring to search across name/city/state/country")
    ap.add_argument("--bbox", nargs=4, type=float, default=None, metavar=("MIN_LAT","MIN_LON","MAX_LAT","MAX_LON"), help="Bounding box filter")
    ap.add_argument("--near", nargs=2, type=float, default=None, metavar=("LAT","LON"), help="Proximity center (lat lon)")
    ap.add_argument("--radius-km", type=float, default=None, help="Proximity radius in kilometers (used with --near)")
    ap.add_argument("--top", type=int, default=1, help="Return top-K matching stations (by proximity/name score). Default 1")
    ap.add_argument("--list-stations-only", action="store_true", help="List matching stations (CSV to stdout) and exit (no data query)")
    ap.add_argument("--wmo-id", default=None, help="Match station by exact WMO ID (column: wmo_id)")
    ap.add_argument("--icao", default=None, help="Match station by exact ICAO (column: icao)")
    ap.add_argument("--debug-station-select", action="store_true", help="Print station CSV diagnostics and selection reasoning")
    ap.add_argument("--print-station-columns", action="store_true", help="Print available station CSV columns and exit")

    args = ap.parse_args()

    # If only listing or printing columns, don't require start/end
    listing_only = bool(args.list_stations_only or args.print_station_columns)

    if not listing_only:
        if not args.start or not args.end:
            print("--start and --end are required for data queries. For listing-only operations, you can omit them.", file=sys.stderr)
            return
    else:
        # Harmless defaults make downstream parsing simple
        if not args.start: args.start = "2000-01-01"
        if not args.end: args.end = "2000-01-02"

    # Early exit: print station columns
    if args.print_station_columns:
        cols = station_headers(args.station_file)
        if cols:
            print("Available station CSV columns:")
            for c in cols:
                print(f"- {c}")
        else:
            print("No station columns found (failed to load station CSV).")
        return

    # Parse start/end if present
    try:
        start = dt.datetime.fromisoformat(args.start)
        end = dt.datetime.fromisoformat(args.end)
    except Exception as e:
        print(f"Failed to parse start/end: {e}", file=sys.stderr)
        sys.exit(2)

    if end < start:
        print("End datetime is before start datetime.", file=sys.stderr)
        sys.exit(2)

    # Station selection / lookup
    if not args.station:
        if not any([args.name, args.city, args.state, args.country, args.contains,
                    args.bbox, args.near, args.wmo_id, args.icao]):
            print("Provide a station ID via --station or at least one selector "
                  "(--state, --contains, --near, --wmo-id, --icao).", file=sys.stderr)
            sys.exit(2)

        bbox_vals = args.bbox if args.bbox is not None else None
        near_vals = args.near if args.near is not None else None

        matches: List[Dict[str, str]] = []

        # Exact ID matches first (WMO/ICAO)
        if args.wmo_id or args.icao:
            rows = collect_station_candidates(args.station_file)
            exact = []
            for r in rows:
                ok = True
                if args.wmo_id and str_ci(r.get("wmo_id")) != str_ci(args.wmo_id):
                    ok = False
                if args.icao and str_ci(r.get("icao")) != str_ci(args.icao):
                    ok = False
                if ok:
                    exact.append(r)
            if exact:
                matches = exact[:args.top]
        else:
            # Fallback to fuzzy/geo/text matching
            matches = select_stations_from_csv(
                station_file=args.station_file,
                name=args.name,
                city=args.city,
                state=args.state,
                country=args.country,
                contains=args.contains,
                bbox=bbox_vals,
                near=near_vals,
                radius_km=args.radius_km,
                top=args.top
            )

        if not matches:
            if args.debug_station_select:
                cols = station_headers(args.station_file)
                print("No stations matched. Diagnostics:", file=sys.stderr)
                print(f"- Loaded columns: {cols}", file=sys.stderr)
                print(f"- Filters: name={args.name!r}, city={args.city!r}, "
                      f"state={args.state!r}, country={args.country!r}, "
                      f"contains={args.contains!r}, bbox={args.bbox}, near={args.near}, "
                      f"radius_km={args.radius_km}, wmo_id={args.wmo_id!r}, "
                      f"icao={args.icao!r}", file=sys.stderr)
                print('- Tip: try --contains "<city or station snippet>" '
                      'or a geographic filter like --near LAT LON', file=sys.stderr)
            print("No stations matched the provided filters.", file=sys.stderr)
            sys.exit(3)

        headers = list(matches[0].keys())
        id_col = best_station_id_column(headers) or "id"

        if args.list_stations_only:
            print(",".join(headers))
            for r in matches:
                print(",".join([str(r.get(h, "")) for h in headers]))
            return

        # Use best match (first) for query
        best = matches[0]
        args.station = best.get(id_col)
        print("Selected station:", args.station, file=sys.stderr)
        print_station_meta(best)
    else:
        # If station id given, still print meta if available
        meta = lookup_station_meta(args.station, args.station_file)
        if meta:
            print_station_meta(meta)

    # Determine years to read
    if args.years:
        try:
            yrs = parse_years_arg(args.years)
        except Exception as e:
            print(f"Invalid --years: {e}", file=sys.stderr)
            sys.exit(2)
    else:
        yrs = years_from_window(start, end)

    urls = build_urls(args.station, yrs, base=args.base_url)

    if args.probe:
        existing = [u for u in urls if head_ok(u)]
        missing = [u for u in urls if u not in existing]
        if not existing:
            print("No existing parquet files found for the requested years/station (after probe).", file=sys.stderr)
            sys.exit(3)
        if missing:
            print(f"Skipping {len(missing)} missing file(s) due to --probe.", file=sys.stderr)
        urls = existing

    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute("PRAGMA enable_progress_bar=false;")

    plist = parquet_list_sql(urls)

    if args.print_schema:
        # Show columns and types to stderr
        desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet({plist}) LIMIT 0").fetchdf()
        print("== Parquet Schema ==", file=sys.stderr)
        try:
            import pandas as pd
            with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 200):
                print(desc.to_string(index=False), file=sys.stderr)
        except Exception:
            for _, row in desc.iterrows():
                print(f"{row['column_name']}: {row['column_type']}", file=sys.stderr)
        print("====================", file=sys.stderr)

    if args.ts_expr:
        ts_expr = args.ts_expr
        how = "user-specified"
    else:
        ts_expr, how = detect_timestamp_expr(con, urls)

    cols = args.columns.strip()
    user_cols = [c.strip() for c in cols.split(',')]

    if "*" in user_cols:
        # don't inject additional columns if selecting *
        pass
    else:
        new_cols = []
        for c in user_cols:
            if c.lower() == 'station_id':
                new_cols.append(f"'{args.station}' AS {c}")
            else:
                new_cols.append(c)
        cols = ", ".join(new_cols)

    where = f"({ts_expr}) BETWEEN TIMESTAMP '{start.isoformat(sep=' ')}' AND TIMESTAMP '{end.isoformat(sep=' ')}'"

    sql = f"""
    WITH src AS (
        SELECT * FROM read_parquet({plist})
    )
    SELECT {cols}
    FROM src
    WHERE {where}
    ORDER BY {ts_expr}
    """
    if args.limit:
        sql += f"\nLIMIT {int(args.limit)}"

    if args.show_sql:
        print("-- Years:", ",".join(str(y) for y in yrs), file=sys.stderr)
        print("-- Timestamp inference:", how, file=sys.stderr)
        print(sql)

    try:
        res = con.execute(sql).fetchdf()
    except Exception as e:
        print("Query failed:", e, file=sys.stderr)
        sys.exit(1)

    if args.output:
        res.to_csv(args.output, index=False)
        print(f"Wrote {len(res)} rows to {args.output}")
    else:
        with pd_option_context() as _:
            print(res.to_string(index=False, max_rows=50, max_cols=200, justify='left'))


from contextlib import contextmanager
@contextmanager
def pd_option_context():
    try:
        import pandas as pd
        with pd.option_context('display.max_rows', 50, 'display.max_columns', 200, 'display.width', 200):
            yield
    except ImportError:
        yield

if __name__ == "__main__":
    main()
