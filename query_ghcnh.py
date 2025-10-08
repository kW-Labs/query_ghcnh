#!/usr/bin/env python3

"""
Query NOAA GHCNh hourly parquet files (hosted over HTTPS) for a given station and date range using DuckDB.
Multi-year support, optional probe for missing yearly files, and a station-lookup helper.

Examples:
  python query_ghcnh_duckdb_v3.py --station USW00023188 --start 2019-12-25 --end 2020-01-05

  python query_ghcnh_duckdb_v3.py --station USW00023188 --years 2018-2020 \
    --start 2018-06-01 --end 2020-06-30 --probe --output out.csv

  python query_ghcnh_duckdb_v3.py --station USW00023188 --start 2021-06-01 --end 2021-06-02 --show-sql

  python query_ghcnh_duckdb_v3.py --station USW00023188 --start 2020-05-01 --end 2020-05-02 \
    --ts-expr "make_timestamp(year, month, day, hour, 0, 0)"

Station lookup:
  python query_ghcnh_duckdb_v3.py --station USW00023188 --start 2020-01-01 --end 2020-01-02 \
    --station-file ghcnh-station-list.csv
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
from pathlib import Path

try:
    from urllib.request import Request, urlopen  # stdlib
except Exception:
    urlopen = None  # type: ignore

DEFAULT_BASE = "https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/by-year"

CANDIDATE_TS_COLS = [
    "timestamp", "time", "datetime", "date_time", "obs_time", "observation_time",
    "valid", "DATE"
]

STATION_ID_CAND_COLS = [
    "ghcn_id", "station", "station_id", "id", "ghcnh_id", "ghcn", "wban", "usaf"
]

STATION_META_PRINT_ORDER = [
    "name","station_name","station","id","ghcn_id","ghcnh_id","country","state","latitude","lat","longitude","lon","elevation","elev","begin","end"
]

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
    return [f"{base}/{y}/parquet/GHCNh_{station}_{y}.parquet" for y in years]

def parquet_list_sql(urls: List[str]) -> str:
    quoted = ", ".join([f"'{u}'" for u in urls])
    return f"[{quoted}]"

def head_ok(url: str, timeout: float = 10.0) -> bool:
    if urlopen is None:
        return True  # best-effort when sandboxed
    try:
        req = Request(url, method="HEAD")
        with urlopen(req, timeout=timeout) as resp:
            code = getattr(resp, "status", 200)
            return 200 <= code < 400
    except Exception:
        return False

def detect_timestamp_expr(con: "duckdb.DuckDBPyConnection", urls: List[str]) -> Tuple[str, str]:
    plist = parquet_list_sql(urls)
    desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet({plist}) LIMIT 0").fetchdf()
    cols = {row['column_name']: row['column_type'] for _, row in desc.iterrows()}
    lower_map = {name.lower(): (name, coltype) for name, coltype in cols.items()}

    # Direct timestamp-like
    for cand in CANDIDATE_TS_COLS:
        if cand.lower() in lower_map:
            real, coltype = lower_map[cand.lower()]
            if "TIMESTAMP" in coltype.upper() or "DATETIME" in coltype.upper():
                return real, f"direct:{real} ({coltype})"

    # DATE + HOUR
    if "date" in lower_map and "hour" in lower_map:
        date_col, date_type = lower_map["date"]
        hour_col, hour_type = lower_map["hour"]
        if "DATE" in date_type.upper():
            ts_expr = f"(CAST({date_col} AS TIMESTAMP) + {hour_col} * INTERVAL 1 HOUR)"
            return ts_expr, f"date+hour:{date_col}+{hour_col}"

    # YEAR,MONTH,DAY,HOUR
    needed = ["year", "month", "day", "hour"]
    if all(k in lower_map for k in needed):
        y, _ = lower_map["year"]
        m, _ = lower_map["month"]
        d, _ = lower_map["day"]
        h, _ = lower_map["hour"]
        ts_expr = f"make_timestamp({y}, {m}, {d}, {h}, 0, 0)"
        return ts_expr, f"ymdh:{y},{m},{d},{h}"

    # 'date' typed as TIMESTAMP
    if "date" in lower_map:
        date_col, date_type = lower_map["date"]
        if "TIMESTAMP" in date_type.upper():
            return date_col, f"direct:{date_col} ({date_type})"

    # CAST stringy time columns
    for cand in ["timestamp", "time", "datetime", "date_time", "obs_time", "observation_time", "valid"]:
        if cand in lower_map:
            real, coltype = lower_map[cand]
            return f"CAST({real} AS TIMESTAMP)", f"cast:{real} ({coltype})"

    raise RuntimeError(
        "Could not infer a timestamp expression. Use --ts-expr (e.g. make_timestamp(year,month,day,hour,0,0))."
    )

def read_station_csv(path: Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    if not path.exists():
        return rows
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            # normalize keys to lowercase
            norm = { (k or "").strip().lower(): (v or "").strip() for k, v in r.items() }
            rows.append(norm)
    return rows

def best_station_id_column(headers_lower: List[str]) -> Optional[str]:
    for cand in STATION_ID_CAND_COLS:
        if cand in headers_lower:
            return cand
    # look for columns that look like *station* or *id*
    for h in headers_lower:
        if "station" in h or (h.endswith("_id") or h == "id"):
            return h
    return None

def lookup_station_meta(station: str, station_file: Optional[str]) -> Optional[Dict[str, str]]:
    if not station_file:
        return None
    path = Path(station_file)
    rows = read_station_csv(path)
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
    # print any remaining non-empty fields up to a limit to avoid noise
    extra = 0
    for k, v in meta.items():
        if k in printed or not v:
            continue
        print(f"{k}: {v}", file=sys.stderr)
        extra += 1
        if extra >= 10:
            break
    print("==================", file=sys.stderr)

def main():
    ap = argparse.ArgumentParser(description="Query NOAA GHCNh hourly parquet files with DuckDB (multi-year + station lookup).")
    ap.add_argument("--station", required=True, help="GHCN station ID (e.g., USW00023188)")
    ap.add_argument("--start", required=True, help="Start datetime (YYYY-MM-DD or ISO-8601)")
    ap.add_argument("--end", required=True, help="End datetime (YYYY-MM-DD or ISO-8601)")
    ap.add_argument("--years", default=None, help="Explicit years to read (e.g. '2018-2020' or '2017,2019,2021')")
    ap.add_argument("--probe", action="store_true", help="HEAD-check and skip missing yearly files (404s)")
    ap.add_argument("--base-url", default=DEFAULT_BASE, help="Base HTTPS path for by-year parquet directory")
    ap.add_argument("--columns", default="*", help="Comma-separated list of columns to select (default '*')")
    ap.add_argument("--limit", type=int, default=None, help="Optional LIMIT")
    ap.add_argument("--ts-expr", default=None, help="Override timestamp expression (DuckDB SQL)")
    ap.add_argument("--output", default=None, help="Write results to CSV at this path")
    ap.add_argument("--show-sql", action="store_true", help="Print the generated SQL before running")
    ap.add_argument("--station-file", default=None, help="Path to a station CSV (e.g., ghcnh-station-list.csv) for metadata lookup")
    ap.add_argument("--print-schema", action="store_true", help="Print the inferred schema from parquet before querying")

    args = ap.parse_args()

    try:
        start = dt.datetime.fromisoformat(args.start)
        end = dt.datetime.fromisoformat(args.end)
    except Exception as e:
        print(f"Failed to parse start/end: {e}", file=sys.stderr)
        sys.exit(2)

    if end < start:
        print("End datetime is before start datetime.", file=sys.stderr)
        sys.exit(2)

    # Station lookup (if provided)
    meta = lookup_station_meta(args.station, args.station_file)
    if meta:
        print_station_meta(meta)
    elif args.station_file:
        print(f"(No station metadata found in {args.station_file} for ID '{args.station}')", file=sys.stderr)

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
        # When user selects all columns, we don't inject the station ID.
        # The original Station_ID column from the parquet file will be used (which is null).
        pass
    else:
        new_cols = []
        for c in user_cols:
            if c.lower() == 'station_id':
                # If user explicitly requests station_id, inject the value from the --station arg.
                # Preserve the user's casing for the alias.
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
