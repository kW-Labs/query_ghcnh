# GHCNh Hourly → DuckDB Query Tool

Query NOAA's **Global Historical Climatology Network – hourly (GHCNh)** parquet files (served over HTTPS) directly with DuckDB. Supports multi-year reads, date-window filtering, optional file existence probing, and station metadata lookup.

## Features

- **Remote parquet over HTTPS** (no AWS credentials needed) via DuckDB `httpfs`
- **Multi-year selection**
  - Implicit from `--start/--end`
  - Or explicit via `--years 2018-2020` or `--years 2017,2019,2021`
- **Date filtering** with an inferred or user-specified timestamp
- **Robust timestamp detection** (tries TIMESTAMP columns, `date+hour`, or `year/month/day/hour`)
- **Conditional Station ID injection**: If you include `station_id` in the `--columns` argument, the script will populate it with the station ID from the `--station` argument.
- **Probe** `--probe` to skip missing yearly files (HTTP HEAD)
- **Station lookup** from a CSV list to print station name/lat/lon/etc.
- **CSV export** and optional schema printout

## Requirements

- Python 3.8+
- `duckdb` Python package

```bash
pip install duckdb
```

*(Optional)* To preview DataFrames nicely in the console, `pandas` is supported but not required.

## Data Location

This tool reads by-year parquet files from NOAA. Expected pattern (corrected):
```
https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/by-year/{year}/parquet/GHCNh_{STATION_ID}_{year}.parquet
```

> If your deployment uses a different base URL, use `--base-url` to override.

## Usage

Basic query across a date window (years inferred from the window):

```bash
python query_ghcnh.py   --station USW00023188   --start 2019-12-25   --end 2020-01-05
```

Explicit multi-year selection (date filter still applied):

```bash
python query_ghcnh.py   --station USW00023188   --years 2018-2020   --start 2018-06-01   --end 2020-06-30
```

Non‑contiguous years, skip missing files, and export to CSV:

```bash
python query_ghcnh.py   --station USW00023188   --years 2017,2019,2021   --start 2017-01-01   --end 2021-12-31   --probe   --output out.csv
```

Show generated SQL and inferred timestamp:

```bash
python query_ghcnh.py   --station USW00023188   --start 2021-06-01   --end 2021-06-02   --show-sql
```

Force a specific timestamp expression if your schema is unusual:

```bash
python query_ghcnh.py   --station USW00023188   --start 2020-05-01   --end 2020-05-02   --ts-expr "make_timestamp(year, month, day, hour, 0, 0)"
```

Print parquet schema before running and use a station list for metadata:

```bash
python query_ghcnh.py   --station USW00023188   --start 2020-01-01   --end 2020-01-02   --print-schema   --station-file ghcnh-station-list.csv
```

## Station Lookup

Pass `--station-file` with a CSV containing station metadata (e.g., columns like `ghcn_id`, `station`, `id`, `name`, `latitude`, `longitude`, `elevation`, etc.).
The script will print a short station summary to **stderr** before querying.

Minimal example CSV header:

```csv
ghcn_id,name,latitude,longitude,elevation,country,state,begin,end
USW00023188,San Diego Intl AP,32.733,-117.183,5,US,CA,1939-01-01,2025-01-01
```

## Options

- `--station` (str, required): GHCN station ID (e.g., `USW00023188`)
- `--start` / `--end` (ISO8601): Date/time window
- `--years` (str): Explicit year list/ranges (e.g., `2017,2019-2021`)
- `--probe` (flag): Skip missing yearly files by checking with HTTP HEAD
- `--base-url` (str): Override base path if needed
- `--columns` (str): Comma-separated selection (defaults to `*`). If you include `station_id` in the list of columns, it will be populated with the value from the `--station` argument.
- `--limit` (int): LIMIT rows
- `--ts-expr` (str): Custom DuckDB timestamp expression
- `--output` (path): Write results to CSV
- `--show-sql` (flag): Print generated SQL and timestamp inference
- `--print-schema` (flag): Print parquet schema
- `--station-file` (path): CSV for station metadata lookup

## Troubleshooting

- **Timestamp could not be inferred**: Pass `--ts-expr`, e.g. `make_timestamp(year,month,day,hour,0,0)` or `CAST(time AS TIMESTAMP)`.
- **HTTP 404 on some years**: Use `--probe` to skip missing yearly files.
- **Slow queries**: Narrow your date window with `--start/--end`, select fewer columns via `--columns`, or LIMIT results.
- **Different schema**: Use `--print-schema` to inspect columns and types.

## License

MIT