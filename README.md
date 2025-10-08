
# GHCNh Hourly → DuckDB Query Tool

Query NOAA's **Global Historical Climatology Network – hourly (GHCNh)** parquet files (served over HTTPS) directly with DuckDB.

## Features

- **Correct NOAA path with `/parquet/`**
  ```
  .../hourly/access/by-year/{year}/parquet/GHCNh_{STATION_ID}_{year}.parquet
  ```
- **Multi-year support**
  - Implicit: inferred from `--start` / `--end`
  - Explicit: `--years 2018-2020` or `--years 2017,2019,2021`
- **Robust timestamp inference**
  - TIMESTAMP column
  - `date+hour` combination
  - `year/month/day/hour`
  - Or override with `--ts-expr`
- **Station selection**
  - Metadata filters: `--name`, `--state`, `--contains`, `--bbox`, `--near` + `--radius-km`
  - Exact matches: `--wmo-id`, `--icao`
  - Ranking: `--top K` (return best K matches)
  - Auto-detects station ID column from CSV (`ghcn_id`, `station`, `station_id`, etc.)
- **Listing/inspection without dates**
  - `--list-stations-only` prints matched stations as CSV
  - `--print-station-columns` shows available CSV headers
- **Other options**
  - `--probe` skip missing yearly files via HTTP HEAD
  - `--print-schema` parquet schema before query
  - `--show-sql` preview generated SQL + inferred timestamp
  - CSV export with `--output`
  - Conditional `station_id` injection if included in `--columns`

## Requirements

- Python 3.8+
- [duckdb](https://duckdb.org/docs/installation/) Python package
```bash
pip install duckdb
```
- *(Optional)* [pandas](https://pandas.pydata.org/) for nicer console printing

## Station List

By default, the script uses the **official NOAA station list CSV**:

```
https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/doc/ghcnh-station-list.csv
```

You can override with a local file or another URL using `--station-file`.

## Usage

### A) Inspect stations

```bash
# Print all available columns in the station CSV
python query_ghcnh_clean.py --print-station-columns

# List top 5 stations containing "san diego"
python query_ghcnh_clean.py --contains "san diego" --top 5 --list-stations-only

# Nearest 5 stations within 50 km
python query_ghcnh_clean.py --near 32.7157 -117.1611 --radius-km 50 --top 5 --list-stations-only

# Exact WMO or ICAO match
python query_ghcnh_clean.py --wmo-id 72290 --list-stations-only
python query_ghcnh_clean.py --icao KSAN --list-stations-only
```

### B) Query data

```bash
# Query by substring and state
python query_ghcnh_clean.py   --state CA --contains "san diego"   --start 2020-01-01 --end 2020-01-03   --limit 50
```

```bash
# Explicit years with probing and export to CSV
python query_ghcnh_clean.py   --station USW00023188   --years 2018-2020   --start 2018-06-01 --end 2020-06-30   --probe --output out.csv
```

### C) Schema preview & SQL

```bash
python query_ghcnh_clean.py   --station USW00023188   --start 2021-06-01 --end 2021-06-02   --print-schema --show-sql
```

### D) Custom timestamp

```bash
python query_ghcnh_clean.py   --station USW00023188   --start 2020-05-01 --end 2020-05-02   --ts-expr "make_timestamp(year, month, day, hour, 0, 0)"
```

## Tips

- **No city/country in your CSV?** Use `--contains` or geographic filters (`--near`, `--bbox`).
- **Troubleshooting station match**: Add `--debug-station-select` to see diagnostics and suggestions.
- **Include station ID in output**: Add `station_id` to `--columns` to inject the selected ID into the results.
- **Skip missing years**: Use `--probe` if not every year is available.

## License

MIT
