# datannurpy

## 0.14.2 (2026-04-06)

- security: SSH tunnels now use `WarningPolicy` instead of `AutoAddPolicy` — prevents silent MITM key acceptance
- security: Oracle SQL queries use bind variables (`:name`) instead of string interpolation
- security: non-Oracle SQL introspection queries use `_quote()` escaping for safe interpolation
- security: GitHub Actions pinned by commit SHA across all workflows
- fix: unified type strings — floating-point types now consistently return `"float"` instead of `"number"` (aligns with frontend `varTypes`)
- refactor: replaced `pyarrow_type_to_str` with `ibis.Schema.from_pyarrow` + `ibis_type_to_str` — single type conversion path
- perf: shared test fixtures, lazy pandas imports, xdist tuned to 4 workers

## 0.14.1 (2026-04-05)

- perf: batch database introspection — one set of system catalog queries per schema instead of per table
- fix: MySQL connection now forwards extra URL parameters (SSL options, charset, etc.) to the driver
- fix: MySQL `CAST(... AS DOUBLE)` replaced with implicit float conversion for MySQL 5.x compatibility
- fix: database folder name and ID no longer include URL query parameters (e.g. `?ssl_mode=DISABLED`)
- fix: credentials and query parameters stripped from `data_path` in exported database folders
- fix: MySQL PK introspection no longer leaks across tables (constraint_name is always `PRIMARY`)
- fix: clearer error message when database connection fails
- fix: database credentials with special characters (`@`, `#`, `$`, etc.) now handled correctly across connection, tunnel, and folder naming

## 0.14.0 (2026-04-04)

- add: `add_database` automatically extracts structural metadata (primary keys, foreign keys, table/column comments, constraint tags) from system catalogs when `depth="schema"` or `"full"`
- add: `ssh_tunnel` parameter on `add_database` for databases behind firewalls (paramiko-based)
- add: `time_series` parameter on `add_database` — groups temporal tables (e.g. `stats_2023`, `stats_2024`) into a single dataset with period tracking per variable
- add: time series prefix grouping now uses effective table list — series datasets are placed in the correct prefix folder instead of year-specific subfolders
- fix: `ibis_type_to_str` now maps `dt.Binary` to `"binary"` — MySQL `BINARY(n)`, `VARBINARY`, `BLOB`, `LONGBLOB` were reported as `"unknown"`
- fix: frequency table `ibis.union` fallback to `pa.concat_tables` for MySQL mixed collations
- fix: modality value/freq IDs now use hash-based keys instead of `sanitize_id` — eliminates ID collisions for values with special characters (JSON, URLs with `$@~`)
- fix: YAML config `log_file` path now resolved relative to config file directory
- fix: `ibis_type_to_str` now resolves `dt.Unknown` columns with known `raw_type` (e.g. MySQL `DOUBLE` → `"float"`, `TINYINT`/`MEDIUMINT` → `"integer"`) — previously reported as `"unknown"` with no stats

## 0.13.3 (2026-04-03)

- fix: YAML config now enforces `add_metadata` execution after all other `add` entries, ensuring manual metadata always overrides auto-scanned values

## 0.13.2 (2026-04-02)

- add: skip non-dataset Excel files (reports, pivots, merged cells) via header validation

## 0.13.1 (2026-04-02)

- fix: time series period detection now works at group level — folder dates (e.g. `old_2024_08/data_2018.csv`) no longer override file dates, and constant dates in identifiers are ignored
- fix: Oracle sampling now uses native `SAMPLE(pct)` clause — `ibis.random()` was evaluated once as a scalar, returning all or no rows
- fix: Oracle tables no longer scanned twice when connected user's schema is already listed — `get_schemas_to_scan` now deduplicates via `SELECT USER FROM DUAL`
- fix: Oracle `data_size` falls back to `user_segments` when `all_segments` returns NULL (insufficient privileges)
- fix: `log_file` now captures all log levels (warnings, skip, progress, summary) — previously only errors were written to the file
- fix: `depth: "schema"` now sets variable types for SAS/SPSS/Stata files — previously all types were `null` (local and remote)
- fix: Excel mixed-type columns now preserve `NaN` as missing instead of converting to `"nan"` string
- fix: nulls excluded from frequency tables — `nb_missing` already carries that information

## 0.13.0 (2026-03-31)

- add: `data_size` field on `Dataset` — data size in bytes (files, database tables, and partitioned datasets)
- refactor: SAS/SPSS/Stata scanner rewritten with streaming Parquet pipeline — constant RAM regardless of file size, fixes OOM on large files (10 Go+)
- add: `sample_size` support for statistical files (SAS/SPSS/Stata) — same reservoir sampling as CSV
- add: `sample_size=100_000` default on `Catalog` — all methods inherit it, override per-method with an int or disable with `None`
- add: `sample_size` support for Parquet, Delta, Hive, and Iceberg — DuckDB reservoir sampling, same pattern as CSV/databases
- rename: `skip_copy` → `csv_skip_copy` (applies only to CSV)

## 0.12.1 (2026-03-31)

- add: `verbose` and `log_file` parameters on `Catalog` for error diagnostics (full tracebacks on stderr or to file)
- fix: scanner errors now use `log_error` instead of `log_warn` — consistent error reporting with traceback support
- fix: Iceberg scan now works regardless of the caller's working directory

## 0.12.0 (2026-03-29)

- remove: `datannurpy` console_scripts entry point — use `python -m datannurpy` instead
- add: CLI supports `--help` and `--version` flags
- fix: ID conflict when scanning same database with different schemas in separate `add_database` calls
- fix: Oracle DATE/TIMESTAMP columns now get `nb_distinct`/`nb_missing` stats from `build_variables`
- fix: remote file warnings now show original filename instead of temporary path
- fix: skip columns with empty names from trailing CSV separators or blank Excel headers
- fix: remote CSV schema-only scan streams header line instead of fixed 4KB partial download
- refactor: remove Oracle date/timestamp stats raw SQL fallback — date columns are now fully skipped for stats on Oracle (simplifies sampling support)
- add: `sample_size` parameter on `add_database` for uniform random sampling on large tables — exact stats on full table, cardinality and freq on sample
- refactor: CSV scanner rewritten with DuckDB streaming — constant RAM regardless of file size, automatic UTF-8/cp1252 handling
- add: `sample_size` on `add_folder` / `add_dataset` and `csv_skip_copy` on `Catalog` / `add_folder` / `add_dataset` for CSV files

## 0.11.0 (2026-03-26)

- add: `app_config` parameter on `Catalog` — pass a `dict[str, str]` to populate a `config.json` in the exported database (e.g. `contact_email`, `banner`, `more_info`)

## 0.10.2 (2026-03-23)

- add: `add_folder` and `add_dataset` accept a list of paths, `add_database` accepts a list of schemas — shared options apply to all entries
- add: `env:` section in YAML config for reusable non-sensitive variables (expanded alongside `env_file` and `.env`)
- fix: Oracle date/timestamp stats computed via raw SQL — bypasses ibis `ExtractEpochSeconds` unsupported operation
- fix: skip `std` computation when a single distinct value, skip all stats when all values are missing
- fix: introduce `ConfigError` for user-facing errors — CLI no longer masks internal bugs (`ValueError`/`FileNotFoundError` from libraries) and now catches `TypeError` from parameter typos
- fix: `last_update_date` for database tables is `None` on first scan instead of current date — populated only when a change is detected

## 0.10.1 (2026-03-22)

- add: new `"geometry"` variable type for spatial columns (POINT, POLYGON, etc. from GeoPackage, PostGIS, etc.)
- fix: Excel date-only columns (all values at midnight) now correctly detected as `"date"` instead of `"datetime"`

## 0.10.0 (2026-03-22)

- add: `min`, `max`, `mean`, `std` statistics on Variable (numeric, string length, date as epoch seconds)
- add: graceful fallback when loading a stale/incompatible database emits a warning and starts fresh instead of crashing
- fix: `refresh=True` now skips loading the existing database entirely
- fix: date statistics no longer overflow on dates before 1901 (epoch_seconds cast to int64)

## 0.9.8 (2026-03-20)

- fix: remote `depth="schema"` SAS/Stata scan no longer fails — use pandas streaming instead of partial download (pyreadstat needs full file)
- fix: remote `depth="schema"` SPSS scan falls back to full download (no streaming support)
- fix: remote `depth="schema"` Excel xlsx reads only headers via openpyxl streaming (no full download)

## 0.9.7 (2026-03-20)

- fix: Oracle crash on connection close (`DPY-1001: not connected`) — `close_connection` now ignores errors when internal connection is already closed
- fix: `depth="structure"` for `add_database` no longer runs `COUNT(*)` and schema introspection on every table — just enumerates tables like `add_folder`

## 0.9.6 (2026-03-20)

- add: validate unknown parameters with closest match suggestion on all public methods
- add: scan continues on file/table error instead of crashing — errors logged and reported in summary
- fix: Excel files with mixed-type columns (`int`, `bytes`, etc.) no longer crash — fallback converts `object` columns to `str`
- fix: `depth="schema"` Excel scan now reads headers only (`nrows=0`) instead of loading entire file
- fix: `depth="schema"` no longer assigns fake `type="string"` to CSV/Excel variables — uses `None` when type is unknown

## 0.9.5 (2026-03-19)

- fix: Oracle < 23 compatibility (`ORA-00923`) — use `con.sql()` instead of `con.table()` to avoid boolean expressions unsupported before Oracle 23
- ci: test Oracle on both 18c and 23c via matrix strategy

## 0.9.4 (2026-03-19)

- add: `.env` file support for YAML config (expand `$VAR` / `${VAR}` in all values, optional `env_file` key)
- fix: add missing Oracle system schemas to exclusion list
- fix: remote SFTP/S3 scans returning 0 datasets on Windows (`Path()` converting `/` to `\`)

## 0.9.3 (2026-03-17)

- add: Oracle TNS name support via `oracle_client_path` parameter in `add_database()` (thick mode)
- fix: CSV scanner now uses Polars instead of DuckDB for cross-platform encoding support (Windows CP1252)
- chore: CI now tests minimum and maximum dependency versions

## 0.9.2 (2026-03-16)

- refactor: replace `warnings.warn` with quiet-aware `log_warn` and clean CLI errors
- chore: bump jsonjsdb to >=0.8.3

## 0.9.1 (2026-03-16)

- fix: add missing `fsspec` explicit dependency for remote storage
- fix: auto-exclude common non-data dirs (`.git`, `.venv`, `__pycache__`, `.ipynb_checkpoints`, etc.) and `~$*` temp files from scans

## 0.9.0 (2026-03-12)

- add: YAML configuration support with `run_config()`
- add: CLI with `python -m datannurpy config.yml`
- add: `datannurpy` entry point for venv usage
- add: `type: dataset` support in YAML config

## 0.8.0 (2026-03-12)

- add: `time_series` parameter for `add_folder` to group temporal files (e.g., `data_2020.csv`, `data_2021.csv`) into single datasets
- add: `nb_files` field on Dataset schema for time series file count
- add: variable period tracking (`start_date`, `end_date`) based on presence across periods

## 0.7.0 (2026-03-08)

- add: remote storage support for `add_folder`, `add_dataset`, `add_database`
- add: `storage_options` parameter for SFTP/S3/Azure/GCS credentials
- add: `add_database` now supports remote SQLite/GeoPackage files
- add: `depth="schema"` optimization for remote files (minimal downloads)

## 0.6.0 (2026-03-08)

- add: `depth` parameter for `add_dataset` and `add_metadata`
- refactor: `depth="structure"` now clears variable/modality/value/freq at load time

## 0.5.0 (2026-03-07)

- add: evolution cascade filtering via parent_relations (jsonjsdb 0.8.2)
- add: Value.id and Freq.id are now runtime fields (not persisted)
- fix: evolution stability - no spurious changes on successive runs
- fix: preserve last_update_timestamp for unchanged database tables
- fix: \_modalities folder no longer deleted on incremental scan
- refactor: \_compute_runtime_id helper for id column computation

## 0.4.2 (2026-03-05)

- add: jsonjsdb as runtime dependency
- refactor: migrate internal storage to jsonjsdb
- remove: `entities/` module (consolidated into `schema.py`)
- remove: `importer/db.py` and `exporter/db.py` (replaced by jsonjsdb)

## 0.4.1 (2026-02-01)

- add: e2e test with demo data and db export
- add: incrmental scan with db import

## 0.4.0 (2026-01-29)

- add: manual metadata merging with add_metadata

## 0.3.2 (2026-01-29)

- add: test coverage with codecov and improve test coverage to 100%
- fix: Iceberg scanner no longer requires network access (replaced DuckDB extension with PyIceberg)
- refactor: remove all methodes from catalog.py and rename writer to exporter
- refactor: remove \_ prefix and close db and file connection

## 0.3.1 (2026-01-25)

- add: progress logging with quiet option
- change: improve error messages
- fix: Excel reader no longer requires network access (removed DuckDB spatial dependency)
- fix: resolve relative path before opening browser in export_app
- fix: csv encoding fallback to handle utf-8, cp1252 and iso-8859-1
- refactor: make catalog.py as facade

## 0.3.0 (2026-01-22)

- add: modality supportou
- add: folder based on db table prefix
- add: support for parquet files and Delta/Hive/Iceberg directories
- add: support for SAS, SPSS and Stata files
- add: mssql db support
- add: gpkg support (sqlite based)

## 0.2.1 (2026-01-21)

- add: oracle db support

## 0.2.0 (2026-01-20)

- add: db scan support and use ibis instead of polars

## 0.1.3 (2026-01-19)

- fix: include visualization app in package

## 0.1.2 (2026-01-19)

- change: use static Python version badge in README

## 0.1.1 (2026-01-19)

- add: Python version classifiers for PyPI badges

## 0.1.0 (2026-01-19)

- initial release
