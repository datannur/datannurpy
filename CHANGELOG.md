# datannurpy

## 0.10.2 (2026-03-23)

- fix: introduce `ConfigError` for user-facing errors — CLI no longer masks internal bugs (`ValueError`/`FileNotFoundError` from libraries) and now catches `TypeError` from parameter typos
- fix: `last_update_date` for database tables is `None` on first scan instead of current date — populated only when a change is detected
- add: `add_folder` and `add_dataset` accept a list of paths, `add_database` accepts a list of schemas — shared options apply to all entries

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
