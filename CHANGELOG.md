# datannurpy

## 0.9.3 (2026-03-17)

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
