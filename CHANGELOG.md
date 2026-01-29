# datannurpy

## unreleased

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
