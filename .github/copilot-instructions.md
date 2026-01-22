# datannurpy

Python library for datannur catalog metadata management.

## Architecture

3 layers:

1. **Readers** (`src/datannurpy/readers/`): Ibis for CSV/Excel/Database scanning
2. **Entities** (`src/datannurpy/entities/`): dataclasses (Folder, Dataset, Variable, Modality, Value)
3. **Writers** (`src/datannurpy/writers/`): JSON stdlib for output (PyArrow for data conversion)

## Public API

- `Catalog.add_folder(path, folder=None)` → scans CSV/Excel/Parquet/SAS files (auto-detects Delta/Hive/Iceberg)
- `Catalog.add_dataset(path, folder=None)` → adds single file or partitioned directory (Delta/Hive/Iceberg)
- `Catalog.add_database(connection, folder=None)` → scans database tables
- `Catalog.write(output_dir)` → exports JSON + JSON.js files
- `Catalog.export_app(output_dir)` → exports full datannur app with data
- `Folder(id, name)` → optional, for custom folder metadata

Common options: `include`, `exclude`, `infer_stats`

### Parquet formats

Auto-detection: Delta (`_delta_log/`), Iceberg (`metadata/*.metadata.json`), Hive (`key=value/`)

Metadata extraction: Delta/Iceberg name, description, column docs

### Database connections

Supported connection strings:

- `sqlite:///path/to/db.sqlite`
- `postgresql://user:pass@host:port/database`
- `mysql://user:pass@host:port/database`
- `oracle://user:pass@host:port/service_name`
- `mssql://user:pass@host:port/database`

Database-specific options: `schema`, `sample_size`, `group_by_prefix`, `prefix_min_tables`

Install extras: `pip install datannurpy[postgres]`, `datannurpy[mysql]`, `datannurpy[oracle]`, `datannurpy[mssql]`, `datannurpy[stat]`

## ID Conventions

- Valid chars: `a-zA-Z0-9_, -` (space allowed)
- Separator: `---`

Example with `Folder(id="source")` scanning `data/sales.csv`:

- Dataset: `source---sales_csv`
- Variable: `source---sales_csv---amount`

## Dev Commands

```bash
make check  # ruff + pyright + pytest
```

## Constraints

- Python 3.9+ (use `from __future__ import annotations`)
- pyright mode: standard
- Single-line docstrings (no multi-line Args/Returns)
- Ibis: prefer `.to_pyarrow()` over `.execute()` for better typing
