# datannurpy

Python library for datannur catalog metadata management.

## Architecture

3 layers:

1. **Readers** (`src/datannurpy/readers/`): Ibis for CSV/Excel/Database scanning
2. **Entities** (`src/datannurpy/entities/`): dataclasses (Folder, Dataset, Variable)
3. **Writers** (`src/datannurpy/writers/`): JSON stdlib for output (PyArrow for data conversion)

## Public API

- `Catalog.add_folder(path, folder=None)` → scans files and adds to catalog
- `Catalog.add_database(connection, folder=None)` → scans database tables and adds to catalog
- `Catalog.write(output_dir)` → exports JSON + JSON.js files only
- `Catalog.export_app(output_dir, open_browser=False)` → exports full datannur app with data
- `Folder(id, name)` → optional, for custom folder metadata

Internal entities in `src/datannurpy/entities/`.

### Database connections

Supported connection strings:

- `sqlite:///path/to/db.sqlite`
- `postgresql://user:pass@host:port/database`
- `mysql://user:pass@host:port/database`
- `oracle://user:pass@host:port/service_name`

Options:

- `schema`: specific schema to scan (postgres/mysql)
- `include/exclude`: table name patterns (supports wildcards)
- `sample_size`: limit rows for stats on large tables

Install extras: `pip install datannurpy[postgres]`, `datannurpy[mysql]`, `datannurpy[oracle]`

SQLite works out of the box (uses Python's built-in sqlite3).

## App Bundling

The datannur visualization app is bundled in `src/datannurpy/app/` (gitignored).
To download/update: `make download-app`

## ID Conventions

- Valid chars: `a-zA-Z0-9_, -` (space allowed)
- Separator: `---`

Example with `Folder(id="source")` scanning `data/sales.csv`:

- Dataset: `source---sales_csv`
- Variable: `source---sales_csv---amount`

With subdirectory `data/2024/sales.csv`:

- Folder: `source---2024`
- Dataset: `source---2024---sales_csv`
- Variable: `source---2024---sales_csv---amount`

## Dev Commands

```bash
make check  # ruff + pyright + pytest
```

## Constraints

- Python 3.9+ (use `from __future__ import annotations`)
- pyright mode: standard
- Ibis typing: prefer `.to_pyarrow()` methods over `.execute()` for better type inference
