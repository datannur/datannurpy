# datannurpy

Python library for datannur catalog metadata management.

## Architecture

```
src/datannurpy/
├── catalog.py           # Catalog class (delegates to add_* and exporter)
├── add_folder.py        # Catalog.add_folder implementation
├── add_dataset.py       # Catalog.add_dataset implementation
├── add_database.py      # Catalog.add_database implementation
├── finalize.py          # Cleanup of entities not seen during scan
├── utils/               # Internal utilities (ids, log, modality, prefix)
├── entities/            # Dataclasses: Folder, Dataset, Variable, Modality, Value
├── scanner/             # File/DB scanning → Variables + stats
├── importer/            # db.py (load_db for incremental scan)
└── exporter/            # db.py (export_db), app.py (export_app)
```

## Public API

Main classes: `Catalog`, `Folder`

Methods: `add_folder`, `add_dataset`, `add_database`, `add_metadata`, `finalize`, `export_db`, `export_app`

Common options: `include`, `exclude`, `infer_stats`, `refresh`

See README for full API reference.

## ID Conventions

- Valid chars: `a-zA-Z0-9_, -` (space allowed)
- Separator: `---`

Example with `Folder(id="source")` scanning `data/sales.csv`:

- Dataset: `source---sales_csv`
- Variable: `source---sales_csv---amount`

### ID Helpers

- `sanitize_id(s)` → cleans string for use as ID
- `build_dataset_id(folder_id, dataset_name)` → `folder_id---sanitized_name`
- `build_variable_id(folder_id, dataset_name, var_name)` → `folder_id---dataset---var`

## Dev Commands

```bash
make check  # ruff + pyright + pytest
```

## Constraints

- Python 3.9+ (use `from __future__ import annotations`)
- pyright mode: standard
- Single-line docstrings (no multi-line Args/Returns)
- Ibis: prefer `.to_pyarrow()` over `.execute()` for better typing
