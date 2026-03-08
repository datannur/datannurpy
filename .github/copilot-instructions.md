# datannurpy

Python library for datannur catalog metadata management.

## Architecture

```
src/datannurpy/
├── catalog.py           # Catalog class (delegates to add_* and exporter)
├── add_folder.py        # Catalog.add_folder implementation
├── add_dataset.py       # Catalog.add_dataset implementation
├── add_database.py      # Catalog.add_database implementation
├── add_metadata.py      # Catalog.add_metadata implementation
├── finalize.py          # Cleanup of entities not seen during scan
├── exporter.py          # export_db, export_app
├── schema.py            # Dataclasses: Folder, Dataset, Variable, Modality, Value
├── utils/               # Internal utilities (ids, log, modality, prefix, folder, time)
├── scanner/             # File/DB scanning → Variables + stats
└── app/                 # Web app template files (copied during export)
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

## Development

Uses **uv** for dependency management and **Makefile** for common tasks.

```bash
uv sync                  # Install/update dependencies
uv run make check        # Run all checks (ruff + pyright + pytest with 100% coverage)
uv run make test         # Run pytest only
uv run pytest -k "test_name" -x  # Run specific test
```

## Constraints

- Python 3.9+ (use `from __future__ import annotations`)
- pyright mode: standard
- 100% test coverage required
- Single-line docstrings (no multi-line Args/Returns)
- Ibis: prefer `.to_pyarrow()` over `.execute()` for better typing
- Optional dependencies (deltalake, pyiceberg, pyreadstat): import inline only when used
