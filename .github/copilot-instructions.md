# datannurpy

Python library for datannur catalog metadata management.

## Architecture

3 layers:

1. **Readers** (`src/datannurpy/readers/`): Polars for CSV/Excel scanning
2. **Entities** (`src/datannurpy/entities/`): dataclasses (Folder, Dataset, Variable)
3. **Writers** (`src/datannurpy/writers/`): JSON stdlib for output

## Public API

- `Catalog.add_folder(path, folder=None)` → scans and adds to catalog
- `Catalog.write(output_dir)` → exports JSON + JSON.js files only
- `Catalog.export_app(output_dir, open_browser=False)` → exports full datannur app with data
- `Folder(id, name)` → optional, for custom folder metadata

Internal entities in `src/datannurpy/entities/`.

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
- Polars typing issues: use `# pyright: ignore[reportCallIssue]` when needed
