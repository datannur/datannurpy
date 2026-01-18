# Contributing

## Setup

```bash
git clone https://github.com/datannur/datannurpy.git
cd datannurpy
uv sync
make download-app  # fetch visualization app for bundling
```

## Development

```bash
make check        # lint + typecheck + tests (run before committing)
make test         # tests only
make lint         # ruff check + format
make typecheck    # pyright
```

## Pull Requests

1. Create a branch from `main`
2. Make changes with tests
3. Run `make check`
4. Submit PR

## Code Style

- Python 3.9+ (`from __future__ import annotations`)
- Formatting: ruff (automatic)
- Types: pyright standard mode
- Tests: pytest with `tmp_path` for file operations
