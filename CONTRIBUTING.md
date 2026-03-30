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

## Database Integration Tests (optional, macOS only)

Tests PostgreSQL, MySQL, SQL Server and Oracle via Docker containers.

**Prerequisites (one-time):**

```bash
# Container runtime
brew install orbstack              # lightweight Docker alternative using Apple Virtualization.framework

# Native client libraries
brew install mysql-client          # required to build mysqlclient Python package
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18  # ODBC driver for SQL Server

# Add to ~/.zshrc
export PKG_CONFIG_PATH="/opt/homebrew/opt/mysql-client/lib/pkgconfig"

# Python database drivers
uv sync --extra databases
```

**Usage:**

```bash
make test-db       # starts containers, runs tests, keeps containers up
make test-db-down  # stop and remove containers
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
