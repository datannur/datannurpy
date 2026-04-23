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
The Makefile handles Python dependency installation automatically (`uv sync --extra databases`).

**Prerequisites (one-time):**

```bash
# Container runtime
brew install orbstack              # lightweight Docker alternative using Apple Virtualization.framework

# Native client libraries
brew install mysql-client          # required to build mysqlclient Python package
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18  # ODBC driver for SQL Server

# Add mysql-client pkg-config path to ~/.zshrc
export PKG_CONFIG_PATH="/opt/homebrew/opt/mysql-client/lib/pkgconfig"
```

**Usage:**

```bash
make test-db          # installs DB drivers, starts containers, runs tests
make test-db-all      # same, plus Oracle 18
make test-db-down     # stop and remove containers
```

## Demo Publication

Manual, 2-step workflow for publishing the hybrid demo (local `data/` scan + editorial metadata from the datannur front repo) to a remote server via SSH.

**One-time setup:**

```bash
make download-app         # fetches the front repo (also preserves db-source/)
make demo                 # scan + export to examples/output_editorial/
make demo-setup           # npm install + playwright chromium (~90 MB)
make demo-configs-init    # creates templates in examples/datannur_app/configs/
# then edit examples/datannur_app/configs/{deploy,static-make,llm-web}.config.json
```

**Regular workflow:**

```bash
make demo           # refresh scan + local preview (opens browser)
make demo-publish   # inject configs + prerender + deploy via SSH
```

The configs folder is gitignored (single source of truth for editorial content stays in the front repo).

## Documentation Site

User-facing documentation lives in [`docs/`](docs/) (VitePress) and is published to [docs.datannur.com/builder](https://docs.datannur.com/builder/). Update the relevant page in `docs/` when adding or changing user-visible features.

**Requires Node.js >= 22.6.0** (native TypeScript support).

```bash
cd docs
npm ci                   # install dependencies (first time)
npm run docs:dev         # VitePress dev server
npm run docs:build       # build docs site
npm run docs:preview     # preview built docs
npm run docs:deploy      # deploy to docs.datannur.com/builder/
npm run docs:release     # build + deploy
```

Structure:

- `docs/index.md` — Getting Started (entry point)
- `docs/*.md` — One page per section (flat layout)
- `docs/.vitepress/config.ts` — site config (nav, sidebar, theme)
- `docs/public/` — static assets (logo, favicon)
- `docs/deploy/` — deploy script and config

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
