.PHONY: test lint typecheck check download-app

test:
	uv run pytest

lint:
	uv run ruff check . && uv run ruff format --check .

typecheck:
	uv run pyright src/datannurpy tests

check: lint typecheck test

download-app:
	uv run python scripts/download_app.py
