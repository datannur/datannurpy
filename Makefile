.PHONY: test lint typecheck check download-app coverage test-cov

test:
	uv run pytest

test-cov:
	uv run pytest --cov=src/datannurpy --cov-report=xml

coverage:
	uv run pytest --cov=src/datannurpy --cov-report=term-missing --cov-report=html

lint:
	uv run ruff check . && uv run ruff format --check .

typecheck:
	uv run pyright src/datannurpy tests

check: lint typecheck test

download-app:
	uv run python scripts/download_app.py
