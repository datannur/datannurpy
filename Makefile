.PHONY: test lint typecheck check download-app coverage test-cov update-snapshots test-db test-db-all test-db-oracle18 test-db-setup test-db-up test-db-down

test:
	uv run pytest

test-cov:
	uv run pytest --cov=src/datannurpy --cov-report=term-missing --cov-report=xml --cov-report=html --cov-fail-under=100

update-snapshots:
	UPDATE_SNAPSHOTS=1 uv run pytest tests/test_e2e_snapshot.py -v

coverage:
	uv run pytest --cov=src/datannurpy --cov-report=term-missing --cov-report=html

lint:
	uv run ruff check . && uv run ruff format --check .

typecheck:
	uv run pyright src/datannurpy tests

check:
	@uv run ruff check . && uv run ruff format --check . &
	@uv run pyright src/datannurpy tests &
	@wait
	uv run pytest --cov=src/datannurpy --cov-report=term-missing --cov-report=xml --cov-report=html --cov-fail-under=100

download-app:
	uv run python scripts/download_app.py

test-db-setup:
	uv sync --extra databases

test-db-up:
	@command -v orbctl >/dev/null && orbctl start || true
	docker compose -f docker-compose.test.yml --env-file /dev/null up -d --wait
	@docker exec datannurpy-mssql /opt/mssql-tools18/bin/sqlcmd \
		-S localhost -U sa -P 'Test@123!' -C \
		-Q "IF DB_ID('testdb') IS NULL CREATE DATABASE testdb"

test-db-down:
	docker compose -f docker-compose.test.yml --env-file /dev/null down -v

test-db: test-db-setup test-db-up
	@echo "=== PostgreSQL ==="
	TEST_POSTGRES_URL=postgresql://test:test@localhost:15432/testdb \
	uv run pytest tests/database/test_postgres.py -v -n 0
	@echo "=== MySQL ==="
	TEST_MYSQL_URL=mysql://root:test@localhost:13306/testdb \
	uv run pytest tests/database/test_mysql.py -v -n 0
	@echo "=== SQL Server ==="
	TEST_MSSQL_URL='mssql://sa:Test@123!@localhost:11433/testdb?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes' \
	uv run pytest tests/database/test_mssql.py -v -n 0
	@echo "=== Oracle 23 ==="
	TEST_ORACLE_URL=oracle://system:test@localhost:11523/FREEPDB1 \
	uv run pytest tests/database/test_oracle.py -v -n 0

test-db-oracle18: test-db-up
	@echo "=== Oracle 18 ==="
	TEST_ORACLE_URL=oracle://system:test@localhost:11518/XEPDB1 \
	uv run pytest tests/database/test_oracle.py -v -n 0

test-db-all: test-db test-db-oracle18
