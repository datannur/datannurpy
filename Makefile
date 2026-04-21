.PHONY: test lint typecheck check download-app coverage test-cov update-snapshots test-db test-db-all test-db-oracle18 test-db-setup test-db-up test-db-down audit demo demo-setup demo-publish demo-configs-init

DEMO_CONFIG   := examples/demo_editorial.yml
DEMO_OUT      := examples/output_editorial
DEMO_CONFIGS  := examples/datannur_app/configs
DEMO_CFG_FILES := deploy.config.json static-make.config.json llm-web.config.json

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
	@uv run ruff check . && uv run ruff format --check . & LINT_PID=$$!; \
	uv run pyright src/datannurpy tests & TYPE_PID=$$!; \
	wait $$LINT_PID || exit 1; \
	wait $$TYPE_PID || exit 1
	uv run pytest --cov=src/datannurpy --cov-report=term-missing --cov-report=xml --cov-report=html --cov-fail-under=100

download-app:
	uv run python scripts/download_app.py

audit:
	uv export --no-dev --no-hashes | uv run --with pip-audit pip-audit -r /dev/stdin

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

# Demo publication (manual, 2-step)
#
#   1. make demo              → scan + export to $(DEMO_OUT), opens in browser
#   2. make demo-setup        → one-shot: npm install + playwright chromium
#   3. make demo-publish      → inject configs + prerender + deploy via SSH
#
# Configs (deploy, static-make, llm-web) live in $(DEMO_CONFIGS) (gitignored).
# Bootstrap them once with `make demo-configs-init`, then edit manually.

demo:
	uv run python -m datannurpy $(DEMO_CONFIG)

demo-setup:
	@test -f $(DEMO_OUT)/package.json || { echo "Run 'make demo' first"; exit 1; }
	cd $(DEMO_OUT) && npm install && npx playwright install chromium

demo-configs-init:
	@test -d $(DEMO_OUT)/data-template || { echo "Run 'make demo' first to get the templates"; exit 1; }
	@mkdir -p $(DEMO_CONFIGS)
	@for f in $(DEMO_CFG_FILES); do \
		if [ ! -f $(DEMO_CONFIGS)/$$f ]; then \
			cp $(DEMO_OUT)/data-template/$$f $(DEMO_CONFIGS)/$$f; \
			echo "Created $(DEMO_CONFIGS)/$$f — edit it with your values"; \
		else \
			echo "Exists   $(DEMO_CONFIGS)/$$f"; \
		fi; \
	done

demo-publish:
	@test -d $(DEMO_OUT) || { echo "Run 'make demo' first"; exit 1; }
	@test -d $(DEMO_OUT)/node_modules || { echo "Run 'make demo-setup' first"; exit 1; }
	@test -f $(DEMO_CONFIGS)/deploy.config.json || { echo "Missing configs — run 'make demo-configs-init' and fill in $(DEMO_CONFIGS)/"; exit 1; }
	@for f in $(DEMO_CFG_FILES); do \
		if [ -f $(DEMO_CONFIGS)/$$f ]; then \
			cp $(DEMO_CONFIGS)/$$f $(DEMO_OUT)/data/$$f; \
			echo "Injected $$f"; \
		fi; \
	done
	cd $(DEMO_OUT) && npm run static-deploy
