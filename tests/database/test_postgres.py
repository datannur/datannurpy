"""PostgreSQL integration tests.

These tests require a PostgreSQL server and are skipped by default.
Set TEST_POSTGRES_URL environment variable to run them.

Run via: GitHub Actions > "Database Integration Tests" > "Run workflow"
"""

from __future__ import annotations

import os
from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest

from datannurpy.scanner.database import connect

from .base import BaseDatabaseTests, BaseSchemaTests
from .conftest import (
    create_schema_tables,
    create_test_tables,
    drop_schema_tables,
    drop_test_tables,
)

if TYPE_CHECKING:
    import ibis


class TestPostgreSQL(BaseDatabaseTests, BaseSchemaTests):
    """PostgreSQL integration tests."""

    @pytest.fixture(scope="class")
    def postgres_url(self) -> str:
        url = os.environ.get("TEST_POSTGRES_URL")
        if not url:
            pytest.skip("PostgreSQL not available (set TEST_POSTGRES_URL)")
        return url

    @pytest.fixture(scope="class")
    def _pg_con(self, postgres_url: str) -> Generator[ibis.BaseBackend, None, None]:
        con, _ = connect(postgres_url)
        yield con

    @pytest.fixture(scope="class")
    def db(
        self, _pg_con: ibis.BaseBackend
    ) -> Generator[tuple[ibis.BaseBackend, str, str], None, None]:
        yield _pg_con, "postgres", "postgres"

    @pytest.fixture(scope="class")
    def db_with_employees(
        self, _pg_con: ibis.BaseBackend
    ) -> Generator[tuple[ibis.BaseBackend, str, str], None, None]:
        """Create employees/departments tables in PostgreSQL."""
        drop_test_tables(_pg_con)
        create_test_tables(_pg_con)
        yield _pg_con, "postgres", "postgres"
        drop_test_tables(_pg_con)

    @pytest.fixture(scope="class")
    def db_with_schemas(
        self, _pg_con: ibis.BaseBackend
    ) -> Generator[tuple[ibis.BaseBackend, str], None, None]:
        """Create schemas and tables in PostgreSQL."""
        drop_schema_tables(_pg_con, "postgres")
        create_schema_tables(_pg_con, "postgres")
        yield _pg_con, "postgres"
        drop_schema_tables(_pg_con, "postgres")
