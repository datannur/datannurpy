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

    @pytest.fixture
    def postgres_url(self) -> str:
        url = os.environ.get("TEST_POSTGRES_URL")
        if not url:
            pytest.skip("PostgreSQL not available (set TEST_POSTGRES_URL)")
        return url

    @pytest.fixture
    def db(
        self, postgres_url: str
    ) -> Generator[tuple[ibis.BaseBackend, str, str], None, None]:
        con, _ = connect(postgres_url)
        yield con, "postgres", "postgres"

    @pytest.fixture
    def db_with_employees(
        self, postgres_url: str
    ) -> Generator[tuple[ibis.BaseBackend, str, str], None, None]:
        """Create employees/departments tables in PostgreSQL."""
        con, _ = connect(postgres_url)
        drop_test_tables(con)
        create_test_tables(con)
        yield con, "postgres", "postgres"
        drop_test_tables(con)

    @pytest.fixture
    def db_with_schemas(
        self, postgres_url: str
    ) -> Generator[tuple[ibis.BaseBackend, str], None, None]:
        """Create schemas and tables in PostgreSQL."""
        con, _ = connect(postgres_url)
        drop_schema_tables(con, "postgres")
        create_schema_tables(con, "postgres")
        yield con, "postgres"
        drop_schema_tables(con, "postgres")
