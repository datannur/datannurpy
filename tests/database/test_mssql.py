"""SQL Server integration tests.

These tests require a SQL Server instance and are skipped by default.
Set TEST_MSSQL_URL environment variable to run them.

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


class TestMSSQL(BaseDatabaseTests, BaseSchemaTests):
    """SQL Server integration tests."""

    @pytest.fixture
    def mssql_url(self) -> str:
        url = os.environ.get("TEST_MSSQL_URL")
        if not url:
            pytest.skip("SQL Server not available (set TEST_MSSQL_URL)")
        return url

    @pytest.fixture
    def db(
        self, mssql_url: str
    ) -> Generator[tuple[ibis.BaseBackend, str, str], None, None]:
        con, _ = connect(mssql_url)
        yield con, "mssql", "mssql"

    @pytest.fixture
    def db_with_employees(
        self, mssql_url: str
    ) -> Generator[tuple[ibis.BaseBackend, str, str], None, None]:
        """Create employees/departments tables in SQL Server."""
        con, _ = connect(mssql_url)
        drop_test_tables(con, "mssql")
        create_test_tables(con, "mssql")
        yield con, "mssql", "mssql"
        drop_test_tables(con, "mssql")

    @pytest.fixture
    def db_with_schemas(
        self, mssql_url: str
    ) -> Generator[tuple[ibis.BaseBackend, str], None, None]:
        """Create schemas and tables in SQL Server."""
        con, _ = connect(mssql_url)
        drop_schema_tables(con, "mssql")
        create_schema_tables(con, "mssql")
        yield con, "mssql"
        drop_schema_tables(con, "mssql")
