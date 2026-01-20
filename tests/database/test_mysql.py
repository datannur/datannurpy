"""MySQL integration tests.

These tests require a MySQL server and are skipped by default.
Set TEST_MYSQL_URL environment variable to run them.

Run via: GitHub Actions > "Database Integration Tests" > "Run workflow"
"""

from __future__ import annotations

import os
from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest

from datannurpy.readers.database import connect

from .base import BaseDatabaseTests, BaseSchemaTests
from .conftest import (
    create_schema_tables,
    create_test_tables,
    drop_schema_tables,
    drop_test_tables,
)

if TYPE_CHECKING:
    import ibis


class TestMySQL(BaseDatabaseTests, BaseSchemaTests):
    """MySQL integration tests."""

    @pytest.fixture
    def mysql_url(self) -> str:
        url = os.environ.get("TEST_MYSQL_URL")
        if not url:
            pytest.skip("MySQL not available (set TEST_MYSQL_URL)")
        return url

    @pytest.fixture
    def db(
        self, mysql_url: str
    ) -> Generator[tuple[ibis.BaseBackend, str, str], None, None]:
        con, _ = connect(mysql_url)
        yield con, "mysql", "mysql"

    @pytest.fixture
    def db_with_employees(
        self, mysql_url: str
    ) -> Generator[tuple[ibis.BaseBackend, str, str], None, None]:
        """Create employees/departments tables in MySQL."""
        con, _ = connect(mysql_url)
        drop_test_tables(con)
        create_test_tables(con)
        yield con, "mysql", "mysql"
        drop_test_tables(con)

    @pytest.fixture
    def db_with_schemas(
        self, mysql_url: str
    ) -> Generator[tuple[ibis.BaseBackend, str], None, None]:
        """Create schemas and tables in MySQL."""
        con, _ = connect(mysql_url)
        drop_schema_tables(con, "mysql")
        create_schema_tables(con, "mysql")
        yield con, "mysql"
        drop_schema_tables(con, "mysql")
