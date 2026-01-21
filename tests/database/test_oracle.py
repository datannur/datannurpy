"""Oracle integration tests.

These tests require an Oracle server and are skipped by default.
Set TEST_ORACLE_URL environment variable to run them.

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


class TestOracle(BaseDatabaseTests, BaseSchemaTests):
    """Oracle integration tests."""

    @pytest.fixture
    def oracle_url(self) -> str:
        url = os.environ.get("TEST_ORACLE_URL")
        if not url:
            pytest.skip("Oracle not available (set TEST_ORACLE_URL)")
        return url

    @pytest.fixture
    def db(
        self, oracle_url: str
    ) -> Generator[tuple[ibis.BaseBackend, str, str], None, None]:
        con, _ = connect(oracle_url)
        yield con, "oracle", "oracle"

    @pytest.fixture
    def db_with_employees(
        self, oracle_url: str
    ) -> Generator[tuple[ibis.BaseBackend, str, str], None, None]:
        """Create employees/departments tables in Oracle."""
        con, _ = connect(oracle_url)
        drop_test_tables(con, "oracle")
        create_test_tables(con, "oracle")
        yield con, "oracle", "oracle"
        drop_test_tables(con, "oracle")

    @pytest.fixture
    def db_with_schemas(
        self, oracle_url: str
    ) -> Generator[tuple[ibis.BaseBackend, str], None, None]:
        """Create schemas and tables in Oracle."""
        con, _ = connect(oracle_url)
        drop_schema_tables(con, "oracle")
        create_schema_tables(con, "oracle")
        yield con, "oracle"
        drop_schema_tables(con, "oracle")
