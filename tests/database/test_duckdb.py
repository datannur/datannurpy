"""DuckDB backend tests."""

from __future__ import annotations

from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest

from .base import BaseDatabaseTests, BaseSchemaTests
from .conftest import create_test_tables

if TYPE_CHECKING:
    import ibis


class TestDuckDB(BaseDatabaseTests, BaseSchemaTests):
    """DuckDB tests."""

    @pytest.fixture
    def db(
        self, duckdb_with_schemas: ibis.BaseBackend
    ) -> Generator[tuple[ibis.BaseBackend, str, str], None, None]:
        yield duckdb_with_schemas, "duckdb", "duckdb"

    @pytest.fixture
    def db_with_employees(
        self,
    ) -> Generator[tuple[ibis.BaseBackend, str, str], None, None]:
        """Create DuckDB with employees/departments tables."""
        import ibis

        con = ibis.duckdb.connect(":memory:")
        create_test_tables(con)
        yield con, "duckdb", "duckdb"
        con.disconnect()

    @pytest.fixture
    def db_with_schemas(
        self, duckdb_with_schemas: ibis.BaseBackend
    ) -> Generator[tuple[ibis.BaseBackend, str], None, None]:
        """Provide DuckDB with schemas for BaseSchemaTests."""
        yield duckdb_with_schemas, "duckdb"
