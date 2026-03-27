"""DuckDB backend tests."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from datannurpy import Catalog, Folder

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


class TestDuckDBIncrementalScan:
    """Test incremental scan for DuckDB with multiple schemas."""

    def test_rescan_marks_existing_schema_folders_as_seen(
        self, duckdb_with_schemas: ibis.BaseBackend, tmp_path: Path
    ) -> None:
        """Rescanning database should mark existing schema folders as _seen=True."""
        app_dir = tmp_path

        # First scan - no schema specified, will scan all (sales, inventory)
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_database(
            duckdb_with_schemas,
            Folder(id="db", name="Database"),
        )
        catalog1.export_db()

        # Should have schema folders (sales, inventory)
        schema_folders = [f for f in catalog1.folder.all() if f.type == "schema"]
        assert len(schema_folders) >= 2  # at least sales and inventory

        # Reload and rescan
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_database(
            duckdb_with_schemas,
            Folder(id="db", name="Database"),
        )
        catalog2.finalize()

        # All schema folders should be kept (marked as seen) - same count
        schema_folders2 = [f for f in catalog2.folder.all() if f.type == "schema"]
        assert len(schema_folders2) == len(schema_folders)


class TestDuckDBSeparateSchemas:
    """Test separate add_database calls with different schemas."""

    def test_separate_schema_calls_no_id_conflict(self) -> None:
        """Two add_database calls with different schemas sharing a table name should not conflict."""
        import ibis
        import pyarrow as pa

        con = ibis.duckdb.connect(":memory:")
        con.raw_sql("CREATE SCHEMA schema_a")
        con.raw_sql("CREATE SCHEMA schema_b")
        con.create_table("shared_table", pa.table({"id": [1]}), database="schema_a")
        con.create_table("shared_table", pa.table({"id": [2]}), database="schema_b")

        catalog = Catalog(quiet=True)
        catalog.add_database(con, Folder(id="db", name="DB"), schema="schema_a")
        catalog.add_database(con, Folder(id="db", name="DB"), schema="schema_b")

        datasets = catalog.dataset.all()
        assert len(datasets) == 2
        ids = {d.id for d in datasets}
        assert "db---schema_a---shared_table" in ids
        assert "db---schema_b---shared_table" in ids
        con.disconnect()
