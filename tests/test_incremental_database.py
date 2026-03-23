"""Tests for incremental scan functionality (database tables)."""

from __future__ import annotations

import sqlite3
from collections.abc import Generator
from pathlib import Path

import ibis
import pytest

from datannurpy import Catalog, Folder
from datannurpy.scanner.database import (
    build_table_data_path,
    close_connection,
    compute_schema_signature,
    get_table_row_count,
)


@pytest.fixture
def sample_db(tmp_path: Path) -> Path:
    """Create a temporary SQLite database with sample data."""
    db_path = tmp_path / "test.db"
    # Use sqlite3 directly to ensure data is committed
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE users (id INT, name TEXT)")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
    cursor.execute("CREATE TABLE orders (id INT, amount REAL)")
    cursor.execute("INSERT INTO orders VALUES (1, 100.0), (2, 250.0)")
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def ibis_con(sample_db: Path) -> Generator[ibis.BaseBackend, None, None]:
    """Create an Ibis SQLite connection that is properly closed."""
    con = ibis.sqlite.connect(sample_db)
    yield con
    close_connection(con)


class TestBuildTableDataPath:
    """Tests for build_table_data_path."""

    def test_without_schema(self):
        """Build data_path without schema."""
        result = build_table_data_path("sqlite", "mydb", None, "users")
        assert result == "sqlite://mydb/users"

    def test_with_schema(self):
        """Build data_path with schema."""
        result = build_table_data_path("postgres", "mydb", "public", "users")
        assert result == "postgres://mydb/public/users"


class TestComputeSchemaSignature:
    """Tests for compute_schema_signature."""

    def test_same_schema_same_signature(self, ibis_con: ibis.BaseBackend):
        """Same schema should produce same signature."""
        sig1 = compute_schema_signature(ibis_con, "users", None)
        sig2 = compute_schema_signature(ibis_con, "users", None)
        assert sig1 == sig2

    def test_different_schema_different_signature(self, ibis_con: ibis.BaseBackend):
        """Different schemas should produce different signatures."""
        sig_users = compute_schema_signature(ibis_con, "users", None)
        sig_orders = compute_schema_signature(ibis_con, "orders", None)
        assert sig_users != sig_orders

    def test_schema_change_changes_signature(self, tmp_path: Path):
        """Modifying schema should change signature."""
        db_path = tmp_path / "test.db"
        # Use sqlite3 directly
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE test (id INT, name TEXT)")
        conn.commit()
        conn.close()

        con = ibis.sqlite.connect(db_path)
        sig_before = compute_schema_signature(con, "test", None)
        close_connection(con)

        # Modify schema
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("ALTER TABLE test ADD COLUMN email TEXT")
        conn.commit()
        conn.close()

        con = ibis.sqlite.connect(db_path)
        sig_after = compute_schema_signature(con, "test", None)
        close_connection(con)

        assert sig_before != sig_after


class TestGetTableRowCount:
    """Tests for get_table_row_count."""

    def test_row_count(self, ibis_con: ibis.BaseBackend):
        """Get correct row count."""
        count = get_table_row_count(ibis_con, "users", None)
        assert count == 2


class TestIncrementalScanDatabase:
    """Tests for incremental scan with database tables."""

    def test_unchanged_table_is_skipped(self, sample_db: Path, tmp_path: Path):
        """Unchanged table should be skipped on second scan."""
        app_dir = tmp_path / "catalog"
        conn_str = f"sqlite:////{sample_db}"

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_database(conn_str, Folder(id="db", name="Database"))
        catalog1.export_db()

        assert len(catalog1.dataset.all()) == 2

        # Second scan
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_database(conn_str, Folder(id="db", name="Database"))

        # Should still have 2 datasets (unchanged)
        assert len(catalog2.dataset.all()) == 2
        # All should be marked as seen
        for ds in catalog2.dataset.all():
            assert ds._seen is True

    def test_modified_table_is_rescanned(self, sample_db: Path, tmp_path: Path):
        """Modified table (row count change) should be rescanned."""
        app_dir = tmp_path / "catalog"
        conn_str = f"sqlite:////{sample_db}"

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_database(conn_str, Folder(id="db", name="Database"))
        catalog1.export_db()

        users_ds = next(
            ds for ds in catalog1.dataset.all() if "users" in (ds.name or "")
        )
        assert users_ds.nb_row == 2

        # Modify table (add row)
        conn = sqlite3.connect(sample_db)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO users VALUES (3, 'Charlie')")
        conn.commit()
        conn.close()

        # Second scan
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_database(conn_str, Folder(id="db", name="Database"))

        # Should have rescanned with new row count
        users_ds2 = next(
            ds for ds in catalog2.dataset.all() if "users" in (ds.name or "")
        )
        assert users_ds2.nb_row == 3

    def test_schema_change_triggers_rescan(self, sample_db: Path, tmp_path: Path):
        """Schema change should trigger rescan."""
        app_dir = tmp_path / "catalog"
        conn_str = f"sqlite:////{sample_db}"

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_database(conn_str, Folder(id="db", name="Database"))
        catalog1.export_db()

        users_ds = next(
            ds for ds in catalog1.dataset.all() if "users" in (ds.name or "")
        )
        old_signature = users_ds.schema_signature

        # Modify schema (add column)
        conn = sqlite3.connect(sample_db)
        cursor = conn.cursor()
        cursor.execute("ALTER TABLE users ADD COLUMN email TEXT")
        conn.commit()
        conn.close()

        # Second scan
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_database(conn_str, Folder(id="db", name="Database"))

        # Should have new signature
        users_ds2 = next(
            ds for ds in catalog2.dataset.all() if "users" in (ds.name or "")
        )
        assert users_ds2.schema_signature != old_signature

    def test_new_table_is_added(self, sample_db: Path, tmp_path: Path):
        """New table should be added on second scan."""
        app_dir = tmp_path / "catalog"
        conn_str = f"sqlite:////{sample_db}"

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_database(conn_str, Folder(id="db", name="Database"))
        catalog1.export_db()

        assert len(catalog1.dataset.all()) == 2

        # Add new table
        conn = sqlite3.connect(sample_db)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE products (id INT, name TEXT)")
        cursor.execute("INSERT INTO products VALUES (1, 'Widget')")
        conn.commit()
        conn.close()

        # Second scan
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_database(conn_str, Folder(id="db", name="Database"))

        # Should have 3 datasets
        assert len(catalog2.dataset.all()) == 3
        assert any("products" in (ds.name or "") for ds in catalog2.dataset.all())

    def test_refresh_forces_rescan(self, sample_db: Path, tmp_path: Path):
        """refresh=True should force rescan even if unchanged."""
        app_dir = tmp_path / "catalog"
        conn_str = f"sqlite:////{sample_db}"

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_database(conn_str, Folder(id="db", name="Database"))
        catalog1.export_db()

        initial_vars = len(catalog1.variable.all())

        # Second scan with refresh=True - should rescan and update existing datasets
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_database(conn_str, Folder(id="db", name="Database"), refresh=True)

        # With refresh, tables are rescanned and updated (not duplicated)
        # The key is that refresh=True doesn't skip the tables even if unchanged
        assert len(catalog2.dataset.all()) == 2  # Same count, rescanned in place
        assert len(catalog2.variable.all()) == initial_vars  # Same variables

    def test_refresh_false_skips_unchanged(self, sample_db: Path, tmp_path: Path):
        """refresh=False should skip unchanged tables."""
        app_dir = tmp_path / "catalog"
        conn_str = f"sqlite:////{sample_db}"

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_database(conn_str, Folder(id="db", name="Database"))
        catalog1.export_db()

        # Second scan with same folder (datasets should be skipped)
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_database(conn_str, Folder(id="db", name="Database"))

        # Should have same number of datasets (not duplicated)
        assert len(catalog2.dataset.all()) == 2

    def test_data_path_stored_for_tables(self, sample_db: Path, tmp_path: Path):
        """Tables should have data_path stored for incremental tracking."""
        catalog = Catalog(quiet=True)
        conn_str = f"sqlite:////{sample_db}"
        catalog.add_database(conn_str, Folder(id="db", name="Database"))

        for ds in catalog.dataset.all():
            assert ds.data_path is not None
            assert ds.data_path.startswith("sqlite://")

    def test_schema_signature_stored(self, sample_db: Path, tmp_path: Path):
        """Tables should have schema_signature stored."""
        catalog = Catalog(quiet=True)
        conn_str = f"sqlite:////{sample_db}"
        catalog.add_database(conn_str, Folder(id="db", name="Database"))

        for ds in catalog.dataset.all():
            assert ds.schema_signature is not None
            assert len(ds.schema_signature) == 32  # MD5 hex

    def test_first_scan_no_update_date(self, sample_db: Path, tmp_path: Path):
        """First scan should set last_update_date/timestamp to None."""
        catalog = Catalog(quiet=True)
        conn_str = f"sqlite:////{sample_db}"
        catalog.add_database(conn_str, Folder(id="db", name="Database"))

        for ds in catalog.dataset.all():
            assert ds.last_update_timestamp is None
            assert ds.last_update_date is None

    def test_change_detected_sets_update_date(self, sample_db: Path, tmp_path: Path):
        """Rescan with change should populate last_update_date."""
        app_dir = tmp_path / "catalog"
        conn_str = f"sqlite:////{sample_db}"

        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_database(conn_str, Folder(id="db", name="Database"))
        catalog1.export_db()

        # Modify table
        conn = sqlite3.connect(sample_db)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO users VALUES (3, 'Charlie')")
        conn.commit()
        conn.close()

        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_database(conn_str, Folder(id="db", name="Database"))

        users = next(d for d in catalog2.dataset.all() if "users" in (d.name or ""))
        assert users.last_update_date is not None
        assert users.last_update_timestamp is not None

    def test_refresh_preserves_timestamp_when_unchanged(
        self, sample_db: Path, tmp_path: Path
    ):
        """refresh=True on unchanged data should preserve last_update_timestamp."""
        app_dir = tmp_path / "catalog"
        conn_str = f"sqlite:////{sample_db}"

        # Scan 1: first scan (timestamps are None)
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_database(conn_str, Folder(id="db", name="Database"))
        catalog1.export_db()

        # Modify table to trigger a real change
        conn = sqlite3.connect(sample_db)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO users VALUES (3, 'Charlie')")
        conn.commit()
        conn.close()

        # Scan 2: detects change → sets timestamp
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_database(conn_str, Folder(id="db", name="Database"))
        catalog2.export_db()

        users2 = next(d for d in catalog2.dataset.all() if "users" in (d.name or ""))
        saved_ts = users2.last_update_timestamp
        assert saved_ts is not None

        # Scan 3: refresh=True but no change → preserves timestamp
        catalog3 = Catalog(app_path=app_dir, quiet=True)
        catalog3.add_database(conn_str, Folder(id="db", name="Database"), refresh=True)

        users3 = next(d for d in catalog3.dataset.all() if "users" in (d.name or ""))
        assert users3.last_update_timestamp == saved_ts


class TestCloseConnection:
    """Tests for close_connection helper."""

    def test_close_connection_with_internal_con(self, sample_db: Path):
        """close_connection closes SQLite internal connection."""
        con = ibis.sqlite.connect(sample_db)
        # Verify connection works
        assert con.list_tables()
        # Close it
        close_connection(con)
        # Connection should be closed (accessing it may raise or return empty)

    def test_close_connection_without_internal_con(self):
        """close_connection handles backends without internal 'con' attribute."""
        from unittest.mock import MagicMock

        # Create a mock backend without 'con' attribute
        mock_con = MagicMock(spec=["disconnect"])
        close_connection(mock_con)
        mock_con.disconnect.assert_called_once()

    def test_close_connection_with_non_closable_internal(self):
        """close_connection handles internal con without close method."""
        from unittest.mock import MagicMock

        # Create a mock backend with 'con' but no 'close' method on it
        mock_con = MagicMock(spec=["disconnect", "con"])
        mock_con.con = MagicMock(spec=[])  # No close method
        close_connection(mock_con)
        mock_con.disconnect.assert_called_once()

    def test_close_connection_already_closed(self):
        """close_connection ignores error when internal connection is already closed."""
        from unittest.mock import MagicMock

        mock_con = MagicMock(spec=["disconnect", "con"])
        mock_con.con = MagicMock()
        mock_con.con.close.side_effect = Exception("DPY-1001: not connected")
        close_connection(mock_con)
        mock_con.disconnect.assert_called_once()
        mock_con.con.close.assert_called_once()


class TestOracleBranches:
    """Tests for Oracle-specific branches using mocks."""

    _oracle_schema_patch = "datannurpy.scanner.database._oracle_get_schema"
    _mock_schema_result = (ibis.schema({"ID": "int64"}), set(), set())

    def test_compute_schema_signature_oracle_with_schema(self):
        """Oracle backend should use con.sql() with explicit schema."""
        from unittest.mock import MagicMock, patch

        mock_con = MagicMock()
        mock_table = MagicMock()
        mock_table.schema.return_value = {"id": "int64", "name": "string"}
        mock_table.rename.return_value = mock_table
        mock_con.sql.return_value = mock_table

        with (
            patch(
                "datannurpy.scanner.database.get_backend_name", return_value="oracle"
            ),
            patch(self._oracle_schema_patch, return_value=self._mock_schema_result),
        ):
            compute_schema_signature(mock_con, "users", "myschema")

        mock_con.sql.assert_called_once()
        assert mock_con.sql.call_args[0][0] == 'SELECT * FROM "MYSCHEMA"."USERS"'
        mock_con.table.assert_not_called()

    def test_compute_schema_signature_oracle_no_schema(self):
        """Oracle backend without schema should use con.sql() with table only."""
        from unittest.mock import MagicMock, patch

        mock_con = MagicMock()
        mock_table = MagicMock()
        mock_table.schema.return_value = {"id": "int64", "name": "string"}
        mock_table.rename.return_value = mock_table
        mock_con.sql.return_value = mock_table

        with (
            patch(
                "datannurpy.scanner.database.get_backend_name", return_value="oracle"
            ),
            patch(self._oracle_schema_patch, return_value=self._mock_schema_result),
        ):
            compute_schema_signature(mock_con, "users", None)

        mock_con.sql.assert_called_once()
        assert mock_con.sql.call_args[0][0] == 'SELECT * FROM "USERS"'
        mock_con.table.assert_not_called()

    def test_get_table_row_count_oracle_with_schema(self):
        """Oracle backend should use con.sql() with explicit schema."""
        from unittest.mock import MagicMock, patch

        mock_con = MagicMock()
        mock_table = MagicMock()
        mock_count = MagicMock()
        mock_count.to_pyarrow.return_value.as_py.return_value = 42
        mock_table.count.return_value = mock_count
        mock_table.rename.return_value = mock_table
        mock_con.sql.return_value = mock_table

        with (
            patch(
                "datannurpy.scanner.database.get_backend_name", return_value="oracle"
            ),
            patch(self._oracle_schema_patch, return_value=self._mock_schema_result),
        ):
            result = get_table_row_count(mock_con, "users", "myschema")

        mock_con.sql.assert_called_once()
        assert mock_con.sql.call_args[0][0] == 'SELECT * FROM "MYSCHEMA"."USERS"'
        mock_con.table.assert_not_called()
        assert result == 42

    def test_get_table_row_count_oracle_no_schema(self):
        """Oracle backend without schema should use con.sql() with table only."""
        from unittest.mock import MagicMock, patch

        mock_con = MagicMock()
        mock_table = MagicMock()
        mock_count = MagicMock()
        mock_count.to_pyarrow.return_value.as_py.return_value = 10
        mock_table.count.return_value = mock_count
        mock_table.rename.return_value = mock_table
        mock_con.sql.return_value = mock_table

        with (
            patch(
                "datannurpy.scanner.database.get_backend_name", return_value="oracle"
            ),
            patch(self._oracle_schema_patch, return_value=self._mock_schema_result),
        ):
            result = get_table_row_count(mock_con, "users", None)

        mock_con.sql.assert_called_once()
        assert mock_con.sql.call_args[0][0] == 'SELECT * FROM "USERS"'
        mock_con.table.assert_not_called()
        assert result == 10


class TestDepthParameterDatabase:
    """Test depth parameter for database scanning."""

    def test_depth_structure_creates_datasets_without_variables(self, sample_db: Path):
        """depth='structure' should create datasets but no variables."""
        conn_str = f"sqlite:////{sample_db}"

        catalog = Catalog(quiet=True)
        catalog.add_database(conn_str, depth="structure")

        # Should have datasets (users, orders)
        assert len(catalog.dataset.all()) == 2
        # But no variables
        assert len(catalog.variable.all()) == 0

        # Structure mode: no queries, nb_row is None
        for ds in catalog.dataset.all():
            assert ds.nb_row is None

    def test_depth_schema_creates_variables_without_stats(self, sample_db: Path):
        """depth='schema' should create variables without stats."""
        conn_str = f"sqlite:////{sample_db}"

        catalog = Catalog(quiet=True)
        catalog.add_database(conn_str, depth="schema")

        # Should have datasets and variables
        assert len(catalog.dataset.all()) == 2
        assert len(catalog.variable.all()) > 0

        # Variables should have no stats (nb_distinct, nb_missing)
        for var in catalog.variable.all():
            assert var.nb_distinct is None
            assert var.nb_missing is None

    def test_depth_at_catalog_level_affects_database(self, sample_db: Path):
        """depth set at Catalog level should affect add_database."""
        conn_str = f"sqlite:////{sample_db}"

        # Set depth at catalog level
        catalog = Catalog(depth="structure", quiet=True)
        catalog.add_database(conn_str)

        assert len(catalog.dataset.all()) == 2
        assert len(catalog.variable.all()) == 0  # structure mode

    def test_depth_override_at_add_database(self, sample_db: Path):
        """depth at add_database should override catalog.depth."""
        conn_str = f"sqlite:////{sample_db}"

        catalog = Catalog(depth="structure", quiet=True)
        catalog.add_database(conn_str, depth="schema")

        assert len(catalog.dataset.all()) == 2
        assert len(catalog.variable.all()) > 0  # schema mode overrides

    def test_depth_structure_skips_queries(self, sample_db: Path):
        """depth='structure' should not compute schema_signature or nb_row."""
        from unittest.mock import patch

        conn_str = f"sqlite:////{sample_db}"
        catalog = Catalog(quiet=True)

        with (
            patch("datannurpy.add_database.compute_schema_signature") as mock_sig,
            patch("datannurpy.add_database.get_table_row_count") as mock_count,
        ):
            catalog.add_database(conn_str, depth="structure")

        mock_sig.assert_not_called()
        mock_count.assert_not_called()
        assert len(catalog.dataset.all()) == 2

    def test_depth_structure_incremental_skips_unchanged(self, sample_db: Path):
        """depth='structure' second run marks existing datasets as seen."""
        conn_str = f"sqlite:////{sample_db}"
        catalog = Catalog(quiet=True)
        catalog.add_database(conn_str, depth="structure")
        assert len(catalog.dataset.all()) == 2

        # Second run without refresh: should skip (mark seen)
        catalog.add_database(conn_str, depth="structure")
        assert len(catalog.dataset.all()) == 2

    def test_depth_structure_with_prefix_grouping(self, tmp_path: Path):
        """depth='structure' respects prefix-based folder grouping."""
        db_path = tmp_path / "prefixed.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE sales_orders (id INT)")
        cursor.execute("CREATE TABLE sales_items (id INT)")
        cursor.execute("CREATE TABLE hr_employees (id INT)")
        cursor.execute("CREATE TABLE hr_departments (id INT)")
        conn.commit()
        conn.close()

        conn_str = f"sqlite:////{db_path}"
        catalog = Catalog(quiet=True)
        catalog.add_database(conn_str, depth="structure")

        assert len(catalog.dataset.all()) == 4
        prefix_folders = [f for f in catalog.folder.all() if f.type == "table_prefix"]
        assert len(prefix_folders) == 2  # sales, hr
