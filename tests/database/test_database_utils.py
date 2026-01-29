"""Database utility functions tests."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import ibis
import pytest

from datannurpy.scanner.database import (
    connect,
    get_database_name,
    get_database_path,
    get_schemas_to_scan,
    list_schemas,
    list_tables,
    parse_connection_string,
    raise_driver_error,
    scan_table,
)


class TestParseConnectionString:
    """Tests for connection string parsing (all backends)."""

    def test_sqlite_relative_path(self) -> None:
        backend, kwargs = parse_connection_string("sqlite:///data.db")
        assert backend == "sqlite"
        assert kwargs["path"] == "data.db"

    def test_sqlite_absolute_path(self) -> None:
        backend, kwargs = parse_connection_string("sqlite:////tmp/data.db")
        assert backend == "sqlite"
        assert kwargs["path"] == "/tmp/data.db"

    def test_postgres_full(self) -> None:
        backend, kwargs = parse_connection_string(
            "postgresql://user:pass@localhost:5432/mydb"
        )
        assert backend == "postgres"
        assert kwargs["host"] == "localhost"
        assert kwargs["port"] == "5432"
        assert kwargs["user"] == "user"
        assert kwargs["password"] == "pass"
        assert kwargs["database"] == "mydb"

    def test_mysql_full(self) -> None:
        backend, kwargs = parse_connection_string(
            "mysql://root:secret@db.example.com/app"
        )
        assert backend == "mysql"
        assert kwargs["host"] == "db.example.com"
        assert kwargs["user"] == "root"
        assert kwargs["password"] == "secret"
        assert kwargs["database"] == "app"

    def test_oracle_full(self) -> None:
        backend, kwargs = parse_connection_string(
            "oracle://system:secret@db.example.com:1521/ORCL"
        )
        assert backend == "oracle"
        assert kwargs["host"] == "db.example.com"
        assert kwargs["port"] == "1521"
        assert kwargs["user"] == "system"
        assert kwargs["password"] == "secret"
        assert kwargs["database"] == "ORCL"

    def test_mssql_full(self) -> None:
        backend, kwargs = parse_connection_string(
            "mssql://sa:secret@db.example.com:1433/mydb"
        )
        assert backend == "mssql"
        assert kwargs["host"] == "db.example.com"
        assert kwargs["port"] == "1433"
        assert kwargs["user"] == "sa"
        assert kwargs["password"] == "secret"
        assert kwargs["database"] == "mydb"

    def test_postgres_no_hostname(self) -> None:
        """Connection without hostname (uses localhost default)."""
        backend, kwargs = parse_connection_string("postgresql:///mydb")
        assert backend == "postgres"
        assert "host" not in kwargs
        assert kwargs["database"] == "mydb"

    def test_postgres_no_database(self) -> None:
        """Connection without database path."""
        backend, kwargs = parse_connection_string("postgresql://localhost")
        assert backend == "postgres"
        assert kwargs["host"] == "localhost"
        assert "database" not in kwargs

    def test_postgres_with_query_params(self) -> None:
        """Connection with query string parameters."""
        backend, kwargs = parse_connection_string(
            "postgresql://localhost/mydb?sslmode=require&connect_timeout=10"
        )
        assert backend == "postgres"
        assert kwargs["database"] == "mydb"
        assert kwargs["sslmode"] == "require"
        assert kwargs["connect_timeout"] == "10"

    def test_unsupported_scheme(self) -> None:
        with pytest.raises(ValueError, match="Unsupported database scheme"):
            parse_connection_string("mongodb://user:pass@host/db")


class TestRaiseDriverError:
    """Tests for driver error messages."""

    def test_known_backend(self) -> None:
        with pytest.raises(ImportError, match="PostgreSQL requires psycopg2"):
            raise_driver_error("postgres", ModuleNotFoundError("psycopg2"))

    def test_unknown_backend(self) -> None:
        with pytest.raises(ImportError, match="Missing driver for foo"):
            raise_driver_error("foo", ModuleNotFoundError("foo"))


class TestConnect:
    """Tests for connect function."""

    def test_unsupported_ibis_backend(self) -> None:
        """Passing an unsupported Ibis backend raises ValueError."""
        mock_con = MagicMock(spec=ibis.BaseBackend)
        with patch(
            "datannurpy.scanner.database.get_backend_name", return_value="pyspark"
        ):
            with pytest.raises(ValueError, match="pyspark.*is not supported"):
                connect(mock_con)

    def test_external_backend_calls_helper(self) -> None:
        """Non-sqlite backends call _connect_external_backend."""
        mock_con = MagicMock(spec=ibis.BaseBackend)
        with patch(
            "datannurpy.scanner.database._connect_external_backend",
            return_value=mock_con,
        ) as mock_connect:
            con, backend = connect("postgresql://localhost/mydb")
            mock_connect.assert_called_once_with(
                "postgres", {"host": "localhost", "database": "mydb"}
            )
            assert con is mock_con
            assert backend == "postgres"


class TestGetDatabaseName:
    """Tests for get_database_name function."""

    def test_connection_string_sqlite(self) -> None:
        """Connection string for SQLite extracts database name from path."""
        con = ibis.sqlite.connect(":memory:")
        try:
            result = get_database_name("sqlite:////path/to/mydb.sqlite", con, "sqlite")
            assert result == "mydb"
        finally:
            con.disconnect()

    def test_connection_string_postgres(self) -> None:
        """Connection string for postgres extracts database name from path."""
        con = ibis.sqlite.connect(":memory:")  # Mock con, not actually used
        try:
            result = get_database_name("postgresql://localhost/mydb", con, "postgres")
            assert result == "mydb"
        finally:
            con.disconnect()

    def test_connection_string_no_database(self) -> None:
        """Connection string without database falls back to backend name."""
        con = ibis.sqlite.connect(":memory:")
        try:
            result = get_database_name("postgresql://localhost", con, "postgres")
            assert result == "postgres"
        finally:
            con.disconnect()

    def test_connection_object_sqlite(self) -> None:
        """Passing an Ibis connection object returns backend name for SQLite."""
        con = ibis.sqlite.connect(":memory:")
        try:
            # SQLite current_database is "main", so we fallback to backend_name
            result = get_database_name(con, con, "sqlite")
            assert result == "sqlite"
        finally:
            con.disconnect()

    def test_connection_object_with_current_database(self) -> None:
        """Connection object with current_database returns the database name."""
        mock_con = MagicMock(spec=ibis.BaseBackend)
        mock_con.current_database = "mydb"
        result = get_database_name(mock_con, mock_con, "postgres")
        assert result == "mydb"


class TestGetDatabasePath:
    """Tests for get_database_path function."""

    def test_sqlite_with_path(self) -> None:
        """SQLite connection with file path returns resolved path."""
        result = get_database_path("sqlite:////tmp/test.db", "sqlite")
        assert result is not None
        assert result.endswith("tmp/test.db")

    def test_sqlite_memory(self) -> None:
        """SQLite in-memory connection returns None."""
        result = get_database_path("sqlite:///:memory:", "sqlite")
        assert result is None

    def test_postgres_returns_none(self) -> None:
        """Non-file backends return None."""
        result = get_database_path("postgresql://localhost/mydb", "postgres")
        assert result is None


class TestGetSchemasToScan:
    """Tests for get_schemas_to_scan function."""

    def test_oracle_appends_none(self) -> None:
        """Oracle backend appends None to schemas list."""
        mock_con = MagicMock(spec=ibis.BaseBackend)
        with patch(
            "datannurpy.scanner.database.list_schemas", return_value=["hr", "sales"]
        ):
            result = get_schemas_to_scan(mock_con, None, "oracle")
        assert result == ["hr", "sales", None]

    def test_empty_schemas_returns_none(self) -> None:
        """When all schemas are system schemas, returns [None]."""
        mock_con = MagicMock(spec=ibis.BaseBackend)
        # Return only system schemas that get filtered out
        with patch("datannurpy.scanner.database.list_schemas", return_value=[]):
            result = get_schemas_to_scan(mock_con, None, "postgres")
        assert result == [None]


class TestListSchemas:
    """Tests for list_schemas function."""

    def test_oracle_uses_raw_sql(self) -> None:
        """Oracle backend uses raw SQL to list schemas."""
        mock_con = MagicMock()
        type(mock_con).__module__ = "ibis.backends.oracle"
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("HR",), ("SALES",)]
        mock_con.raw_sql.return_value = mock_cursor

        result = list_schemas(mock_con)

        assert result == ["hr", "sales"]  # sorted, lowercase

    def test_oracle_without_raw_sql(self) -> None:
        """Oracle backend without raw_sql returns empty list."""
        mock_con = MagicMock()
        type(mock_con).__module__ = "ibis.backends.oracle"
        mock_con.raw_sql = None

        result = list_schemas(mock_con)

        assert result == []

    def test_with_list_schemas_method(self) -> None:
        """Backend with list_schemas method uses it."""
        mock_con = MagicMock()
        type(mock_con).__module__ = "ibis.backends.unknown"
        mock_con.list_schemas.return_value = ["public", "sales"]

        result = list_schemas(mock_con)

        assert result == ["public", "sales"]

    def test_exception_returns_empty_list(self) -> None:
        """When list_schemas raises an exception, returns empty list."""
        mock_con = MagicMock()
        type(mock_con).__module__ = "ibis.backends.unknown"
        mock_con.list_schemas.side_effect = RuntimeError("Connection lost")

        result = list_schemas(mock_con)

        assert result == []

    def test_no_schema_methods_returns_empty_list(self) -> None:
        """When neither list_schemas nor list_databases exist, returns empty list."""
        mock_con = MagicMock()
        type(mock_con).__module__ = "ibis.backends.unknown"
        del mock_con.list_schemas
        del mock_con.list_databases

        result = list_schemas(mock_con)

        assert result == []


class TestListTables:
    """Tests for list_tables function."""

    def test_oracle_uses_raw_sql(self) -> None:
        """Oracle backend uses raw SQL to list tables."""
        mock_con = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("EMPLOYEES",), ("DEPARTMENTS",)]
        mock_con.raw_sql.return_value = mock_cursor

        result = list_tables(mock_con, backend_name="oracle")

        assert result == ["departments", "employees"]  # sorted, lowercase

    def test_oracle_with_schema(self) -> None:
        """Oracle backend with schema uses all_tables with owner filter."""
        mock_con = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("SALES",)]
        mock_con.raw_sql.return_value = mock_cursor

        result = list_tables(mock_con, schema="hr", backend_name="oracle")

        # Verify query uses all_tables with owner
        mock_con.raw_sql.assert_called_once()
        query = mock_con.raw_sql.call_args[0][0]
        assert "all_tables" in query
        assert "HR" in query  # schema uppercased
        assert result == ["sales"]

    def test_fallback_to_ibis_list_tables(self) -> None:
        """When raw_sql not available, fallback to Ibis list_tables."""
        mock_con = MagicMock()
        mock_con.raw_sql = None  # No raw_sql support
        mock_con.list_tables.return_value = ["users", "orders"]

        result = list_tables(mock_con, backend_name="unknown")

        mock_con.list_tables.assert_called_once()
        assert result == ["orders", "users"]  # sorted


class TestScanTable:
    """Tests for scan_table function."""

    def test_oracle_uppercases_identifiers(self) -> None:
        """Oracle backend uppercases table name and schema."""
        # Create a mock that makes get_backend_name return "oracle"
        # by having __module__ end with "oracle"
        mock_con = MagicMock()
        type(mock_con).__module__ = "ibis.backends.oracle"

        mock_table = MagicMock()
        mock_table.count.return_value.to_pyarrow.return_value.as_py.return_value = 0
        mock_table.rename.return_value = mock_table
        mock_con.table.return_value = mock_table

        with patch(
            "datannurpy.scanner.database.build_variables", return_value=([], None)
        ):
            scan_table(mock_con, "employees", schema="hr", dataset_id="test")

        # Verify table was called with uppercased identifiers
        mock_con.table.assert_called_once_with("EMPLOYEES", database="HR")

    def test_oracle_without_schema(self) -> None:
        """Oracle backend without schema only uppercases table name."""
        mock_con = MagicMock()
        type(mock_con).__module__ = "ibis.backends.oracle"

        mock_table = MagicMock()
        mock_table.count.return_value.to_pyarrow.return_value.as_py.return_value = 0
        mock_table.rename.return_value = mock_table
        mock_con.table.return_value = mock_table

        with patch(
            "datannurpy.scanner.database.build_variables", return_value=([], None)
        ):
            scan_table(mock_con, "employees", dataset_id="test")

        mock_con.table.assert_called_once_with("EMPLOYEES")

    def test_oracle_without_raw_sql(self) -> None:
        """Oracle backend without raw_sql skips CLOB detection."""
        mock_con = MagicMock()
        type(mock_con).__module__ = "ibis.backends.oracle"
        mock_con.raw_sql = None  # No raw_sql support

        mock_table = MagicMock()
        mock_table.count.return_value.to_pyarrow.return_value.as_py.return_value = 0
        mock_table.rename.return_value = mock_table
        mock_con.table.return_value = mock_table

        with patch(
            "datannurpy.scanner.database.build_variables", return_value=([], None)
        ):
            scan_table(mock_con, "employees", dataset_id="test")

        mock_con.table.assert_called_once_with("EMPLOYEES")

    def test_oracle_clob_detection_failure(self) -> None:
        """Oracle CLOB detection failure is silently ignored."""
        mock_con = MagicMock()
        type(mock_con).__module__ = "ibis.backends.oracle"
        mock_con.raw_sql.side_effect = RuntimeError("Query failed")

        mock_table = MagicMock()
        mock_table.count.return_value.to_pyarrow.return_value.as_py.return_value = 0
        mock_table.rename.return_value = mock_table
        mock_con.table.return_value = mock_table

        with patch(
            "datannurpy.scanner.database.build_variables", return_value=([], None)
        ):
            # Should not raise, continues without skip_stats_columns
            scan_table(mock_con, "employees", dataset_id="test")

        mock_con.table.assert_called_once_with("EMPLOYEES")
