"""Database utility functions tests."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import ibis
import pytest

from datannurpy.errors import ConfigError
from datannurpy.scanner._oracle import (
    _init_oracle_client,
    _oracle_get_schema,
    _oracle_patch_date_stats,
    _oracle_type_to_ibis,
)
from datannurpy.scanner.database import (
    _get_table,
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

    def test_oracle_tns_no_host(self) -> None:
        """Oracle URI without host parses as TNS name."""
        backend, kwargs = parse_connection_string("oracle://system:secret@/TNS_NAME")
        assert backend == "oracle"
        assert "host" not in kwargs
        assert kwargs["user"] == "system"
        assert kwargs["password"] == "secret"
        assert kwargs["database"] == "TNS_NAME"

    def test_unsupported_scheme(self) -> None:
        with pytest.raises(ConfigError, match="Unsupported database scheme"):
            parse_connection_string("mongodb://user:pass@host/db")


class TestRaiseDriverError:
    """Tests for driver error messages."""

    def test_known_backend(self) -> None:
        with pytest.raises(ConfigError, match="PostgreSQL requires psycopg2"):
            raise_driver_error("postgres", ModuleNotFoundError("psycopg2"))

    def test_unknown_backend(self) -> None:
        with pytest.raises(ConfigError, match="Missing driver for foo"):
            raise_driver_error("foo", ModuleNotFoundError("foo"))


class TestConnect:
    """Tests for connect function."""

    def test_unsupported_ibis_backend(self) -> None:
        """Passing an unsupported Ibis backend raises ValueError."""
        mock_con = MagicMock(spec=ibis.BaseBackend)
        with patch(
            "datannurpy.scanner.database.get_backend_name", return_value="pyspark"
        ):
            with pytest.raises(ConfigError, match="pyspark.*is not supported"):
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
                "postgres",
                {"host": "localhost", "database": "mydb"},
                oracle_client_path=None,
            )
            assert con is mock_con
            assert backend == "postgres"

    def test_oracle_client_path_passed_through(self) -> None:
        """oracle_client_path is forwarded to _connect_external_backend."""
        mock_con = MagicMock(spec=ibis.BaseBackend)
        with patch(
            "datannurpy.scanner.database._connect_external_backend",
            return_value=mock_con,
        ) as mock_connect:
            con, backend = connect(
                "oracle://user:pass@/TNS_NAME",
                oracle_client_path="/opt/oracle/client",
            )
            mock_connect.assert_called_once_with(
                "oracle",
                {"user": "user", "password": "pass", "database": "TNS_NAME"},
                oracle_client_path="/opt/oracle/client",
            )
            assert con is mock_con
            assert backend == "oracle"


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


class TestOracleTypeToIbis:
    """Tests for _oracle_type_to_ibis mapping."""

    def test_number_with_scale(self) -> None:
        assert _oracle_type_to_ibis("NUMBER", 10, 2) == "float64"

    def test_number_integer(self) -> None:
        assert _oracle_type_to_ibis("NUMBER", 10, 0) == "int64"

    def test_number_generic(self) -> None:
        assert _oracle_type_to_ibis("NUMBER", None, None) == "float64"

    def test_varchar2(self) -> None:
        assert _oracle_type_to_ibis("VARCHAR2", None, None) == "string"

    def test_date(self) -> None:
        assert _oracle_type_to_ibis("DATE", None, None) == "timestamp"

    def test_timestamp_with_tz(self) -> None:
        assert (
            _oracle_type_to_ibis("TIMESTAMP(6) WITH TIME ZONE", None, None)
            == "timestamp"
        )

    def test_blob(self) -> None:
        assert _oracle_type_to_ibis("BLOB", None, None) == "binary"

    def test_unknown_type_defaults_to_string(self) -> None:
        assert _oracle_type_to_ibis("SDO_GEOMETRY", None, None) == "string"


class TestOracleGetSchema:
    """Tests for _oracle_get_schema metadata query."""

    def test_with_schema(self) -> None:
        """Queries all_tab_columns with owner filter."""
        mock_con = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("ID", "NUMBER", 10, 0),
            ("NAME", "VARCHAR2", None, None),
        ]
        mock_con.raw_sql.return_value = mock_cursor

        schema, lob_columns, date_columns = _oracle_get_schema(
            mock_con, "employees", "hr"
        )

        query = mock_con.raw_sql.call_args[0][0]
        assert "all_tab_columns" in query
        assert "HR" in query
        assert "EMPLOYEES" in query
        assert schema == ibis.schema({"ID": "int64", "NAME": "string"})
        assert lob_columns == set()
        assert date_columns == set()

    def test_without_schema(self) -> None:
        """Queries user_tab_columns without owner filter."""
        mock_con = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("ID", "NUMBER", 10, 0),
        ]
        mock_con.raw_sql.return_value = mock_cursor

        schema, lob_columns, date_columns = _oracle_get_schema(
            mock_con, "employees", None
        )

        query = mock_con.raw_sql.call_args[0][0]
        assert "user_tab_columns" in query
        assert schema == ibis.schema({"ID": "int64"})
        assert lob_columns == set()
        assert date_columns == set()

    def test_detects_lob_columns(self) -> None:
        """LOB columns are returned in the lob_columns set."""
        mock_con = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("ID", "NUMBER", 10, 0),
            ("NOTES", "CLOB", None, None),
            ("DATA", "BLOB", None, None),
        ]
        mock_con.raw_sql.return_value = mock_cursor

        _, lob_columns, date_columns = _oracle_get_schema(mock_con, "docs", None)

        assert lob_columns == {"notes", "data"}
        assert date_columns == set()

    def test_detects_date_columns(self) -> None:
        """DATE and TIMESTAMP columns are returned in the date_columns set."""
        mock_con = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("ID", "NUMBER", 10, 0),
            ("CREATED", "DATE", None, None),
            ("UPDATED", "TIMESTAMP(6)", None, None),
            ("LOGGED", "TIMESTAMP(6) WITH TIME ZONE", None, None),
        ]
        mock_con.raw_sql.return_value = mock_cursor

        _, _, date_columns = _oracle_get_schema(mock_con, "events", None)

        assert date_columns == {"created", "updated", "logged"}


class TestOraclePatchDateStats:
    """Tests for _oracle_patch_date_stats raw SQL fallback."""

    def test_patches_date_stats(self) -> None:
        """Date stats are computed via raw SQL and patched onto variables."""
        from datannurpy.schema import Variable

        mock_con = MagicMock()
        mock_cursor = MagicMock()
        # min, max, avg, stddev for one date column
        mock_cursor.fetchone.return_value = (0.0, 86400.0, 43200.0, 12345.6789)
        mock_con.raw_sql.return_value = mock_cursor

        var = Variable(
            id="created",
            name="created",
            dataset_id="ds",
            type="datetime",
            nb_distinct=3,
            nb_duplicate=2,
            nb_missing=0,
        )
        _oracle_patch_date_stats(mock_con, "events", None, {"created"}, [var])

        assert var.min == 0.0
        assert var.max == 86400.0
        assert var.mean == 43200.0
        assert var.std == round(12345.6789, 6)

    def test_handles_null_result(self) -> None:
        """All-null column returns None stats from SQL."""
        from datannurpy.schema import Variable

        mock_con = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (None, None, None, None)
        mock_con.raw_sql.return_value = mock_cursor

        var = Variable(
            id="created",
            name="created",
            dataset_id="ds",
            type="datetime",
            nb_distinct=2,
            nb_duplicate=1,
            nb_missing=2,
        )
        _oracle_patch_date_stats(mock_con, "events", None, {"created"}, [var])

        assert var.min is None
        assert var.max is None
        assert var.mean is None
        assert var.std is None

    def test_single_distinct_skips_stddev(self) -> None:
        """Single distinct value → STDDEV not computed, std stays None."""
        from datannurpy.schema import Variable

        mock_con = MagicMock()
        mock_cursor = MagicMock()
        # Only 3 values returned (MIN, MAX, AVG) — no STDDEV requested
        mock_cursor.fetchone.return_value = (86400.0, 86400.0, 86400.0)
        mock_con.raw_sql.return_value = mock_cursor

        var = Variable(
            id="created",
            name="created",
            dataset_id="ds",
            type="datetime",
            nb_distinct=1,
            nb_duplicate=2,
            nb_missing=0,
        )
        _oracle_patch_date_stats(mock_con, "events", None, {"created"}, [var])

        assert var.min == 86400.0
        assert var.max == 86400.0
        assert var.mean == 86400.0
        assert var.std is None
        # Verify STDDEV not in query
        query = mock_con.raw_sql.call_args[0][0]
        assert "STDDEV" not in query

    def test_all_missing_skips_column(self) -> None:
        """Column with all missing values → no SQL query at all."""
        from datannurpy.schema import Variable

        mock_con = MagicMock()

        var = Variable(
            id="created",
            name="created",
            dataset_id="ds",
            type="datetime",
            nb_distinct=0,
            nb_duplicate=0,
            nb_missing=5,
        )
        _oracle_patch_date_stats(mock_con, "events", None, {"created"}, [var])

        mock_con.raw_sql.assert_not_called()
        assert var.min is None

    def test_handles_sql_error(self) -> None:
        """SQL error leaves stats as None."""
        from datannurpy.schema import Variable

        mock_con = MagicMock()
        mock_con.raw_sql.side_effect = RuntimeError("ORA-00942")

        var = Variable(
            id="created",
            name="created",
            dataset_id="ds",
            type="datetime",
            nb_distinct=3,
            nb_duplicate=2,
            nb_missing=0,
        )
        _oracle_patch_date_stats(mock_con, "events", None, {"created"}, [var])

        assert var.min is None
        assert var.max is None

    def test_with_schema_qualifies_table(self) -> None:
        """Schema is used to qualify the table name."""
        from datannurpy.schema import Variable

        mock_con = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (0.0, 86400.0, 43200.0, 1000.0)
        mock_con.raw_sql.return_value = mock_cursor

        var = Variable(
            id="created",
            name="created",
            dataset_id="ds",
            type="datetime",
            nb_distinct=3,
            nb_duplicate=2,
            nb_missing=0,
        )
        _oracle_patch_date_stats(mock_con, "events", "hr", {"created"}, [var])

        query = mock_con.raw_sql.call_args[0][0]
        assert '"HR"."EVENTS"' in query

    def test_fetchone_returns_none(self) -> None:
        """Empty table returns None from fetchone."""
        from datannurpy.schema import Variable

        mock_con = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_con.raw_sql.return_value = mock_cursor

        var = Variable(
            id="created",
            name="created",
            dataset_id="ds",
            type="datetime",
            nb_distinct=3,
            nb_duplicate=2,
            nb_missing=0,
        )
        _oracle_patch_date_stats(mock_con, "events", None, {"created"}, [var])

        assert var.min is None

    def test_skips_missing_variable(self) -> None:
        """Date column not in variables list is silently skipped."""
        mock_con = MagicMock()

        # No variable named "created" in the list → no SQL query
        _oracle_patch_date_stats(mock_con, "events", None, {"created"}, [])

        mock_con.raw_sql.assert_not_called()

    def test_skips_variable_without_stats(self) -> None:
        """Variable with nb_missing=None (schema-only) is skipped."""
        from datannurpy.schema import Variable

        mock_con = MagicMock()
        var = Variable(id="created", name="created", dataset_id="ds", type="datetime")
        _oracle_patch_date_stats(mock_con, "events", None, {"created"}, [var])

        mock_con.raw_sql.assert_not_called()


class TestGetTable:
    """Tests for _get_table function (Oracle < 23 compatibility)."""

    def test_oracle_uses_sql_with_schema(self) -> None:
        """Oracle backend uses con.sql() with explicit schema."""
        mock_con = MagicMock()
        mock_table = MagicMock()
        mock_con.sql.return_value = mock_table
        mock_schema = ibis.schema({"ID": "int64"})

        with patch(
            "datannurpy.scanner.database._oracle_get_schema",
            return_value=(mock_schema, set(), set()),
        ):
            _get_table(mock_con, "employees", "hr", "oracle")

        mock_con.sql.assert_called_once_with(
            'SELECT * FROM "HR"."EMPLOYEES"', schema=mock_schema
        )
        mock_table.rename.assert_called_once_with(str.lower)
        mock_con.table.assert_not_called()

    def test_oracle_uses_sql_without_schema(self) -> None:
        """Oracle backend without schema uses con.sql() with table only."""
        mock_con = MagicMock()
        mock_table = MagicMock()
        mock_con.sql.return_value = mock_table
        mock_schema = ibis.schema({"ID": "int64"})

        with patch(
            "datannurpy.scanner.database._oracle_get_schema",
            return_value=(mock_schema, set(), set()),
        ):
            _get_table(mock_con, "employees", None, "oracle")

        mock_con.sql.assert_called_once_with(
            'SELECT * FROM "EMPLOYEES"', schema=mock_schema
        )
        mock_table.rename.assert_called_once_with(str.lower)

    def test_oracle_uses_precomputed_schema(self) -> None:
        """Oracle backend skips _oracle_get_schema when oracle_schema is provided."""
        mock_con = MagicMock()
        mock_table = MagicMock()
        mock_con.sql.return_value = mock_table
        mock_schema = ibis.schema({"ID": "int64"})

        with patch(
            "datannurpy.scanner.database._oracle_get_schema",
        ) as mock_get_schema:
            _get_table(mock_con, "employees", None, "oracle", oracle_schema=mock_schema)

        mock_get_schema.assert_not_called()
        mock_con.sql.assert_called_once_with(
            'SELECT * FROM "EMPLOYEES"', schema=mock_schema
        )

    def test_non_oracle_uses_table_with_schema(self) -> None:
        """Non-Oracle backends use con.table() with schema."""
        mock_con = MagicMock()

        _get_table(mock_con, "employees", "public", "postgres")

        mock_con.table.assert_called_once_with("employees", database="public")
        mock_con.sql.assert_not_called()

    def test_non_oracle_uses_table_without_schema(self) -> None:
        """Non-Oracle backends use con.table() without schema."""
        mock_con = MagicMock()

        _get_table(mock_con, "employees", None, "postgres")

        mock_con.table.assert_called_once_with("employees")


class TestScanTable:
    """Tests for scan_table function."""

    _oracle_schema_patch = "datannurpy.scanner.database._oracle_get_schema"
    _mock_schema_result = (ibis.schema({"ID": "int64"}), set(), set())

    def _make_oracle_mock(self) -> tuple[MagicMock, MagicMock]:
        """Create a mock Oracle connection with con.sql() support."""
        mock_con = MagicMock()
        type(mock_con).__module__ = "ibis.backends.oracle"
        mock_table = MagicMock()
        mock_table.count.return_value.to_pyarrow.return_value.as_py.return_value = 0
        mock_table.rename.return_value = mock_table
        mock_con.sql.return_value = mock_table
        return mock_con, mock_table

    def test_oracle_uses_sql_with_schema(self) -> None:
        """Oracle backend uses con.sql() with explicit schema."""
        mock_con, _ = self._make_oracle_mock()

        with (
            patch(self._oracle_schema_patch, return_value=self._mock_schema_result),
            patch(
                "datannurpy.scanner.database.build_variables", return_value=([], None)
            ),
        ):
            scan_table(mock_con, "employees", schema="hr", dataset_id="test")

        mock_con.sql.assert_called_once()
        assert mock_con.sql.call_args[0][0] == 'SELECT * FROM "HR"."EMPLOYEES"'
        assert "schema" in mock_con.sql.call_args[1]
        mock_con.table.assert_not_called()

    def test_oracle_uses_sql_without_schema(self) -> None:
        """Oracle backend without schema uses con.sql()."""
        mock_con, _ = self._make_oracle_mock()

        with (
            patch(self._oracle_schema_patch, return_value=self._mock_schema_result),
            patch(
                "datannurpy.scanner.database.build_variables", return_value=([], None)
            ),
        ):
            scan_table(mock_con, "employees", dataset_id="test")

        mock_con.sql.assert_called_once()
        assert mock_con.sql.call_args[0][0] == 'SELECT * FROM "EMPLOYEES"'
        mock_con.table.assert_not_called()

    def test_oracle_skips_lob_detection_when_no_stats(self) -> None:
        """Oracle backend with infer_stats=False skips LOB column detection."""
        mock_con, _ = self._make_oracle_mock()

        with (
            patch(self._oracle_schema_patch, return_value=self._mock_schema_result),
            patch(
                "datannurpy.scanner.database.build_variables", return_value=([], None)
            ),
        ):
            scan_table(mock_con, "employees", dataset_id="test", infer_stats=False)

        mock_con.sql.assert_called_once()

    def test_oracle_metadata_failure_retries_in_get_table(self) -> None:
        """When _oracle_get_schema fails in scan_table, _get_table retries."""
        mock_con, _ = self._make_oracle_mock()
        call_count = 0

        def side_effect(
            *args: object, **kwargs: object
        ) -> tuple[ibis.Schema, set[str], set[str]]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("First call fails")
            return self._mock_schema_result

        with (
            patch(self._oracle_schema_patch, side_effect=side_effect),
            patch(
                "datannurpy.scanner.database.build_variables", return_value=([], None)
            ),
        ):
            scan_table(mock_con, "employees", dataset_id="test")

        assert (
            call_count == 2
        )  # First fails in scan_table, second succeeds in _get_table
        mock_con.sql.assert_called_once()

    def test_oracle_date_stats_computed_via_raw_sql(self) -> None:
        """Oracle date columns get stats from raw SQL fallback."""
        from datannurpy.schema import Variable

        mock_con, mock_table = self._make_oracle_mock()
        mock_table.count.return_value.to_pyarrow.return_value.as_py.return_value = 5

        date_schema = (
            ibis.schema({"ID": "int64", "CREATED": "timestamp"}),
            set(),
            {"created"},
        )
        var = Variable(id="created", name="created", dataset_id="ds", type="datetime")

        with (
            patch(self._oracle_schema_patch, return_value=date_schema),
            patch(
                "datannurpy.scanner.database.build_variables",
                return_value=([var], None),
            ),
            patch("datannurpy.scanner.database._oracle_patch_date_stats") as mock_patch,
        ):
            scan_table(mock_con, "events", dataset_id="ds")

        mock_patch.assert_called_once_with(mock_con, "events", None, {"created"}, [var])


class TestInitOracleClient:
    """Tests for _init_oracle_client function."""

    def test_calls_init_oracle_client(self) -> None:
        """First call initializes Oracle thick mode."""
        import datannurpy.scanner._oracle as oracle_mod

        oracle_mod._oracle_client_initialized = False
        with patch.dict("sys.modules", {"oracledb": MagicMock()}) as modules:
            _init_oracle_client("/opt/oracle/client", raise_driver_error)
            modules["oracledb"].init_oracle_client.assert_called_once_with(
                lib_dir="/opt/oracle/client"
            )
        oracle_mod._oracle_client_initialized = False

    def test_only_called_once(self) -> None:
        """Second call is a no-op."""
        import datannurpy.scanner._oracle as oracle_mod

        oracle_mod._oracle_client_initialized = False
        mock_oracledb = MagicMock()
        with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
            _init_oracle_client("/opt/oracle/client", raise_driver_error)
            _init_oracle_client("/other/path", raise_driver_error)
            mock_oracledb.init_oracle_client.assert_called_once()
        oracle_mod._oracle_client_initialized = False

    def test_missing_oracledb_raises(self) -> None:
        """Missing oracledb raises ImportError with clear message."""
        import datannurpy.scanner._oracle as oracle_mod

        oracle_mod._oracle_client_initialized = False
        with patch.dict("sys.modules", {"oracledb": None}):
            with pytest.raises(ConfigError, match="oracledb"):
                _init_oracle_client("/opt/oracle/client", raise_driver_error)
        oracle_mod._oracle_client_initialized = False
