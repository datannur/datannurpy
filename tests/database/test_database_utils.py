"""Database utility functions tests."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import ibis
import pytest

from datannurpy.errors import ConfigError
from datannurpy.scanner._oracle import (
    _init_oracle_client,
    _oracle_get_schema,
    _oracle_type_to_ibis,
)
from datannurpy.scanner.database import (
    _get_table,
    _tunnel_uri,
    connect,
    get_database_name,
    get_database_path,
    get_schemas_to_scan,
    get_table_data_size,
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


class TestTunnelUri:
    """Tests for _tunnel_uri helper."""

    def test_mysql_full(self) -> None:
        uri = "mysql://user:pass@dbhost:3306/mydb"
        assert _tunnel_uri(uri, 12345) == "mysql://user:pass@localhost:12345/mydb"

    def test_postgres_no_port(self) -> None:
        uri = "postgresql://user:pass@remote.host/mydb"
        assert _tunnel_uri(uri, 9999) == "postgresql://user:pass@localhost:9999/mydb"

    def test_no_password(self) -> None:
        uri = "mysql://user@dbhost/mydb"
        assert _tunnel_uri(uri, 5555) == "mysql://user@localhost:5555/mydb"

    def test_no_credentials(self) -> None:
        uri = "postgresql://dbhost:5432/mydb"
        assert _tunnel_uri(uri, 7777) == "postgresql://localhost:7777/mydb"


class TestOpenSshTunnel:
    """Tests for open_ssh_tunnel context manager."""

    def _run_tunnel(
        self, ssh_config: dict[str, Any], uri: str, local_port: int = 54321
    ) -> tuple[str, Any]:
        """Helper: open tunnel with mocked paramiko + socket, return (tunneled_uri, mock_client)."""
        mock_transport = MagicMock()
        mock_client = MagicMock()
        mock_client.get_transport.return_value = mock_transport

        mock_sock = MagicMock()
        mock_sock.getsockname.return_value = ("127.0.0.1", local_port)

        with (
            patch("datannurpy.scanner.database.paramiko") as mock_paramiko,
            patch("datannurpy.scanner.database.socket") as mock_socket_mod,
            patch("datannurpy.scanner.database.select") as mock_select,
        ):
            mock_paramiko.SSHClient.return_value = mock_client
            mock_paramiko.AutoAddPolicy.return_value = "auto_policy"
            mock_socket_mod.AF_INET = 2
            mock_socket_mod.SOCK_STREAM = 1
            mock_socket_mod.socket.return_value = mock_sock
            # select never returns readable sockets (no actual forwarding)
            mock_select.select.return_value = ([], [], [])

            from datannurpy.scanner.database import open_ssh_tunnel

            with open_ssh_tunnel(ssh_config, uri) as tunneled:
                result = tunneled
        return result, mock_client

    def test_opens_and_closes_tunnel(self) -> None:
        ssh_config: dict[str, Any] = {"host": "ssh.example.com", "user": "myuser"}
        tunneled, mock_client = self._run_tunnel(
            ssh_config, "mysql://dbuser:dbpass@dbhost/mydb"
        )
        assert tunneled == "mysql://dbuser:dbpass@localhost:54321/mydb"
        mock_client.connect.assert_called_once_with(
            hostname="ssh.example.com", port=22, username="myuser"
        )
        mock_client.close.assert_called_once()

    def test_with_password_and_port(self) -> None:
        ssh_config: dict[str, Any] = {
            "host": "ssh.host",
            "port": 2222,
            "user": "u",
            "password": "p",
        }
        tunneled, mock_client = self._run_tunnel(
            ssh_config, "postgresql://a:b@pghost:5432/db", local_port=11111
        )
        assert tunneled == "postgresql://a:b@localhost:11111/db"
        mock_client.connect.assert_called_once_with(
            hostname="ssh.host", port=2222, username="u", password="p"
        )

    def test_with_key_file(self) -> None:
        ssh_config: dict[str, Any] = {
            "host": "ssh.host",
            "user": "u",
            "key_file": "/home/u/.ssh/id_rsa",
        }
        tunneled, mock_client = self._run_tunnel(
            ssh_config, "mysql://a:b@dbhost/mydb", local_port=22222
        )
        assert tunneled == "mysql://a:b@localhost:22222/mydb"
        mock_client.connect.assert_called_once_with(
            hostname="ssh.host",
            port=22,
            username="u",
            key_filename="/home/u/.ssh/id_rsa",
        )

    def test_minimal_config(self) -> None:
        """SSH config with only host (no user/password/key)."""
        ssh_config: dict[str, Any] = {"host": "ssh.host"}
        tunneled, mock_client = self._run_tunnel(
            ssh_config, "mysql://a:b@dbhost/mydb", local_port=33333
        )
        assert tunneled == "mysql://a:b@localhost:33333/mydb"
        mock_client.connect.assert_called_once_with(hostname="ssh.host", port=22)


class TestAddDatabaseSshTunnel:
    """Tests for add_database with ssh_tunnel parameter."""

    def test_ssh_tunnel_calls_open_ssh_tunnel(self) -> None:
        """add_database with ssh_tunnel opens a tunnel and scans the tunneled URI."""
        from datannurpy import Catalog
        from datannurpy.add_database import add_database

        catalog = Catalog(quiet=True, refresh=True)
        ssh_config = {"host": "ssh.example.com", "user": "u"}
        with (
            patch("datannurpy.add_database.open_ssh_tunnel") as mock_tunnel,
            patch("datannurpy.add_database._add_database_impl") as mock_impl,
        ):
            mock_tunnel.return_value.__enter__ = MagicMock(
                return_value="mysql://user:pass@localhost:54321/mydb"
            )
            mock_tunnel.return_value.__exit__ = MagicMock(return_value=False)
            add_database(
                catalog,
                "mysql://user:pass@dbhost/mydb",
                ssh_tunnel=ssh_config,
            )
            mock_tunnel.assert_called_once_with(
                ssh_config, "mysql://user:pass@dbhost/mydb"
            )
            mock_impl.assert_called_once()
            call_args = mock_impl.call_args
            assert call_args[0][1] == "mysql://user:pass@localhost:54321/mydb"
            assert call_args[1]["remote_path"] == "mysql://user:pass@dbhost/mydb"


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

    def test_oracle_appends_none_when_user_not_in_schemas(self) -> None:
        """Oracle appends None when connected user is not in schemas list."""
        mock_con = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("ADMIN",)
        mock_con.raw_sql.return_value = mock_cursor
        with patch(
            "datannurpy.scanner.database.list_schemas", return_value=["hr", "sales"]
        ):
            result = get_schemas_to_scan(mock_con, None, "oracle")
        assert result == ["hr", "sales", None]

    def test_oracle_skips_none_when_user_already_in_schemas(self) -> None:
        """Oracle does not append None when connected user is already listed."""
        mock_con = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("HR",)
        mock_con.raw_sql.return_value = mock_cursor
        with patch(
            "datannurpy.scanner.database.list_schemas", return_value=["hr", "sales"]
        ):
            result = get_schemas_to_scan(mock_con, None, "oracle")
        assert result == ["hr", "sales"]
        assert None not in result

    def test_oracle_no_raw_sql_appends_none(self) -> None:
        """Oracle without raw_sql falls back to appending None."""
        mock_con = MagicMock()
        mock_con.raw_sql = None
        with patch("datannurpy.scanner.database.list_schemas", return_value=["hr"]):
            result = get_schemas_to_scan(mock_con, None, "oracle")
        assert result == ["hr", None]

    def test_oracle_null_fetchone_appends_none(self) -> None:
        """Oracle with fetchone() returning None falls back to appending None."""
        mock_con = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_con.raw_sql.return_value = mock_cursor
        with patch("datannurpy.scanner.database.list_schemas", return_value=["hr"]):
            result = get_schemas_to_scan(mock_con, None, "oracle")
        assert result == ["hr", None]

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

    def test_oracle_sample_pct_with_schema(self) -> None:
        """Oracle SAMPLE clause is appended when sample_pct is provided."""
        mock_con = MagicMock()
        mock_table = MagicMock()
        mock_con.sql.return_value = mock_table
        mock_schema = ibis.schema({"ID": "int64"})

        _get_table(
            mock_con,
            "employees",
            "hr",
            "oracle",
            oracle_schema=mock_schema,
            sample_pct=83.05,
        )

        mock_con.sql.assert_called_once_with(
            'SELECT * FROM "HR"."EMPLOYEES" SAMPLE(83.05)', schema=mock_schema
        )

    def test_oracle_sample_pct_without_schema(self) -> None:
        """Oracle SAMPLE clause works without schema."""
        mock_con = MagicMock()
        mock_table = MagicMock()
        mock_con.sql.return_value = mock_table
        mock_schema = ibis.schema({"ID": "int64"})

        _get_table(
            mock_con,
            "employees",
            None,
            "oracle",
            oracle_schema=mock_schema,
            sample_pct=50.0,
        )

        mock_con.sql.assert_called_once_with(
            'SELECT * FROM "EMPLOYEES" SAMPLE(50.0)', schema=mock_schema
        )


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

    def test_oracle_date_columns_skipped_for_stats(self) -> None:
        """Oracle date columns are added to skip_stats_columns (no raw SQL fallback)."""
        mock_con, mock_table = self._make_oracle_mock()
        mock_table.count.return_value.to_pyarrow.return_value.as_py.return_value = 5

        date_schema = (
            ibis.schema({"ID": "int64", "CREATED": "timestamp"}),
            set(),
            {"created"},
        )

        with (
            patch(self._oracle_schema_patch, return_value=date_schema),
            patch(
                "datannurpy.scanner.database.build_variables",
                return_value=([], None),
            ) as mock_bv,
        ):
            scan_table(mock_con, "events", dataset_id="ds")

        # date columns should be in skip_stats_columns
        call_kwargs = mock_bv.call_args[1]
        assert "created" in call_kwargs["skip_stats_columns"]

    def test_oracle_sampling_uses_sample_clause(self) -> None:
        """Oracle sampling uses SAMPLE(pct) instead of ibis.random()."""
        import pandas as pd

        mock_con, mock_table = self._make_oracle_mock()
        mock_table.count.return_value.to_pyarrow.return_value.as_py.return_value = (
            200_000
        )

        # The sampled table returned by the second _get_table call
        mock_sampled = MagicMock()
        mock_sampled.rename.return_value = mock_sampled
        mock_sampled.execute.return_value = pd.DataFrame({"id": [1, 2, 3]})

        # First call returns the full table, second returns sampled
        mock_con.sql.side_effect = [mock_table, mock_sampled]

        with (
            patch(self._oracle_schema_patch, return_value=self._mock_schema_result),
            patch(
                "datannurpy.scanner.database.build_variables",
                return_value=([], None),
            ),
            patch("datannurpy.scanner.database.ibis.memtable"),
        ):
            scan_table(
                mock_con,
                "employees",
                dataset_id="test",
                sample_size=100_000,
                row_count=200_000,
            )

        # Second sql() call should include SAMPLE clause
        assert mock_con.sql.call_count == 2
        second_call_sql = mock_con.sql.call_args_list[1][0][0]
        assert "SAMPLE(" in second_call_sql
        assert "50.0" in second_call_sql


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


class TestGetTableDataSize:
    """Tests for get_table_data_size per-engine dispatch."""

    def _mock_con(self, result: list[tuple[object, ...]]) -> MagicMock:
        mock = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = result[0] if result else None
        mock.raw_sql.return_value = mock_cursor
        return mock

    def test_sqlite(self) -> None:
        con = self._mock_con([(8192,)])
        with patch(
            "datannurpy.scanner.database.get_backend_name", return_value="sqlite"
        ):
            assert get_table_data_size(con, "users", None) == 8192

    def test_postgres_no_schema(self) -> None:
        con = self._mock_con([(16384,)])
        with patch(
            "datannurpy.scanner.database.get_backend_name", return_value="postgres"
        ):
            assert get_table_data_size(con, "orders", None) == 16384
        query = con.raw_sql.call_args[0][0]
        assert "pg_total_relation_size" in query
        assert "'orders'" in query

    def test_postgres_with_schema(self) -> None:
        con = self._mock_con([(32768,)])
        with patch(
            "datannurpy.scanner.database.get_backend_name", return_value="postgres"
        ):
            assert get_table_data_size(con, "orders", "sales") == 32768
        query = con.raw_sql.call_args[0][0]
        assert "'sales.orders'" in query

    def test_mysql_no_schema(self) -> None:
        con = self._mock_con([(4096,)])
        with patch(
            "datannurpy.scanner.database.get_backend_name", return_value="mysql"
        ):
            assert get_table_data_size(con, "items", None) == 4096
        query = con.raw_sql.call_args[0][0]
        assert "information_schema.tables" in query
        assert "table_schema" not in query

    def test_mysql_with_schema(self) -> None:
        con = self._mock_con([(4096,)])
        with patch(
            "datannurpy.scanner.database.get_backend_name", return_value="mysql"
        ):
            assert get_table_data_size(con, "items", "shop") == 4096
        query = con.raw_sql.call_args[0][0]
        assert "table_schema = 'shop'" in query

    def test_mssql(self) -> None:
        con = self._mock_con([(65536,)])
        with patch(
            "datannurpy.scanner.database.get_backend_name", return_value="mssql"
        ):
            assert get_table_data_size(con, "logs", None) == 65536
        query = con.raw_sql.call_args[0][0]
        assert "sys.partitions" in query

    def test_oracle_no_schema(self) -> None:
        con = self._mock_con([(131072,)])
        with patch(
            "datannurpy.scanner.database.get_backend_name", return_value="oracle"
        ):
            assert get_table_data_size(con, "employees", None) == 131072
        query = con.raw_sql.call_args[0][0]
        assert "user_segments" in query
        assert "'EMPLOYEES'" in query

    def test_oracle_with_schema(self) -> None:
        con = self._mock_con([(131072,)])
        with patch(
            "datannurpy.scanner.database.get_backend_name", return_value="oracle"
        ):
            assert get_table_data_size(con, "employees", "hr") == 131072
        query = con.raw_sql.call_args_list[0][0][0]
        assert "all_segments" in query
        assert "'HR'" in query

    def test_oracle_with_schema_fallback_to_user_segments(self) -> None:
        """Oracle falls back to user_segments when all_segments returns NULL."""
        mock_cursor_null = MagicMock()
        mock_cursor_null.fetchone.return_value = (None,)
        mock_cursor_ok = MagicMock()
        mock_cursor_ok.fetchone.return_value = (131072,)
        con = MagicMock()
        con.raw_sql.side_effect = [mock_cursor_null, mock_cursor_ok]
        with patch(
            "datannurpy.scanner.database.get_backend_name", return_value="oracle"
        ):
            assert get_table_data_size(con, "employees", "hr") == 131072
        assert con.raw_sql.call_count == 2
        assert "all_segments" in con.raw_sql.call_args_list[0][0][0]
        assert "user_segments" in con.raw_sql.call_args_list[1][0][0]

    def test_no_raw_sql_returns_none(self) -> None:
        con = MagicMock()
        con.raw_sql = None
        with patch(
            "datannurpy.scanner.database.get_backend_name", return_value="sqlite"
        ):
            assert get_table_data_size(con, "t", None) is None

    def test_unknown_backend_returns_none(self) -> None:
        con = self._mock_con([(0,)])
        with patch(
            "datannurpy.scanner.database.get_backend_name", return_value="unknown"
        ):
            assert get_table_data_size(con, "t", None) is None

    def test_exception_returns_none(self) -> None:
        con = MagicMock()
        con.raw_sql.side_effect = Exception("dbstat not available")
        with patch(
            "datannurpy.scanner.database.get_backend_name", return_value="sqlite"
        ):
            assert get_table_data_size(con, "t", None) is None

    def test_null_result_returns_none(self) -> None:
        con = self._mock_con([(None,)])
        with patch(
            "datannurpy.scanner.database.get_backend_name", return_value="sqlite"
        ):
            assert get_table_data_size(con, "empty_table", None) is None
