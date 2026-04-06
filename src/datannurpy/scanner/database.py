"""Database reader using Ibis backends."""

from __future__ import annotations

import fnmatch
import hashlib
import select
import socket
import threading
import warnings
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn
from urllib.parse import parse_qs, quote, unquote, urlparse, urlunparse

import ibis
import paramiko
import pyarrow as pa

from ..errors import ConfigError
from ..schema import Variable
from ._oracle import (
    ORACLE_SYSTEM_TABLE_PREFIXES,
    _init_oracle_client,
    _oracle_get_schema,
)
from .utils import build_variables

if TYPE_CHECKING:
    from collections.abc import Sequence


# Backend name mapping from URI scheme
SCHEME_TO_BACKEND: dict[str, str] = {
    "sqlite": "sqlite",
    "postgresql": "postgres",
    "postgres": "postgres",
    "mysql": "mysql",
    "oracle": "oracle",
    "mssql": "mssql",
}

# Default ports per backend (used for SSH tunnel)
DEFAULT_PORTS: dict[str, int] = {
    "postgres": 5432,
    "mysql": 3306,
    "oracle": 1521,
    "mssql": 1433,
}


def _quote(value: str) -> str:
    """Escape single quotes for safe SQL interpolation."""
    return value.replace("'", "''")


# System schemas to exclude when scanning (per backend)
SYSTEM_SCHEMAS: dict[str, set[str]] = {
    "postgres": {
        "information_schema",
        "pg_catalog",
        "pg_toast",
    },
    "mysql": {
        "information_schema",
        "mysql",
        "performance_schema",
        "sys",
    },
    "duckdb": {
        "information_schema",
    },
    "mssql": {
        "information_schema",
        "sys",
        "guest",
        "INFORMATION_SCHEMA",
        "db_owner",
        "db_accessadmin",
        "db_securityadmin",
        "db_ddladmin",
        "db_backupoperator",
        "db_datareader",
        "db_datawriter",
        "db_denydatareader",
        "db_denydatawriter",
    },
    "oracle": {
        "SYS",
        "SYSTEM",
        "OUTLN",
        "DBSNMP",
        "APPQOSSYS",
        "AUDSYS",
        "DBSFWUSER",
        "DGPDB_INT",
        "GGSYS",
        "ANONYMOUS",
        "CTXSYS",
        "DVSYS",
        "DVF",
        "GSMADMIN_INTERNAL",
        "MDSYS",
        "OJVMSYS",
        "OLAPSYS",
        "LBACSYS",
        "REMOTE_SCHEDULER_AGENT",
        "SYSBACKUP",
        "SYSDG",
        "SYSKM",
        "SYSRAC",
        "SYS$UMF",
        "XDB",
        "XS$NULL",
        "WMSYS",
        "ORDDATA",
        "ORDPLUGINS",
        "ORDSYS",
        "SI_INFORMTN_SCHEMA",
    },
}

# SQLite system table prefixes to exclude (GeoPackage metadata, rtree indexes)
SQLITE_SYSTEM_TABLE_PREFIXES: tuple[str, ...] = (
    "gpkg_",  # GeoPackage metadata tables
    "rtree_",  # R-tree spatial index tables
)


def get_backend_name(con: ibis.BaseBackend) -> str:
    """Get backend name from connection object."""
    return type(con).__module__.split(".")[-1]


def close_connection(con: ibis.BaseBackend) -> None:
    """Close an Ibis connection properly."""
    # Try standard disconnect first
    con.disconnect()
    # SQLite backend doesn't implement disconnect(), close internal connection
    if hasattr(con, "con"):
        internal_con = getattr(con, "con")
        if hasattr(internal_con, "close"):
            try:
                internal_con.close()
            except Exception:
                pass  # Already closed (Oracle, etc.)


def _encode_uri_credentials(uri: str) -> str:
    """URL-encode credentials in a database URI so urlparse handles special chars."""
    scheme_end = uri.find("://")
    if scheme_end == -1:
        return uri
    rest = uri[scheme_end + 3 :]
    at_idx = rest.rfind("@")
    if at_idx == -1:
        return uri
    userinfo = rest[:at_idx]
    colon_idx = userinfo.find(":")
    if colon_idx == -1:
        return uri
    user = quote(userinfo[:colon_idx], safe="")
    password = quote(userinfo[colon_idx + 1 :], safe="")
    return f"{uri[:scheme_end]}://{user}:{password}@{rest[at_idx + 1 :]}"


def _tunnel_uri(uri: str, local_port: int) -> str:
    """Replace host:port in a database URI with localhost:local_port."""
    parsed = urlparse(_encode_uri_credentials(uri))
    userinfo = ""
    if parsed.username and parsed.password:
        userinfo = f"{unquote(parsed.username)}:{unquote(parsed.password)}@"
    elif parsed.username:
        userinfo = f"{unquote(parsed.username)}@"
    new_netloc = f"{userinfo}localhost:{local_port}"
    return urlunparse(parsed._replace(netloc=new_netloc))


def sanitize_connection_url(uri: str) -> str:
    """Strip credentials and query params from a database URI for safe storage."""
    parsed = urlparse(_encode_uri_credentials(uri))
    if not parsed.username and not parsed.query:
        return uri
    netloc = parsed.hostname or ""
    if parsed.port:
        netloc = f"{netloc}:{parsed.port}"
    return urlunparse(parsed._replace(netloc=netloc, query="", fragment=""))


@contextmanager
def open_ssh_tunnel(
    ssh_config: dict[str, Any],
    connection: str,
) -> Any:
    """Open an SSH tunnel to a database and yield the tunneled URI."""
    backend, kwargs = parse_connection_string(connection)
    remote_host = kwargs.get("host", "localhost")
    remote_port = int(kwargs.get("port", DEFAULT_PORTS.get(backend, 3306)))

    ssh_host = ssh_config["host"]
    ssh_port = int(ssh_config.get("port", 22))

    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())

    connect_kwargs: dict[str, Any] = {
        "hostname": ssh_host,
        "port": ssh_port,
    }
    if "user" in ssh_config:
        connect_kwargs["username"] = ssh_config["user"]
    if "password" in ssh_config:
        connect_kwargs["password"] = ssh_config["password"]
    if "key_file" in ssh_config:
        connect_kwargs["key_filename"] = ssh_config["key_file"]

    try:
        client.connect(**connect_kwargs)
    except paramiko.SSHException as exc:
        client.close()
        msg = (
            f"SSH host key verification failed for '{ssh_host}': {exc}. "
            f"Connect once manually (ssh {ssh_host}) to add the key "
            f"to your known_hosts file, then retry."
        )
        raise ConfigError(msg) from exc
    transport = client.get_transport()
    assert transport is not None

    # Bind a local socket to get a free port
    local_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    local_sock.bind(("127.0.0.1", 0))
    local_sock.listen(1)
    local_port = local_sock.getsockname()[1]

    stop_event = threading.Event()

    def _forward() -> None:  # pragma: no cover  # pragma: no cover
        while not stop_event.is_set():
            readable, _, _ = select.select([local_sock], [], [], 0.5)
            if local_sock in readable:
                conn, _ = local_sock.accept()
                channel = transport.open_channel(
                    "direct-tcpip",
                    (remote_host, remote_port),
                    conn.getpeername(),
                )
                if channel is None:
                    conn.close()
                    continue
                while True:
                    r, _, _ = select.select([conn, channel], [], [], 1.0)
                    if conn in r:
                        data = conn.recv(4096)
                        if not data:
                            break
                        channel.sendall(data)
                    if channel in r:
                        data = channel.recv(4096)
                        if not data:
                            break
                        conn.sendall(data)
                channel.close()
                conn.close()

    thread = threading.Thread(target=_forward, daemon=True)
    thread.start()

    try:
        yield _tunnel_uri(connection, local_port)
    finally:
        stop_event.set()
        thread.join(timeout=2)
        local_sock.close()
        client.close()


def raise_driver_error(backend: str, original_error: Exception) -> NoReturn:
    """Raise clear error message for missing database drivers."""
    messages = {
        "postgres": (
            "PostgreSQL support requires optional dependencies. "
            "Install with: pip install datannurpy[postgres]"
        ),
        "mysql": (
            "MySQL support requires optional dependencies. "
            "Install with: pip install datannurpy[mysql]"
        ),
        "oracle": (
            "Oracle support requires optional dependencies. "
            "Install with: pip install datannurpy[oracle]"
        ),
        "mssql": (
            "SQL Server support requires optional dependencies. "
            "Install with: pip install datannurpy[mssql]\n"
            "ODBC driver: macOS: brew install freetds | "
            "Linux: apt install tdsodbc | "
            "Windows: install Microsoft ODBC Driver for SQL Server"
        ),
    }
    msg = messages.get(backend, f"Missing driver for {backend}")
    raise ConfigError(msg) from original_error


def _is_missing_backend_dependency_error(backend: str, error: Exception) -> bool:
    """Return True when an exception indicates a missing optional backend dep."""
    if isinstance(error, (ModuleNotFoundError, ImportError)):
        return True

    msg = str(error).lower()
    return (
        f"failed to import the {backend} backend due to missing dependencies" in msg
        or "due to missing dependencies" in msg
        and backend in msg
    )


def parse_connection_string(connection: str) -> tuple[str, dict[str, str]]:
    """Parse a connection string into (backend_name, kwargs)."""
    parsed = urlparse(_encode_uri_credentials(connection))
    scheme = parsed.scheme.lower()

    backend = SCHEME_TO_BACKEND.get(scheme)
    if backend is None:
        supported = ", ".join(sorted(SCHEME_TO_BACKEND.keys()))
        raise ConfigError(
            f"Unsupported database scheme: {scheme!r}. Supported: {supported}"
        )

    kwargs: dict[str, str] = {}

    if backend == "sqlite":
        # Strip leading / from path (sqlite:///path -> /path, sqlite:////abs -> //abs)
        path = parsed.path[1:] if parsed.path.startswith("/") else parsed.path
        kwargs["path"] = path if path else ":memory:"
    else:
        # PostgreSQL / MySQL / Oracle
        if parsed.hostname:
            kwargs["host"] = parsed.hostname
        if parsed.port:
            kwargs["port"] = str(parsed.port)
        if parsed.username:
            kwargs["user"] = unquote(parsed.username)
        if parsed.password:
            kwargs["password"] = unquote(parsed.password)
        if parsed.path and parsed.path != "/":
            kwargs["database"] = parsed.path.lstrip("/")

        # Parse query string for additional params
        if parsed.query:
            query_params = parse_qs(parsed.query)
            for key, values in query_params.items():
                kwargs[key] = values[0]

    return backend, kwargs


def is_remote_database_file(connection: str) -> bool:
    """Check if connection is a remote file URL (sftp://, s3://, etc.), not a database URL."""
    if "://" not in connection:
        return False
    scheme = urlparse(connection).scheme.lower()
    return scheme not in SCHEME_TO_BACKEND and scheme not in ("file", "duckdb")


def _connect_external_backend(
    backend: str,
    kwargs: dict[str, str],
    *,
    oracle_client_path: str | None = None,
) -> ibis.BaseBackend:  # pragma: no cover
    """Connect to external database backends (requires drivers)."""
    try:
        if backend == "postgres":
            return ibis.postgres.connect(
                host=kwargs.get("host", "localhost"),
                port=int(kwargs.get("port", 5432)),
                user=kwargs.get("user"),
                password=kwargs.get("password"),
                database=kwargs.get("database"),
            )
        if backend == "mysql":
            known_mysql = {"host", "port", "user", "password", "database"}
            mysql_kwargs: dict[str, str | int] = {
                "host": kwargs.get("host", "localhost"),
                "port": int(kwargs.get("port", 3306)),
            }
            if kwargs.get("user"):
                mysql_kwargs["user"] = kwargs["user"]
            if kwargs.get("password"):
                mysql_kwargs["password"] = kwargs["password"]
            if kwargs.get("database"):
                mysql_kwargs["database"] = kwargs["database"]
            for key, value in kwargs.items():
                if key not in known_mysql:
                    mysql_kwargs[key] = value
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore", message="Unable to set session timezone"
                )
                return ibis.mysql.connect(**mysql_kwargs)
        if backend == "oracle":
            if oracle_client_path:
                _init_oracle_client(oracle_client_path, raise_driver_error)
            host = kwargs.get("host")
            database = kwargs.get("database")
            # No host → TNS name resolved via tnsnames.ora / LDAP (thick mode)
            if not host and database:
                return ibis.oracle.connect(
                    user=kwargs.get("user"),
                    password=kwargs.get("password"),
                    dsn=database,
                )
            return ibis.oracle.connect(
                host=host or "localhost",
                port=int(kwargs.get("port", 1521)),
                user=kwargs.get("user"),
                password=kwargs.get("password"),
                database=database,
            )
        # mssql
        known_params = {"host", "port", "user", "password", "database", "driver"}
        mssql_kwargs: dict[str, str | int] = {
            "host": kwargs.get("host", "localhost"),
            "port": int(kwargs.get("port", 1433)),
        }
        if kwargs.get("user"):
            mssql_kwargs["user"] = kwargs["user"]
        if kwargs.get("password"):
            mssql_kwargs["password"] = kwargs["password"]
        if kwargs.get("database"):
            mssql_kwargs["database"] = kwargs["database"]
        if kwargs.get("driver"):
            mssql_kwargs["driver"] = kwargs["driver"]
        for key, value in kwargs.items():
            if key not in known_params:
                mssql_kwargs[key] = value
        return ibis.mssql.connect(**mssql_kwargs)
    except ModuleNotFoundError as e:
        raise_driver_error(backend, e)
    except Exception as e:
        if _is_missing_backend_dependency_error(backend, e):
            raise_driver_error(backend, e)
        host = kwargs.get("host", "localhost")
        port = kwargs.get("port")
        target = f"{host}:{port}" if port else host
        raise ConfigError(f"Failed to connect to {backend} ({target}): {e}") from e


def connect(
    connection: str | ibis.BaseBackend,
    *,
    oracle_client_path: str | None = None,
) -> tuple[ibis.BaseBackend, str]:
    """Connect to a database, return (connection, backend_name)."""
    if isinstance(connection, ibis.BaseBackend):
        backend_name = get_backend_name(connection)
        if backend_name in ("pyspark", "datafusion", "polars"):
            raise ConfigError(
                f"Backend {backend_name!r} is not supported for database scanning. "
                "Use sqlite, postgres, mysql, oracle, mssql, or duckdb."
            )
        return connection, backend_name

    backend, kwargs = parse_connection_string(connection)

    if backend == "sqlite":
        con = ibis.sqlite.connect(kwargs.get("path", ":memory:"))
    else:
        con = _connect_external_backend(
            backend, kwargs, oracle_client_path=oracle_client_path
        )

    return con, backend


def get_database_name(
    connection: str | ibis.BaseBackend,
    con: ibis.BaseBackend,
    backend_name: str,
) -> str:
    """Extract database name from connection."""
    if isinstance(connection, str):
        parsed = urlparse(connection)
        if backend_name == "sqlite":
            path = parsed.netloc + parsed.path if parsed.netloc else parsed.path
            return Path(path).stem or "sqlite"
        else:
            return parsed.path.lstrip("/") or backend_name
    # For connection objects, use current_database or fallback to backend name
    db_name = getattr(con, "current_database", None)
    # SQLite returns "main" which isn't useful, use backend_name instead
    if db_name and db_name != "main":
        return str(db_name)
    return backend_name


def get_database_path(
    connection: str,
    backend_name: str,
) -> str | None:
    """Get file path for file-based databases (SQLite, DuckDB)."""
    if backend_name not in ("sqlite", "duckdb"):
        return None

    _, kwargs = parse_connection_string(connection)
    path = kwargs.get("path", "")

    if path and path != ":memory:":
        return str(Path(path).resolve())

    return None


def get_schemas_to_scan(
    con: ibis.BaseBackend,
    schema: str | None,
    backend_name: str,
) -> list[str | None]:
    """Determine which schemas to scan."""
    if schema is not None:
        return [schema]

    if backend_name not in SYSTEM_SCHEMAS:
        return [None]

    available = list_schemas(con)
    system = SYSTEM_SCHEMAS[backend_name]
    schemas: list[str | None] = [s for s in available if s not in system]
    if backend_name == "oracle":
        # Append None (user_tables) only if the connected user isn't already
        # in the schemas list — otherwise their tables would be scanned twice.
        raw_sql = getattr(con, "raw_sql", None)
        current_user = None
        if raw_sql:
            row = raw_sql("SELECT USER FROM DUAL").fetchone()
            if row:
                current_user = row[0].lower()
        if current_user not in schemas:
            schemas.append(None)
    elif not schemas:
        schemas = [None]
    return schemas


def match_patterns(items: list[str], patterns: Sequence[str]) -> set[str]:
    """Match items against glob patterns."""
    matched: set[str] = set()
    for pattern in patterns:
        if "*" in pattern or "?" in pattern:
            matched.update(fnmatch.filter(items, pattern))
        elif pattern in items:
            matched.add(pattern)
    return matched


def list_tables(
    con: ibis.BaseBackend,
    schema: str | None = None,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    backend_name: str | None = None,
) -> list[str]:
    """List tables in a database, filtered by include/exclude patterns. Views excluded."""
    backend = backend_name or get_backend_name(con)

    # Oracle stores unquoted identifiers in UPPERCASE
    db_schema = schema.upper() if schema and backend == "oracle" else schema

    # Get tables - use backend-specific queries to filter views and system tables
    raw_sql = getattr(con, "raw_sql", None)
    tables: list[str] = []

    if raw_sql and backend == "oracle":
        # Use USER_TABLES/ALL_TABLES to get only tables (excludes views)
        # Normalize to lowercase since Oracle stores identifiers in UPPERCASE
        if db_schema:
            result = raw_sql(
                "SELECT table_name FROM all_tables WHERE owner = :owner",
                parameters={"owner": db_schema},
            ).fetchall()
        else:
            result = raw_sql("SELECT table_name FROM user_tables").fetchall()
        tables = [row[0].lower() for row in result]
        # Filter out Oracle system tables (MVIEW$_*, OL$*, SCHEDULER_*, etc.)
        tables = [
            t
            for t in tables
            if not any(t.startswith(prefix) for prefix in ORACLE_SYSTEM_TABLE_PREFIXES)
        ]
    elif raw_sql and backend == "sqlite":
        result = raw_sql(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        tables = [row[0] for row in result]
        # Filter out GeoPackage/rtree system tables
        tables = [
            t
            for t in tables
            if not any(t.startswith(prefix) for prefix in SQLITE_SYSTEM_TABLE_PREFIXES)
        ]
    elif raw_sql and backend in ("duckdb", "postgres", "mysql", "mssql"):
        # Use information_schema (standard SQL)
        query = (
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_type = 'BASE TABLE'"
        )
        if schema:
            query += f" AND table_schema = '{_quote(schema)}'"
        result = raw_sql(query).fetchall()
        tables = [row[0] for row in result]
    else:
        # Fallback to Ibis list_tables
        tables = list(
            con.list_tables(database=db_schema) if db_schema else con.list_tables()
        )

    if include is not None:
        included = match_patterns(tables, include)
        tables = [t for t in tables if t in included]

    if exclude is not None:
        excluded = match_patterns(tables, exclude)
        tables = [t for t in tables if t not in excluded]

    return sorted(tables)


def list_schemas(con: ibis.BaseBackend) -> list[str]:
    """List schemas in a database (postgres/mysql only)."""
    backend = get_backend_name(con)
    system_schemas = SYSTEM_SCHEMAS.get(backend, set())

    # Oracle: use raw SQL to list user schemas (more reliable than Ibis)
    if backend == "oracle":
        raw_sql = getattr(con, "raw_sql", None)
        if not raw_sql:
            return []
        # List all users that have at least one table (user schemas)
        result = raw_sql(
            "SELECT DISTINCT owner FROM all_tables "
            "WHERE owner NOT IN ("
            + ",".join(f"'{_quote(s)}'" for s in system_schemas)
            + ")"
        ).fetchall()
        return sorted([row[0].lower() for row in result])

    # Try to get schemas - not all backends support this
    try:
        list_schemas_fn = getattr(con, "list_schemas", None)
        if list_schemas_fn:
            schemas = list(list_schemas_fn())
            schemas = [s for s in schemas if s not in system_schemas]
            return schemas
        list_databases_fn = getattr(con, "list_databases", None)
        if list_databases_fn:
            return list(list_databases_fn())
    except Exception:
        pass
    return []


def build_table_data_path(
    backend_name: str,
    db_name: str,
    schema: str | None,
    table_name: str,
) -> str:
    """Build a unique data_path for a database table."""
    if schema:
        return f"{backend_name}://{db_name}/{schema}/{table_name}"
    return f"{backend_name}://{db_name}/{table_name}"


def _get_table(
    con: ibis.BaseBackend,
    table_name: str,
    schema: str | None,
    backend: str,
    *,
    oracle_schema: ibis.Schema | None = None,
    sample_pct: float | None = None,
) -> ibis.expr.types.Table:
    """Get an ibis table reference, with Oracle < 23 compatibility."""
    if backend == "oracle":
        # Use con.sql() with explicit schema instead of con.table() to avoid
        # ibis generating boolean expressions (Oracle < 23) or creating
        # temporary views for schema inference (cross-schema access).
        # sql() lives on SQLBackend, not BaseBackend, so use getattr.
        if oracle_schema is None:
            oracle_schema, _, _ = _oracle_get_schema(con, table_name, schema)
        sql_method = getattr(con, "sql")
        uc_table = table_name.upper().replace('"', '""')
        if schema:
            uc_schema = schema.upper().replace('"', '""')
            qualified = f'"{uc_schema}"."{uc_table}"'
        else:
            qualified = f'"{uc_table}"'
        sample_clause = f" SAMPLE({sample_pct})" if sample_pct is not None else ""
        table = sql_method(
            f"SELECT * FROM {qualified}{sample_clause}", schema=oracle_schema
        )
        return table.rename(str.lower)

    if schema:
        return con.table(table_name, database=schema)
    return con.table(table_name)


def _normalize_dtype(dtype: ibis.expr.datatypes.DataType) -> str:
    """Normalize dtype string for stable hashing across ibis/backend versions."""
    s = str(dtype)
    if s.startswith("unknown"):
        return "unknown"
    return s


def compute_schema_signature(
    con: ibis.BaseBackend, table_name: str, schema: str | None
) -> str:
    """Compute a hash signature of the table schema (column names and types)."""
    backend = get_backend_name(con)
    table = _get_table(con, table_name, schema, backend)

    # Build schema string from column names and types
    # Sort by column name for consistency
    # Normalize unknown(...) types to "unknown" for cross-version stability
    schema_parts = sorted(
        f"{col}:{_normalize_dtype(dtype)}" for col, dtype in table.schema().items()
    )
    schema_str = "|".join(schema_parts)

    # Return MD5 hash (fast, collision-resistant enough for this use case)
    return hashlib.md5(schema_str.encode()).hexdigest()


def get_table_row_count(
    con: ibis.BaseBackend, table_name: str, schema: str | None
) -> int:
    """Get row count for a table."""
    backend = get_backend_name(con)
    table = _get_table(con, table_name, schema, backend)
    return int(table.count().to_pyarrow().as_py())


def get_table_data_size(
    con: ibis.BaseBackend, table_name: str, schema: str | None
) -> int | None:
    """Get table size in bytes, or None if unsupported."""
    backend = get_backend_name(con)
    raw_sql = getattr(con, "raw_sql", None)
    if raw_sql is None:
        return None
    try:
        if backend == "sqlite":
            row = raw_sql(
                f"SELECT SUM(pgsize) FROM dbstat WHERE name = '{_quote(table_name)}'"
            ).fetchone()
            return int(row[0]) if row and row[0] is not None else None
        if backend == "postgres":
            qualified = (
                f"'{_quote(schema)}.{_quote(table_name)}'"
                if schema
                else f"'{_quote(table_name)}'"
            )
            row = raw_sql(f"SELECT pg_total_relation_size({qualified})").fetchone()
            return int(row[0]) if row and row[0] is not None else None
        if backend == "mysql":
            query = (
                "SELECT data_length + index_length "
                "FROM information_schema.tables "
                f"WHERE table_name = '{_quote(table_name)}'"
            )
            if schema:
                query += f" AND table_schema = '{_quote(schema)}'"
            row = raw_sql(query).fetchone()
            return int(row[0]) if row and row[0] is not None else None
        if backend == "mssql":
            qualified = (
                f"'{_quote(schema)}.{_quote(table_name)}'"
                if schema
                else f"'{_quote(table_name)}'"
            )
            row = raw_sql(
                "SELECT SUM(a.total_pages) * 8 * 1024 "
                "FROM sys.partitions p "
                "JOIN sys.allocation_units a "
                "ON p.partition_id = a.container_id "
                f"WHERE p.object_id = OBJECT_ID({qualified})"
            ).fetchone()
            return int(row[0]) if row and row[0] is not None else None
        if backend == "oracle":
            uc_table = table_name.upper()
            if schema:
                uc_schema = schema.upper()
                row = raw_sql(
                    "SELECT SUM(bytes) FROM all_segments "
                    "WHERE segment_name = :seg AND owner = :owner",
                    parameters={"seg": uc_table, "owner": uc_schema},
                ).fetchone()
                if row and row[0] is not None:
                    return int(row[0])
                # Fallback: user_segments works for the connected user's tables
                # when all_segments returns NULL (insufficient privileges).
            row = raw_sql(
                "SELECT SUM(bytes) FROM user_segments WHERE segment_name = :seg",
                parameters={"seg": uc_table},
            ).fetchone()
            return int(row[0]) if row and row[0] is not None else None
    except Exception:
        return None
    return None


def scan_table(
    con: ibis.BaseBackend,
    table_name: str,
    *,
    schema: str | None = None,
    dataset_id: str,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
    sample_size: int | None = None,
    row_count: int | None = None,
) -> tuple[list[Variable], int | None, int | None, pa.Table | None]:
    """Scan a database table and return (variables, row_count, sample_size, freq_table)."""
    backend = get_backend_name(con)

    # Oracle: get schema + detect LOB columns in a single query
    skip_stats_columns: set[str] = set()
    oracle_schema: ibis.Schema | None = None
    if backend == "oracle":
        try:
            oracle_schema, lob_columns, date_columns = _oracle_get_schema(
                con, table_name, schema
            )
            if infer_stats:
                skip_stats_columns = lob_columns | date_columns
        except Exception:
            pass  # If metadata fails, _get_table will retry

    table = _get_table(con, table_name, schema, backend, oracle_schema=oracle_schema)

    # Schema-only mode: just column names + types, no row count
    if not infer_stats:
        variables, _ = build_variables(
            table,
            nb_rows=0,
            dataset_id=dataset_id,
            infer_stats=False,
        )
        return variables, None, None, None

    # Get exact row count (use provided value or compute)
    if row_count is None:
        row_count = int(table.count().to_pyarrow().as_py())

    # When sampling: materialize sample locally, compute streaming stats on full table
    actual_sample_size: int | None = None
    if sample_size is not None and row_count > sample_size and infer_stats:
        fraction = sample_size / row_count
        if backend == "oracle":  # pragma: no cover
            # Oracle: use native SAMPLE(pct) clause — ibis.random() is evaluated
            # once as a scalar on Oracle, returning all or no rows.
            sample_pct = round(fraction * 100, 2)
            sampled = _get_table(
                con,
                table_name,
                schema,
                backend,
                oracle_schema=oracle_schema,
                sample_pct=sample_pct,
            )
        elif backend == "sqlite":
            # SQLite aggregates fail on random-filtered subqueries
            sampled = table.order_by(ibis.random()).limit(sample_size)
        else:
            sampled = table.sample(fraction)

        # Materialize sample → local PyArrow → ibis memtable
        # Oracle returns Decimal for NUMBER columns that PyArrow can't convert;
        # use execute() → pandas → Arrow as a workaround.
        if backend == "oracle":  # pragma: no cover
            sample_arrow = pa.Table.from_pandas(sampled.execute())
        else:
            sample_arrow = sampled.to_pyarrow()
        actual_sample_size = len(sample_arrow)
        sample_table = ibis.memtable(sample_arrow)

        variables, freq_table = build_variables(
            sample_table,
            nb_rows=actual_sample_size,
            dataset_id=dataset_id,
            infer_stats=True,
            freq_threshold=freq_threshold,
            skip_stats_columns=skip_stats_columns if skip_stats_columns else None,
            full_table=table,
            full_nb_rows=row_count,
        )
    else:
        variables, freq_table = build_variables(
            table,
            nb_rows=row_count,
            dataset_id=dataset_id,
            infer_stats=infer_stats,
            freq_threshold=freq_threshold,
            skip_stats_columns=skip_stats_columns if skip_stats_columns else None,
        )

    return variables, row_count, actual_sample_size, freq_table
