"""Database reader using Ibis backends."""

from __future__ import annotations

from typing import TYPE_CHECKING

import ibis

from ..entities import Variable
from ._utils import build_variables

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
        "DBSFWUSER",
        "GGSYS",
        "ANONYMOUS",
        "CTXSYS",
        "DVSYS",
        "DVF",
        "GSMADMIN_INTERNAL",
        "MDSYS",
        "OLAPSYS",
        "LBACSYS",
        "XDB",
        "WMSYS",
        "ORDDATA",
        "ORDPLUGINS",
        "ORDSYS",
        "SI_INFORMTN_SCHEMA",
    },
}

# Oracle system table prefixes to exclude (these exist in user schemas like SYSTEM)
ORACLE_SYSTEM_TABLE_PREFIXES: tuple[str, ...] = (
    "mview$_",
    "mview_",
    "ol$",
    "aq$_",
    "scheduler_",
    "redo_",
    "sqlplus_",
    "help",
    "product_privs",
)


def _get_backend_name(con: ibis.BaseBackend) -> str:
    """Get backend name from connection object."""
    return type(con).__module__.split(".")[-1]


def parse_connection_string(connection: str) -> tuple[str, dict[str, str]]:
    """Parse a connection string into (backend_name, kwargs)."""
    from urllib.parse import parse_qs, urlparse

    parsed = urlparse(connection)
    scheme = parsed.scheme.lower()

    backend = SCHEME_TO_BACKEND.get(scheme)
    if backend is None:
        supported = ", ".join(sorted(SCHEME_TO_BACKEND.keys()))
        raise ValueError(
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
            kwargs["user"] = parsed.username
        if parsed.password:
            kwargs["password"] = parsed.password
        if parsed.path and parsed.path != "/":
            kwargs["database"] = parsed.path.lstrip("/")

        # Parse query string for additional params
        if parsed.query:
            query_params = parse_qs(parsed.query)
            for key, values in query_params.items():
                if values:
                    kwargs[key] = values[0]

    return backend, kwargs


def connect(connection: str | ibis.BaseBackend) -> tuple[ibis.BaseBackend, str]:
    """Connect to a database, return (connection, backend_name)."""
    if isinstance(connection, ibis.BaseBackend):
        backend_name = _get_backend_name(connection)
        if backend_name in ("pyspark", "datafusion", "polars"):
            raise ValueError(
                f"Backend {backend_name!r} is not supported for database scanning. "
                "Use sqlite, postgres, mysql, oracle, mssql, or duckdb."
            )
        return connection, backend_name

    backend, kwargs = parse_connection_string(connection)

    if backend == "sqlite":
        con = ibis.sqlite.connect(kwargs.get("path", ":memory:"))
    elif backend == "postgres":
        con = ibis.postgres.connect(
            host=kwargs.get("host", "localhost"),
            port=int(kwargs.get("port", 5432)),
            user=kwargs.get("user"),
            password=kwargs.get("password"),
            database=kwargs.get("database"),
        )
    elif backend == "mysql":
        con = ibis.mysql.connect(
            host=kwargs.get("host", "localhost"),
            port=int(kwargs.get("port", 3306)),
            user=kwargs.get("user"),
            password=kwargs.get("password"),
            database=kwargs.get("database"),
        )
    elif backend == "oracle":
        con = ibis.oracle.connect(
            host=kwargs.get("host", "localhost"),
            port=int(kwargs.get("port", 1521)),
            user=kwargs.get("user"),
            password=kwargs.get("password"),
            database=kwargs.get("database"),
        )
    elif backend == "mssql":
        # Build connection kwargs - extract known params, pass rest as extras
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
        # Pass additional params (e.g., TrustServerCertificate) to pyodbc
        for key, value in kwargs.items():
            if key not in known_params:
                mssql_kwargs[key] = value
        con = ibis.mssql.connect(**mssql_kwargs)  # type: ignore[arg-type]
    else:
        raise ValueError(f"Unsupported backend: {backend}")

    return con, backend


def get_database_name(
    connection: str | ibis.BaseBackend,
    con: ibis.BaseBackend,
    backend_name: str,
) -> str:
    """Extract database name from connection."""
    from pathlib import Path
    from urllib.parse import urlparse

    if isinstance(connection, str):
        parsed = urlparse(connection)
        if backend_name == "sqlite":
            path = parsed.netloc + parsed.path if parsed.netloc else parsed.path
            return Path(path).stem or "sqlite"
        else:
            return parsed.path.lstrip("/") or backend_name
    else:
        db_attr = getattr(con, "database", None)
        if db_attr:
            return str(db_attr)
        return backend_name


def get_database_path(
    connection: str | ibis.BaseBackend,
    backend_name: str,
) -> str | None:
    """Get file path for file-based databases (SQLite, DuckDB)."""
    from pathlib import Path
    from urllib.parse import urlparse

    if backend_name not in ("sqlite", "duckdb"):
        return None

    if isinstance(connection, str):
        parsed = urlparse(connection)
        path = parsed.netloc + parsed.path if parsed.netloc else parsed.path
        if path and path != ":memory:":
            return str(Path(path).resolve())
    else:
        # Try to get path from connection object
        db_path = getattr(connection, "con", None)
        if db_path is not None:
            # SQLite connection has .con which is the sqlite3.Connection
            db_file = getattr(db_path, "database", None)
            if db_file and db_file != ":memory:":
                return str(Path(db_file).resolve())

    return None


def get_schemas_to_scan(
    con: ibis.BaseBackend,
    schema: str | None,
    backend_name: str,
) -> list[str | None]:
    """Determine which schemas to scan."""
    if schema is not None:
        return [schema]

    if backend_name in SYSTEM_SCHEMAS:
        available = list_schemas(con)
        system = SYSTEM_SCHEMAS[backend_name]
        schemas: list[str | None] = [s for s in available if s not in system]
        if backend_name == "oracle":
            schemas.append(None)
        elif not schemas:
            schemas = [None]
        return schemas

    return [None]


def _match_patterns(items: list[str], patterns: Sequence[str]) -> set[str]:
    """Match items against glob patterns."""
    import fnmatch

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
    backend = backend_name or _get_backend_name(con)

    # Oracle stores unquoted identifiers in UPPERCASE
    db_schema = schema.upper() if schema and backend == "oracle" else schema

    # Get tables - use backend-specific queries to filter views and system tables
    raw_sql = getattr(con, "raw_sql", None)
    tables: list[str] = []

    if raw_sql and backend == "oracle":
        # Use USER_TABLES/ALL_TABLES to get only tables (excludes views)
        # Normalize to lowercase since Oracle stores identifiers in UPPERCASE
        if db_schema:
            query = f"SELECT table_name FROM all_tables WHERE owner = '{db_schema}'"
        else:
            query = "SELECT table_name FROM user_tables"
        result = raw_sql(query).fetchall()
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
    elif raw_sql and backend in ("duckdb", "postgres", "mysql", "mssql"):
        # Use information_schema (standard SQL)
        query = (
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_type = 'BASE TABLE'"
        )
        if schema:
            query += f" AND table_schema = '{schema}'"
        result = raw_sql(query).fetchall()
        tables = [row[0] for row in result]
    else:
        # Fallback to Ibis list_tables
        tables = list(
            con.list_tables(database=db_schema) if db_schema else con.list_tables()
        )

    if include is not None:
        included = _match_patterns(tables, include)
        tables = [t for t in tables if t in included]

    if exclude is not None:
        excluded = _match_patterns(tables, exclude)
        tables = [t for t in tables if t not in excluded]

    return sorted(tables)


def list_schemas(con: ibis.BaseBackend) -> list[str]:
    """List schemas in a database (postgres/mysql only)."""
    backend = _get_backend_name(con)
    system_schemas = SYSTEM_SCHEMAS.get(backend, set())

    # Oracle: use raw SQL to list user schemas (more reliable than Ibis)
    if backend == "oracle":
        raw_sql = getattr(con, "raw_sql", None)
        if raw_sql:
            # List all users that have at least one table (user schemas)
            result = raw_sql(
                "SELECT DISTINCT owner FROM all_tables "
                "WHERE owner NOT IN ("
                + ",".join(f"'{s}'" for s in system_schemas)
                + ")"
            ).fetchall()
            return sorted([row[0].lower() for row in result])
        return []

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


def scan_table(
    con: ibis.BaseBackend,
    table_name: str,
    *,
    schema: str | None = None,
    dataset_id: str | None = None,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
    sample_size: int | None = None,
) -> tuple[list[Variable], int, ibis.Table | None]:
    """Scan a database table and return (variables, row_count, freq_table)."""
    backend = _get_backend_name(con)

    # Oracle stores unquoted identifiers in UPPERCASE
    if backend == "oracle":
        table_name = table_name.upper()
        if schema:
            schema = schema.upper()

    # Oracle: detect CLOB/NCLOB columns that don't support COUNT DISTINCT
    skip_stats_columns: set[str] = set()
    if backend == "oracle" and infer_stats:
        raw_sql = getattr(con, "raw_sql", None)
        if raw_sql:
            try:
                if schema:
                    query = (
                        f"SELECT column_name FROM all_tab_columns "
                        f"WHERE owner = '{schema}' AND table_name = '{table_name}' "
                        f"AND data_type IN ('CLOB', 'NCLOB', 'BLOB')"
                    )
                else:
                    query = (
                        f"SELECT column_name FROM user_tab_columns "
                        f"WHERE table_name = '{table_name}' "
                        f"AND data_type IN ('CLOB', 'NCLOB', 'BLOB')"
                    )
                result = raw_sql(query).fetchall()
                # Convert to lowercase (we rename columns to lowercase later)
                skip_stats_columns = {row[0].lower() for row in result}
            except Exception:
                pass  # If detection fails, fallback to try/except in build_variables

    # Get table reference
    if schema:
        table = con.table(table_name, database=schema)
    else:
        table = con.table(table_name)

    # Oracle returns UPPERCASE column names, normalize to lowercase
    if backend == "oracle":
        table = table.rename(str.lower)

    # Get exact row count (always full count, not sampled)
    row_count = int(table.count().to_pyarrow().as_py())

    # For stats, optionally sample
    stats_table = table
    stats_row_count = row_count
    if sample_size is not None and row_count > sample_size:
        stats_table = table.limit(sample_size)
        stats_row_count = sample_size

    variables, freq_table = build_variables(
        stats_table,
        nb_rows=stats_row_count,
        dataset_id=dataset_id,
        infer_stats=infer_stats,
        freq_threshold=freq_threshold,
        skip_stats_columns=skip_stats_columns if skip_stats_columns else None,
    )

    return variables, row_count, freq_table
