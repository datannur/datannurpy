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
                "Use sqlite, postgres, mysql, or duckdb."
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
    else:
        raise ValueError(f"Unsupported backend: {backend}")

    return con, backend


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
    # Get all tables
    tables = list(con.list_tables(database=schema) if schema else con.list_tables())
    backend = backend_name or _get_backend_name(con)

    # Filter out views (some backends include them in list_tables)
    raw_sql = getattr(con, "raw_sql", None)
    if raw_sql:
        try:
            if backend == "sqlite":
                result = raw_sql(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
                actual_tables = {row[0] for row in result}
                tables = [t for t in tables if t in actual_tables]
            elif backend in ("duckdb", "postgres", "mysql"):
                # Use information_schema (standard SQL)
                query = (
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_type = 'BASE TABLE'"
                )
                if schema:
                    query += f" AND table_schema = '{schema}'"
                result = raw_sql(query).fetchall()
                actual_tables = {row[0] for row in result}
                tables = [t for t in tables if t in actual_tables]
            elif backend == "oracle":
                # Oracle uses ALL_TABLES or USER_TABLES
                if schema:
                    query = f"SELECT table_name FROM all_tables WHERE owner = '{schema.upper()}'"
                else:
                    query = "SELECT table_name FROM user_tables"
                result = raw_sql(query).fetchall()
                actual_tables = {row[0] for row in result}
                tables = [t for t in tables if t in actual_tables]
        except Exception:
            pass

    if include is not None:
        included = _match_patterns(tables, include)
        tables = [t for t in tables if t in included]

    if exclude is not None:
        excluded = _match_patterns(tables, exclude)
        tables = [t for t in tables if t not in excluded]

    return sorted(tables)


def list_schemas(con: ibis.BaseBackend) -> list[str]:
    """List schemas in a database (postgres/mysql only)."""
    # Try to get schemas - not all backends support this
    try:
        list_schemas_fn = getattr(con, "list_schemas", None)
        if list_schemas_fn:
            return list(list_schemas_fn())
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
    # Get table reference
    if schema:
        table = con.table(table_name, database=schema)
    else:
        table = con.table(table_name)

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
    )

    return variables, row_count, freq_table
