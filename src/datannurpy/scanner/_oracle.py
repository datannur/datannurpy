"""Oracle-specific helpers for database scanning."""

from __future__ import annotations

from typing import TYPE_CHECKING, NoReturn

import ibis

if TYPE_CHECKING:
    from collections.abc import Callable

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

# Oracle data type → ibis type mapping
_ORACLE_TYPE_MAP: dict[str, str] = {
    "VARCHAR2": "string",
    "NVARCHAR2": "string",
    "CHAR": "string",
    "NCHAR": "string",
    "CLOB": "string",
    "NCLOB": "string",
    "LONG": "string",
    "DATE": "timestamp",
    "BLOB": "binary",
    "RAW": "binary",
    "LONG RAW": "binary",
    "FLOAT": "float64",
    "BINARY_FLOAT": "float32",
    "BINARY_DOUBLE": "float64",
}

_ORACLE_LOB_TYPES = {"CLOB", "NCLOB", "BLOB"}
_ORACLE_DATE_TYPES = {"DATE", "TIMESTAMP"}

_oracle_client_initialized = False


def _init_oracle_client(
    lib_dir: str,
    raise_driver_error: Callable[[str, Exception], NoReturn],
) -> None:
    """Initialize Oracle thick mode (can only be called once per process)."""
    global _oracle_client_initialized  # noqa: PLW0603
    if _oracle_client_initialized:
        return
    try:
        import oracledb
    except ImportError as e:
        raise_driver_error("oracle", e)
    oracledb.init_oracle_client(lib_dir=lib_dir)
    _oracle_client_initialized = True


def _oracle_type_to_ibis(
    data_type: str, precision: int | None, scale: int | None
) -> str:
    """Map an Oracle data type to an ibis type string."""
    dt = data_type.upper()
    if dt == "NUMBER":
        if scale is not None and scale > 0:
            return "float64"
        if precision is not None:
            return "int64"
        return "float64"
    if dt.startswith("TIMESTAMP"):
        return "timestamp"
    return _ORACLE_TYPE_MAP.get(dt, "string")


def _oracle_get_schema(
    con: ibis.BaseBackend,
    table_name: str,
    schema: str | None,
) -> tuple[ibis.Schema, set[str], set[str]]:
    """Get table schema, LOB columns, and date/timestamp columns."""
    raw_sql = getattr(con, "raw_sql")
    uc_table = table_name.upper()
    if schema:
        uc_schema = schema.upper()
        query = (
            f"SELECT column_name, data_type, data_precision, data_scale "
            f"FROM all_tab_columns "
            f"WHERE owner = '{uc_schema}' AND table_name = '{uc_table}' "
            f"ORDER BY column_id"
        )
    else:
        query = (
            f"SELECT column_name, data_type, data_precision, data_scale "
            f"FROM user_tab_columns "
            f"WHERE table_name = '{uc_table}' "
            f"ORDER BY column_id"
        )
    result = raw_sql(query).fetchall()
    pairs: dict[str, str] = {}
    lob_columns: set[str] = set()
    date_columns: set[str] = set()
    for col_name, dt, prec, sc in result:
        pairs[col_name] = _oracle_type_to_ibis(dt, prec, sc)
        upper_dt = dt.upper()
        if upper_dt in _ORACLE_LOB_TYPES:
            lob_columns.add(col_name.lower())
        if upper_dt in _ORACLE_DATE_TYPES or upper_dt.startswith("TIMESTAMP"):
            date_columns.add(col_name.lower())
    return ibis.schema(pairs), lob_columns, date_columns
