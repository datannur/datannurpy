"""Oracle-specific helpers for database scanning."""

from __future__ import annotations

from typing import TYPE_CHECKING, NoReturn

import ibis

if TYPE_CHECKING:
    from collections.abc import Callable

    from ..schema import Variable

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


def _oracle_patch_date_stats(
    con: ibis.BaseBackend,
    table_name: str,
    schema: str | None,
    date_columns: set[str],
    variables: list[Variable],
) -> None:
    """Compute date/timestamp stats via raw SQL (epoch_seconds unsupported on Oracle)."""
    raw_sql = getattr(con, "raw_sql")
    uc_table = table_name.upper().replace('"', '""')
    if schema:
        uc_schema = schema.upper().replace('"', '""')
        qualified = f'"{uc_schema}"."{uc_table}"'
    else:
        qualified = f'"{uc_table}"'

    # Filter out columns with all missing or no variable match
    var_lookup = {v.name: v for v in variables}
    eligible_cols: list[str] = []
    for col in date_columns:
        var = var_lookup.get(col)
        if var is None:
            continue
        nb_non_null = (var.nb_distinct or 0) + (var.nb_duplicate or 0)
        if nb_non_null == 0:
            continue
        eligible_cols.append(col)

    if not eligible_cols:
        return

    # Build SELECT with MIN/MAX/AVG and optional STDDEV for each date column
    # CAST to DATE: TIMESTAMP - TIMESTAMP = INTERVAL, DATE - DATE = NUMBER
    epoch_expr = (
        "(CAST(\"{col}\" AS DATE) - TO_DATE('1970-01-01','YYYY-MM-DD')) * 86400"
    )
    select_parts: list[str] = []
    ordered_cols: list[str] = []
    has_stddev: list[bool] = []
    for col in eligible_cols:
        var = var_lookup[col]
        safe_col = col.upper().replace('"', '""')
        expr = epoch_expr.replace("{col}", safe_col)
        select_parts.append(f"MIN({expr})")
        select_parts.append(f"MAX({expr})")
        select_parts.append(f"AVG({expr})")
        need_std = (var.nb_distinct or 0) > 1
        if need_std:
            select_parts.append(f"STDDEV({expr})")
        has_stddev.append(need_std)
        ordered_cols.append(col)

    query = f"SELECT {', '.join(select_parts)} FROM {qualified}"
    try:
        row = raw_sql(query).fetchone()
    except Exception:
        return  # Stats unavailable — leave as None

    if row is None:
        return

    offset = 0
    for i, col in enumerate(ordered_cols):
        var = var_lookup[col]
        raw_min = row[offset]
        raw_max = row[offset + 1]
        raw_mean = row[offset + 2]
        var.min = float(raw_min) if raw_min is not None else None
        var.max = float(raw_max) if raw_max is not None else None
        var.mean = round(float(raw_mean), 6) if raw_mean is not None else None
        if has_stddev[i]:
            raw_std = row[offset + 3]
            var.std = round(float(raw_std), 6) if raw_std is not None else None
            offset += 4
        else:
            offset += 3
