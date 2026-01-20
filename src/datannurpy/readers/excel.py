"""Excel reader using Ibis/DuckDB with spatial extension."""

from __future__ import annotations

from pathlib import Path

import ibis

from ..entities import Variable
from ._utils import build_variables


def scan_excel(
    path: str | Path,
    *,
    sheet_name: str | None = None,
    dataset_id: str | None = None,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
) -> tuple[list[Variable], int, ibis.Table | None]:
    """Scan an Excel file and return (variables, row_count, freq_table)."""
    con = ibis.duckdb.connect()
    # Load spatial extension for Excel support
    con.raw_sql("INSTALL spatial; LOAD spatial;")

    file_path = Path(path)
    # Escape path for SQL (replace single quotes)
    escaped_path = str(file_path).replace("'", "''")

    # Build st_read options
    options: list[str] = []
    if sheet_name is not None:
        escaped_sheet = sheet_name.replace("'", "''")
        options.append(f"layer='{escaped_sheet}'")

    options_str = ", ".join(options)
    if options_str:
        query = f"SELECT * FROM st_read('{escaped_path}', {options_str})"
    else:
        query = f"SELECT * FROM st_read('{escaped_path}')"

    table = con.sql(query)
    row_count: int = table.count().execute()

    variables, freq_table = build_variables(
        table,
        nb_rows=row_count,
        dataset_id=dataset_id,
        infer_stats=infer_stats,
        freq_threshold=freq_threshold,
    )
    return variables, row_count, freq_table
