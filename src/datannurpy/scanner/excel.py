"""Excel reader using pandas + openpyxl/xlrd."""

from __future__ import annotations

import warnings
from pathlib import Path

import ibis
import pandas as pd
import pyarrow as pa

from ..entities import Variable
from .utils import build_variables


def scan_excel(
    path: str | Path,
    *,
    sheet_name: str | int = 0,
    dataset_id: str | None = None,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
) -> tuple[list[Variable], int, pa.Table | None]:
    """Scan an Excel file and return (variables, row_count, freq_table)."""
    file_path = Path(path)
    suffix = file_path.suffix.lower()

    # Check for empty file
    if file_path.stat().st_size == 0:
        return [], 0, None

    # Select engine based on file extension
    # openpyxl for .xlsx (modern), xlrd for .xls (legacy)
    engine = "xlrd" if suffix == ".xls" else "openpyxl"

    # Read Excel file with pandas
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, engine=engine)
    except Exception as e:
        # Handle password-protected, corrupted, or unreadable files
        error_msg = str(e).split("\n")[0]  # First line of error
        warnings.warn(
            f"Could not read Excel file '{file_path.name}': {error_msg}",
            stacklevel=3,
        )
        return [], 0, None

    # Handle empty DataFrame (file with headers only or empty sheet)
    if df.empty:
        return [], 0, None

    # Convert to Ibis table via DuckDB for stats computation
    con = ibis.duckdb.connect()
    try:
        table = con.create_table("excel_data", df)

        row_count: int = table.count().execute()

        variables, freq_table = build_variables(
            table,
            nb_rows=row_count,
            dataset_id=dataset_id,
            infer_stats=infer_stats,
            freq_threshold=freq_threshold,
        )
        return variables, row_count, freq_table
    finally:
        con.disconnect()
