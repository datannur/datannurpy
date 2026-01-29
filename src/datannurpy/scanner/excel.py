"""Excel reader using pandas + openpyxl/xlrd."""

from __future__ import annotations

import warnings
from pathlib import Path

import ibis
import pandas as pd
import pyarrow as pa

from ..entities import Variable
from .utils import build_variables


def read_excel(
    path: str | Path,
    *,
    sheet_name: str | int = 0,
) -> pd.DataFrame | None:
    """Read an Excel file into a pandas DataFrame."""
    file_path = Path(path)
    suffix = file_path.suffix.lower()

    if file_path.stat().st_size == 0:
        return None

    engine = "xlrd" if suffix == ".xls" else "openpyxl"

    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, engine=engine)
        if df.empty:
            return None
        return df
    except Exception as e:
        error_msg = str(e).split("\n")[0]
        warnings.warn(
            f"Could not read Excel file '{file_path.name}': {error_msg}",
            stacklevel=3,
        )
        return None


def scan_excel(
    path: str | Path,
    *,
    sheet_name: str | int = 0,
    dataset_id: str,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
) -> tuple[list[Variable], int, pa.Table | None]:
    """Scan an Excel file and return (variables, row_count, freq_table)."""
    df = read_excel(path, sheet_name=sheet_name)
    if df is None:
        return [], 0, None

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
        con.disconnect()
