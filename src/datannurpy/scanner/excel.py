"""Excel reader using pandas + openpyxl/xlrd."""

from __future__ import annotations

from pathlib import Path

import ibis
import pandas as pd
import pyarrow as pa

from ..schema import Variable
from ..utils import log_warn
from .utils import build_variables


def read_excel(
    path: str | Path,
    *,
    sheet_name: str | int = 0,
    quiet: bool = False,
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
        log_warn(f"Could not read Excel file '{file_path.name}': {error_msg}", quiet)
        return None


def scan_excel(
    path: str | Path,
    *,
    sheet_name: str | int = 0,
    dataset_id: str,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
    quiet: bool = False,
) -> tuple[list[Variable], int, pa.Table | None]:
    """Scan an Excel file and return (variables, row_count, freq_table)."""
    df = read_excel(path, sheet_name=sheet_name, quiet=quiet)
    if df is None:
        return [], 0, None

    con = ibis.duckdb.connect()
    try:
        try:
            table = con.create_table("excel_data", df)
        except pa.ArrowTypeError:
            for col in df.columns:
                if df[col].dtype == "object":
                    df[col] = df[col].astype(str)
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
