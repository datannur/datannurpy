"""Excel reader using pandas + openpyxl/xlrd."""

from __future__ import annotations

from pathlib import Path

import ibis
import pandas as pd

from ..entities import Variable
from ._utils import build_variables


def scan_excel(
    path: str | Path,
    *,
    sheet_name: str | int = 0,
    dataset_id: str | None = None,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
) -> tuple[list[Variable], int, ibis.Table | None]:
    """Scan an Excel file and return (variables, row_count, freq_table)."""
    file_path = Path(path)
    suffix = file_path.suffix.lower()

    # Select engine based on file extension
    # openpyxl for .xlsx (modern), xlrd for .xls (legacy)
    engine = "xlrd" if suffix == ".xls" else "openpyxl"

    # Read Excel file with pandas
    df = pd.read_excel(file_path, sheet_name=sheet_name, engine=engine)

    # Convert to Ibis table via DuckDB for stats computation
    con = ibis.duckdb.connect()
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
