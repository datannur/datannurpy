"""CSV reader using Ibis/DuckDB."""

from __future__ import annotations

from pathlib import Path

import duckdb
import ibis

from ..entities import Variable
from ._utils import build_variables


def scan_csv(
    path: str | Path,
    *,
    dataset_id: str | None = None,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
) -> tuple[list[Variable], int, ibis.Table | None]:
    """Scan a CSV file and return (variables, row_count, freq_table)."""
    file_path = Path(path)

    # Check for truly empty file (no content at all)
    if file_path.stat().st_size == 0:
        return [], 0, None

    con = ibis.duckdb.connect()
    try:
        table = con.read_csv(file_path)
    except duckdb.InvalidInputException:
        # Empty or malformed CSV
        return [], 0, None

    row_count: int = table.count().execute()

    variables, freq_table = build_variables(
        table,
        nb_rows=row_count,
        dataset_id=dataset_id,
        infer_stats=infer_stats,
        freq_threshold=freq_threshold,
    )
    return variables, row_count, freq_table
