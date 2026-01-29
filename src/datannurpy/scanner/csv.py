"""CSV reader using Ibis/DuckDB."""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import ibis
import pandas as pd
import pyarrow as pa

from ..entities import Variable
from .utils import build_variables

if TYPE_CHECKING:
    from ibis.expr.types import Table

# Default encoding fallback order
DEFAULT_ENCODINGS = ("utf-8", "CP1252", "ISO_8859_1")


def _build_encoding_order(csv_encoding: str | None) -> tuple[str | None, ...]:
    """Build encoding order with specified encoding first."""
    if csv_encoding is None:
        return (None, "CP1252", "ISO_8859_1")
    others = [enc for enc in DEFAULT_ENCODINGS if enc.upper() != csv_encoding.upper()]
    return (csv_encoding, *others)


def _read_csv_table(
    file_path: Path,
    con: ibis.BaseBackend,
    csv_encoding: str | None,
) -> Table | None:
    """Read CSV into ibis Table using existing connection. Returns None on error."""
    encodings = _build_encoding_order(csv_encoding)
    last_error: str | None = None

    for encoding in encodings:
        try:
            if encoding is None:
                return con.read_csv(file_path)
            else:
                return con.read_csv(file_path, encoding=encoding)
        except duckdb.InvalidInputException as e:
            last_error = str(e)
            continue

    first_line = last_error.split("\n")[0] if last_error else ""
    warnings.warn(
        f"Could not parse CSV file '{file_path.name}': {first_line}",
        stacklevel=4,
    )
    return None


def read_csv(
    path: str | Path,
    *,
    csv_encoding: str | None = None,
) -> pd.DataFrame | None:
    """Read a CSV file into a pandas DataFrame."""
    file_path = Path(path)

    if file_path.stat().st_size == 0:
        return None

    con = ibis.duckdb.connect()
    try:
        table = _read_csv_table(file_path, con, csv_encoding)
        return table.to_pandas() if table is not None else None
    finally:
        con.disconnect()


def scan_csv(
    path: str | Path,
    *,
    dataset_id: str,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
    csv_encoding: str | None = None,
) -> tuple[list[Variable], int, pa.Table | None]:
    """Scan a CSV file and return (variables, row_count, freq_table)."""
    file_path = Path(path)

    if file_path.stat().st_size == 0:
        return [], 0, None

    con = ibis.duckdb.connect()
    try:
        table = _read_csv_table(file_path, con, csv_encoding)
        if table is None:
            return [], 0, None

        row_count = int(table.count().to_pyarrow().as_py())

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
