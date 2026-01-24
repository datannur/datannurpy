"""CSV reader using Ibis/DuckDB."""

from __future__ import annotations

import warnings
from pathlib import Path

import duckdb
import ibis

from ..entities import Variable
from ._utils import build_variables

# Default encoding fallback order
DEFAULT_ENCODINGS = ("utf-8", "CP1252", "ISO_8859_1")


def _build_encoding_order(csv_encoding: str | None) -> tuple[str | None, ...]:
    """Build encoding order with specified encoding first."""
    if csv_encoding is None:
        # Default: try without encoding (DuckDB default), then fallbacks
        return (None, "CP1252", "ISO_8859_1")
    # Put specified encoding first, then others
    others = [enc for enc in DEFAULT_ENCODINGS if enc.upper() != csv_encoding.upper()]
    return (csv_encoding, *others)


def scan_csv(
    path: str | Path,
    *,
    dataset_id: str | None = None,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
    csv_encoding: str | None = None,
) -> tuple[list[Variable], int, ibis.Table | None]:
    """Scan a CSV file and return (variables, row_count, freq_table)."""
    file_path = Path(path)

    # Check for truly empty file (no content at all)
    if file_path.stat().st_size == 0:
        return [], 0, None

    con = ibis.duckdb.connect()
    encodings = _build_encoding_order(csv_encoding)

    table = None
    last_error: str | None = None

    for encoding in encodings:
        try:
            if encoding is None:
                table = con.read_csv(file_path)
            else:
                table = con.read_csv(file_path, encoding=encoding)
            break  # Success
        except duckdb.InvalidInputException as e:
            last_error = str(e)
            continue  # Try next encoding

    if table is None:
        # All encodings failed - show warning with actual error
        msg = f"Could not parse CSV file '{file_path.name}'"
        if last_error:
            # Extract first line of error message (most relevant)
            first_line = last_error.split("\n")[0]
            msg += f": {first_line}"
        warnings.warn(msg, stacklevel=3)
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
