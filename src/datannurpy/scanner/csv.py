"""CSV reader using Polars for encoding support."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import ibis
import pandas as pd
import polars as pl
import pyarrow as pa

from ..utils import log_warn
from .utils import build_variables

if TYPE_CHECKING:
    from ..schema import Variable

# Default encoding fallback order
DEFAULT_ENCODINGS = ("utf-8", "cp1252", "iso-8859-1")

# Common CSV separators, ordered by frequency
SEPARATORS = (",", ";", "\t", "|")


def _build_encoding_order(csv_encoding: str | None) -> tuple[str, ...]:
    """Build encoding order with specified encoding first."""
    if csv_encoding is None:
        return DEFAULT_ENCODINGS
    normalized = csv_encoding.lower().replace("_", "-")
    others = tuple(enc for enc in DEFAULT_ENCODINGS if enc != normalized)
    return (normalized, *others)


def _detect_separator(file_path: Path) -> str:
    """Detect CSV separator from first line."""
    with open(file_path, "rb") as f:
        header = f.readline().decode("iso-8859-1")
    counts = {sep: header.count(sep) for sep in SEPARATORS}
    best = max(counts, key=lambda s: counts[s])
    return best if counts[best] > 0 else ","


def _read_csv_polars(
    file_path: Path,
    csv_encoding: str | None,
    *,
    n_rows: int | None = None,
    quiet: bool = False,
) -> pl.DataFrame | None:
    """Read CSV with Polars, using encoding fallback."""
    separator = _detect_separator(file_path)
    encodings = _build_encoding_order(csv_encoding)
    last_error: str | None = None

    for encoding in encodings:
        try:
            return pl.read_csv(
                file_path,  # pyright: ignore[reportCallIssue]
                encoding=encoding,
                separator=separator,
                n_rows=n_rows,
                infer_schema_length=10000,
                try_parse_dates=False,
            )
        except Exception as e:
            last_error = str(e)
            continue

    first_line = last_error.split("\n")[0] if last_error else ""
    log_warn(f"Could not parse CSV file '{file_path.name}': {first_line}", quiet)
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

    df = _read_csv_polars(file_path, csv_encoding)
    return df.to_pandas() if df is not None else None


def scan_csv(
    path: str | Path,
    *,
    dataset_id: str,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
    csv_encoding: str | None = None,
    quiet: bool = False,
) -> tuple[list[Variable], int, pa.Table | None]:
    """Scan a CSV file and return (variables, row_count, freq_table)."""
    file_path = Path(path)

    if file_path.stat().st_size == 0:
        return [], 0, None

    df = _read_csv_polars(file_path, csv_encoding, quiet=quiet)
    if df is None:
        return [], 0, None

    row_count = len(df)
    table = ibis.memtable(df)

    variables, freq_table = build_variables(
        table,
        nb_rows=row_count,
        dataset_id=dataset_id,
        infer_stats=infer_stats,
        freq_threshold=freq_threshold,
    )
    return variables, row_count, freq_table
