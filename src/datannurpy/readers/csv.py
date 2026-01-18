"""CSV reader using Polars."""

from __future__ import annotations

from pathlib import Path

import polars as pl
from polars.exceptions import NoDataError

from ..entities import Variable
from ._utils import build_variables


def scan_csv(
    path: str | Path,
    *,
    dataset_id: str | None = None,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
) -> tuple[list[Variable], int, pl.DataFrame | None]:
    """Scan a CSV file and return (variables, row_count, freq_df)."""
    try:
        lf = pl.scan_csv(Path(path))  # pyright: ignore[reportCallIssue]
        df = lf.collect()
    except NoDataError:
        # Empty CSV file
        return [], 0, None
    variables, freq_df = build_variables(
        df,
        dataset_id=dataset_id,
        infer_stats=infer_stats,
        freq_threshold=freq_threshold,
    )
    return variables, len(df), freq_df
