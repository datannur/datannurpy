"""Excel reader using Polars."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from ..entities import Variable
from ._utils import build_variables


def scan_excel(
    path: str | Path,
    *,
    sheet_name: str | None = None,
    dataset_id: str | None = None,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
) -> tuple[list[Variable], int, pl.DataFrame | None]:
    """Scan an Excel file and return (variables, row_count, freq_df)."""
    read_kwargs: dict[str, Any] = {"source": Path(path), "engine": "calamine"}
    if sheet_name is not None:
        read_kwargs["sheet_name"] = sheet_name

    df = pl.read_excel(**read_kwargs)
    variables, freq_df = build_variables(
        df,
        dataset_id=dataset_id,
        infer_stats=infer_stats,
        freq_threshold=freq_threshold,
    )
    return variables, len(df), freq_df
