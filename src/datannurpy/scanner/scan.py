"""Unified file scanner that dispatches to format-specific scanners."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

from ..entities import Variable
from .csv import scan_csv
from .excel import scan_excel
from .parquet import scan_parquet
from .statistical import scan_statistical


@dataclass
class ScanResult:
    """Result of scanning a file."""

    variables: list[Variable]
    nb_row: int
    freq_table: pa.Table | None = None
    description: str | None = None


def scan_file(
    path: Path,
    delivery_format: str,
    *,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
    csv_encoding: str | None = None,
) -> ScanResult:
    """Scan a file and return variables, row count, and optional metadata."""
    if delivery_format == "parquet":
        variables, nb_row, freq_table, metadata = scan_parquet(
            path, infer_stats=infer_stats, freq_threshold=freq_threshold
        )
        return ScanResult(
            variables=variables,
            nb_row=nb_row,
            freq_table=freq_table,
            description=metadata.description if metadata else None,
        )

    if delivery_format in ("sas", "spss", "stata"):
        variables, nb_row, freq_table, metadata = scan_statistical(
            path, infer_stats=infer_stats, freq_threshold=freq_threshold
        )
        return ScanResult(
            variables=variables,
            nb_row=nb_row,
            freq_table=freq_table,
            description=metadata.description if metadata else None,
        )

    if delivery_format == "csv":
        variables, nb_row, freq_table = scan_csv(
            path,
            infer_stats=infer_stats,
            freq_threshold=freq_threshold,
            csv_encoding=csv_encoding,
        )
        return ScanResult(variables=variables, nb_row=nb_row, freq_table=freq_table)

    # Excel (xls, xlsx)
    variables, nb_row, freq_table = scan_excel(
        path, infer_stats=infer_stats, freq_threshold=freq_threshold
    )
    return ScanResult(variables=variables, nb_row=nb_row, freq_table=freq_table)
