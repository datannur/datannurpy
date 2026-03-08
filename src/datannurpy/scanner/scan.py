"""Unified file scanner that dispatches to format-specific scanners."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ..schema import Variable
from .csv import scan_csv
from .excel import scan_excel
from .parquet import scan_parquet
from .parquet.core import scan_delta, scan_hive, scan_iceberg
from .statistical import scan_statistical
from .utils import build_variables_from_schema


@dataclass
class ScanResult:
    """Result of scanning a file."""

    variables: list[Variable]
    nb_row: int | None
    freq_table: pa.Table | None = None
    description: str | None = None
    name: str | None = None  # Dataset name from metadata (Delta, Iceberg)


def scan_file(
    path: Path,
    delivery_format: str,
    *,
    dataset_id: str,
    schema_only: bool = False,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
    csv_encoding: str | None = None,
) -> ScanResult:
    """Scan a file and return variables, row count, and optional metadata.

    Args:
        schema_only: If True, only read schema (no data, no row count, no stats).
    """
    # Schema-only mode: read metadata without scanning data
    if schema_only:
        return _scan_schema_only(path, delivery_format, dataset_id, csv_encoding)

    # Parquet-based formats (parquet, delta, hive, iceberg)
    parquet_scanners = {
        "parquet": scan_parquet,
        "delta": scan_delta,
        "hive": scan_hive,
        "iceberg": scan_iceberg,
    }
    if delivery_format in parquet_scanners:
        variables, nb_row, freq_table, metadata = parquet_scanners[delivery_format](
            path,
            dataset_id=dataset_id,
            infer_stats=infer_stats,
            freq_threshold=freq_threshold,
        )
        return ScanResult(
            variables=variables,
            nb_row=nb_row,
            freq_table=freq_table,
            description=metadata.description if metadata else None,
            name=metadata.name if metadata else None,
        )

    if delivery_format in ("sas", "spss", "stata"):
        variables, nb_row, freq_table, metadata = scan_statistical(
            path,
            dataset_id=dataset_id,
            infer_stats=infer_stats,
            freq_threshold=freq_threshold,
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
            dataset_id=dataset_id,
            infer_stats=infer_stats,
            freq_threshold=freq_threshold,
            csv_encoding=csv_encoding,
        )
        return ScanResult(variables=variables, nb_row=nb_row, freq_table=freq_table)

    # Excel (xls, xlsx)
    variables, nb_row, freq_table = scan_excel(
        path,
        dataset_id=dataset_id,
        infer_stats=infer_stats,
        freq_threshold=freq_threshold,
    )
    return ScanResult(variables=variables, nb_row=nb_row, freq_table=freq_table)


def _scan_schema_only(
    path: Path,
    delivery_format: str,
    dataset_id: str,
    csv_encoding: str | None = None,
) -> ScanResult:
    """Read schema only without scanning data (for depth='schema' mode)."""
    import ibis

    # Parquet-based: read schema from metadata
    if delivery_format in ("parquet", "delta", "hive", "iceberg"):
        if delivery_format == "parquet":
            schema = pq.read_schema(path)
        elif delivery_format == "delta":
            from deltalake import DeltaTable

            dt = DeltaTable(path)
            # deltalake returns arro3 schema, convert to pyarrow
            schema = pa.schema(dt.schema().to_arrow())
        else:  # hive or iceberg - read from first parquet file
            parquet_files = list(path.rglob("*.parquet"))
            schema = (
                pq.read_schema(parquet_files[0]) if parquet_files else pa.schema([])
            )

        variables = build_variables_from_schema(schema, dataset_id)
        return ScanResult(variables=variables, nb_row=None)

    # CSV/Excel/Statistical: use ibis to infer schema from first rows
    con = ibis.duckdb.connect()
    try:
        if delivery_format == "csv":
            from .csv import _read_csv_table

            table = _read_csv_table(path, con, csv_encoding)
            if table is None:
                return ScanResult(variables=[], nb_row=None)
        elif delivery_format == "excel":
            # For Excel, read the full file but don't compute stats
            from .excel import scan_excel

            variables, _, _ = scan_excel(path, dataset_id=dataset_id, infer_stats=False)
            return ScanResult(variables=variables, nb_row=None)
        else:  # statistical formats
            from .statistical import scan_statistical

            variables, _, _, metadata = scan_statistical(
                path, dataset_id=dataset_id, infer_stats=False
            )
            return ScanResult(
                variables=variables,
                nb_row=None,
                description=metadata.description if metadata else None,
            )

        schema = table.to_pyarrow().schema
        variables = build_variables_from_schema(schema, dataset_id)
        return ScanResult(variables=variables, nb_row=None)
    finally:
        con.disconnect()
