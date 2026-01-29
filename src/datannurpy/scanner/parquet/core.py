"""Unified scanner for all Parquet dataset types."""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from pathlib import Path

import ibis
import pyarrow as pa
import pyarrow.parquet as pq

from ...entities import Variable
from ..utils import build_variables
from .discovery import DatasetType, ParquetDatasetInfo


@dataclass
class DatasetMetadata:
    """Unified metadata for any Parquet dataset."""

    description: str | None = None
    name: str | None = None
    column_descriptions: dict[str, str] | None = None


def apply_column_descriptions(
    variables: list[Variable], column_descriptions: dict[str, str] | None
) -> None:
    """Apply column descriptions from metadata to variables."""
    if not column_descriptions:
        return
    for var in variables:
        if var.name and var.name in column_descriptions:
            var.description = column_descriptions[var.name]


def extract_parquet_metadata(path: Path) -> DatasetMetadata:
    """Extract metadata from a Parquet file using PyArrow."""
    pq_file = pq.ParquetFile(path)
    schema = pq_file.schema_arrow

    # Schema-level metadata
    description: str | None = None
    if schema.metadata:
        raw = schema.metadata.get(b"description")
        if raw:
            description = raw.decode("utf-8")

    # Column-level metadata
    column_descriptions: dict[str, str] = {}
    for field in schema:
        if field.metadata:
            raw = field.metadata.get(b"description")
            if raw:
                column_descriptions[field.name] = raw.decode("utf-8")

    return DatasetMetadata(
        description=description,
        column_descriptions=column_descriptions if column_descriptions else None,
    )


def scan_simple(
    path: Path,
    dataset_id: str,
    infer_stats: bool,
    freq_threshold: int | None,
) -> tuple[list[Variable], int, pa.Table | None, DatasetMetadata]:
    """Scan a simple Parquet file."""
    # Extract metadata
    metadata = extract_parquet_metadata(path)

    # Scan with Ibis
    con = ibis.duckdb.connect()
    try:
        table = con.read_parquet(path)
        row_count: int = table.count().execute()

        variables, freq_table = build_variables(
            table,
            nb_rows=row_count,
            dataset_id=dataset_id,
            infer_stats=infer_stats,
            freq_threshold=freq_threshold,
        )

        apply_column_descriptions(variables, metadata.column_descriptions)

        return variables, row_count, freq_table, metadata
    finally:
        con.disconnect()


def scan_delta(
    path: Path,
    dataset_id: str,
    infer_stats: bool,
    freq_threshold: int | None,
) -> tuple[list[Variable], int, pa.Table | None, DatasetMetadata]:
    """Scan a Delta Lake table."""
    # Extract metadata using deltalake if available (optional, for metadata only)
    # DuckDB reads the data via its own delta extension
    metadata = DatasetMetadata()
    try:
        from deltalake import DeltaTable

        dt = DeltaTable(str(path))
        meta = dt.metadata()
        metadata = DatasetMetadata(
            description=meta.description,
            name=meta.name,
        )
    except ImportError:
        warnings.warn(
            "deltalake not installed. Delta table metadata (name, description) "
            "will not be extracted. Install with: pip install datannurpy[delta]",
            stacklevel=2,
        )
    except Exception as e:
        warnings.warn(
            f"Failed to extract Delta table metadata: {e}",
            stacklevel=2,
        )

    # Scan with Ibis
    con = ibis.duckdb.connect()
    try:
        table = con.read_delta(path)
        row_count: int = table.count().execute()

        variables, freq_table = build_variables(
            table,
            nb_rows=row_count,
            dataset_id=dataset_id,
            infer_stats=infer_stats,
            freq_threshold=freq_threshold,
        )

        return variables, row_count, freq_table, metadata
    finally:
        con.disconnect()


def scan_hive(
    path: Path,
    dataset_id: str,
    infer_stats: bool,
    freq_threshold: int | None,
) -> tuple[list[Variable], int, pa.Table | None, DatasetMetadata]:
    """Scan a Hive-partitioned Parquet dataset."""
    # Hive partitioned datasets don't have table-level metadata
    metadata = DatasetMetadata()

    # Scan with Ibis using glob pattern
    con = ibis.duckdb.connect()
    try:
        glob_pattern = str(path / "**" / "*.parquet")
        table = con.read_parquet(glob_pattern, hive_partitioning=True)
        row_count: int = table.count().execute()

        variables, freq_table = build_variables(
            table,
            nb_rows=row_count,
            dataset_id=dataset_id,
            infer_stats=infer_stats,
            freq_threshold=freq_threshold,
        )

        return variables, row_count, freq_table, metadata
    finally:
        con.disconnect()


def scan_iceberg(
    path: Path,
    dataset_id: str,
    infer_stats: bool,
    freq_threshold: int | None,
) -> tuple[list[Variable], int, pa.Table | None, DatasetMetadata]:
    """Scan an Apache Iceberg table using PyIceberg."""
    try:
        from pyiceberg.table import StaticTable
    except ImportError as e:
        msg = "PyIceberg is required to scan Iceberg tables. Install with: pip install datannurpy[iceberg]"
        raise ImportError(msg) from e

    # Find the latest metadata file
    metadata_dir = path / "metadata"
    metadata_files = sorted(metadata_dir.glob("*.metadata.json"), reverse=True)

    # Load table via PyIceberg
    table = StaticTable.from_metadata(str(metadata_files[0]))

    # Extract metadata from PyIceberg schema
    description = table.metadata.properties.get("comment")
    column_descriptions = {
        field.name: field.doc for field in table.schema().fields if field.doc
    }

    metadata = DatasetMetadata(
        description=description,
        column_descriptions=column_descriptions if column_descriptions else None,
    )

    # Read data as Arrow table
    arrow_table = table.scan().to_arrow()
    row_count = len(arrow_table)

    # Convert to Ibis for consistent processing
    ibis_table = ibis.memtable(arrow_table)

    variables, freq_table = build_variables(
        ibis_table,
        nb_rows=row_count,
        dataset_id=dataset_id,
        infer_stats=infer_stats,
        freq_threshold=freq_threshold,
    )

    apply_column_descriptions(variables, metadata.column_descriptions)

    return variables, row_count, freq_table, metadata


SCANNERS = {
    DatasetType.SIMPLE: scan_simple,
    DatasetType.DELTA: scan_delta,
    DatasetType.HIVE: scan_hive,
    DatasetType.ICEBERG: scan_iceberg,
}


def scan_parquet_dataset(
    info: ParquetDatasetInfo,
    *,
    dataset_id: str,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
) -> tuple[list[Variable], int, pa.Table | None, DatasetMetadata]:
    """Scan a Parquet dataset based on its type."""
    scanner = SCANNERS[info.type]
    return scanner(info.path, dataset_id, infer_stats, freq_threshold)


def scan_parquet(
    path: str | Path,
    *,
    dataset_id: str,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
) -> tuple[list[Variable], int, pa.Table | None, DatasetMetadata]:
    """Scan a simple Parquet file and return (variables, row_count, freq_table, metadata)."""
    return scan_simple(Path(path), dataset_id, infer_stats, freq_threshold)
