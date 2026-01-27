"""Unified scanner for all Parquet dataset types."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import ibis
import pyarrow.parquet as pq

from ...entities import Variable
from ..utils import build_variables
from ._discovery import DatasetType, ParquetDatasetInfo


@dataclass
class DatasetMetadata:
    """Unified metadata for any Parquet dataset."""

    description: str | None = None
    name: str | None = None
    column_descriptions: dict[str, str] | None = None


def _extract_parquet_metadata(path: Path) -> DatasetMetadata:
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


def _scan_simple(
    path: Path,
    dataset_id: str | None,
    infer_stats: bool,
    freq_threshold: int | None,
) -> tuple[list[Variable], int, ibis.Table | None, DatasetMetadata]:
    """Scan a simple Parquet file."""
    # Extract metadata
    metadata = _extract_parquet_metadata(path)

    # Scan with Ibis
    con = ibis.duckdb.connect()
    table = con.read_parquet(path)
    row_count: int = table.count().execute()

    variables, freq_table = build_variables(
        table,
        nb_rows=row_count,
        dataset_id=dataset_id,
        infer_stats=infer_stats,
        freq_threshold=freq_threshold,
    )

    # Apply column descriptions to variables
    if metadata.column_descriptions:
        for var in variables:
            if var.name and var.name in metadata.column_descriptions:
                var.description = metadata.column_descriptions[var.name]

    return variables, row_count, freq_table, metadata


def _scan_delta(
    path: Path,
    dataset_id: str | None,
    infer_stats: bool,
    freq_threshold: int | None,
) -> tuple[list[Variable], int, ibis.Table | None, DatasetMetadata]:
    """Scan a Delta Lake table."""
    # Extract metadata using deltalake if available
    metadata = DatasetMetadata()
    try:
        from deltalake import DeltaTable  # pyright: ignore[reportMissingImports]

        dt = DeltaTable(str(path))
        meta = dt.metadata()
        metadata = DatasetMetadata(
            description=meta.description,
            name=meta.name,
        )
    except ImportError:
        pass
    except Exception:
        pass

    # Scan with Ibis
    con = ibis.duckdb.connect()
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


def _scan_hive(
    path: Path,
    dataset_id: str | None,
    infer_stats: bool,
    freq_threshold: int | None,
) -> tuple[list[Variable], int, ibis.Table | None, DatasetMetadata]:
    """Scan a Hive-partitioned Parquet dataset."""
    # Hive partitioned datasets don't have table-level metadata
    metadata = DatasetMetadata()

    # Scan with Ibis using glob pattern
    con = ibis.duckdb.connect()
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


def _extract_iceberg_metadata(path: Path) -> DatasetMetadata:
    """Extract metadata from Iceberg table's metadata JSON."""
    import json

    metadata_dir = path / "metadata"
    if not metadata_dir.exists():
        return DatasetMetadata()

    # Find the latest metadata file (highest version number)
    metadata_files = sorted(metadata_dir.glob("*.metadata.json"), reverse=True)
    if not metadata_files:
        return DatasetMetadata()

    with open(metadata_files[0]) as f:
        meta = json.load(f)

    # Extract table description from properties
    description = meta.get("properties", {}).get("comment")

    # Extract column descriptions from schema
    column_descriptions: dict[str, str] = {}
    schemas = meta.get("schemas", [])
    current_schema_id = meta.get("current-schema-id", 0)

    for schema in schemas:
        if schema.get("schema-id") == current_schema_id:
            for field in schema.get("fields", []):
                if doc := field.get("doc"):
                    column_descriptions[field["name"]] = doc
            break

    return DatasetMetadata(
        description=description,
        column_descriptions=column_descriptions if column_descriptions else None,
    )


def _scan_iceberg(
    path: Path,
    dataset_id: str | None,
    infer_stats: bool,
    freq_threshold: int | None,
) -> tuple[list[Variable], int, ibis.Table | None, DatasetMetadata]:
    """Scan an Apache Iceberg table."""
    import duckdb

    # Extract metadata from Iceberg JSON
    metadata = _extract_iceberg_metadata(path)

    # Use DuckDB directly since Ibis doesn't have read_iceberg
    con = duckdb.connect()
    con.execute("LOAD iceberg")
    con.execute("SET unsafe_enable_version_guessing = true")

    # Read table via iceberg_scan
    result = con.execute(f"SELECT * FROM iceberg_scan('{path}')").fetch_arrow_table()

    # Convert to Ibis for consistent processing
    table = ibis.memtable(result)
    row_count = len(result)

    variables, freq_table = build_variables(
        table,
        nb_rows=row_count,
        dataset_id=dataset_id,
        infer_stats=infer_stats,
        freq_threshold=freq_threshold,
    )

    # Apply column descriptions to variables
    if metadata.column_descriptions:
        for var in variables:
            if var.name and var.name in metadata.column_descriptions:
                var.description = metadata.column_descriptions[var.name]

    return variables, row_count, freq_table, metadata


def scan_parquet_dataset(
    info: ParquetDatasetInfo,
    *,
    dataset_id: str | None = None,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
) -> tuple[list[Variable], int, ibis.Table | None, DatasetMetadata]:
    """Scan a Parquet dataset based on its type."""
    if info.type == DatasetType.SIMPLE:
        return _scan_simple(info.path, dataset_id, infer_stats, freq_threshold)
    elif info.type == DatasetType.DELTA:
        return _scan_delta(info.path, dataset_id, infer_stats, freq_threshold)
    elif info.type == DatasetType.HIVE:
        return _scan_hive(info.path, dataset_id, infer_stats, freq_threshold)
    elif info.type == DatasetType.ICEBERG:
        return _scan_iceberg(info.path, dataset_id, infer_stats, freq_threshold)
    else:
        msg = f"Unknown dataset type: {info.type}"
        raise ValueError(msg)


def scan_parquet(
    path: str | Path,
    *,
    dataset_id: str | None = None,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
) -> tuple[list[Variable], int, ibis.Table | None, DatasetMetadata]:
    """Scan a simple Parquet file and return (variables, row_count, freq_table, metadata)."""
    return _scan_simple(Path(path), dataset_id, infer_stats, freq_threshold)
