"""Unified scanner for all Parquet dataset types."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import ibis
import pyarrow as pa
import pyarrow.parquet as pq

from ...schema import Variable
from ...utils import log_error, log_warn
from ..utils import build_variables
from .discovery import DatasetType, ParquetDatasetInfo


@dataclass
class DatasetMetadata:
    """Unified metadata for any Parquet dataset."""

    description: str | None = None
    name: str | None = None
    column_descriptions: dict[str, str] | None = None
    data_size: int | None = None
    sample_size: int | None = None


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


def _build_with_sampling(
    con: Any,
    table: ibis.Table,
    *,
    row_count: int,
    dataset_id: str,
    infer_stats: bool,
    freq_threshold: int | None,
    sample_size: int | None,
    table_name: str,
) -> tuple[list[Variable], int | None, pa.Table | None]:
    """Build variables with optional DuckDB reservoir sampling."""
    if sample_size is not None and row_count > sample_size and infer_stats:
        cursor: Any = con.raw_sql(
            f"SELECT * FROM {table_name} USING SAMPLE reservoir({sample_size} ROWS)"
        )
        sample_arrow = cursor.fetch_arrow_table()
        sample_table = ibis.memtable(sample_arrow)
        variables, freq_table = build_variables(
            sample_table,
            nb_rows=len(sample_arrow),
            dataset_id=dataset_id,
            infer_stats=True,
            freq_threshold=freq_threshold,
            full_table=table,
            full_nb_rows=row_count,
        )
        return variables, len(sample_arrow), freq_table
    variables, freq_table = build_variables(
        table,
        nb_rows=row_count,
        dataset_id=dataset_id,
        infer_stats=infer_stats,
        freq_threshold=freq_threshold,
    )
    return variables, None, freq_table


def scan_simple(
    path: Path,
    dataset_id: str,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
    *,
    sample_size: int | None = None,
    quiet: bool = False,
) -> tuple[list[Variable], int, pa.Table | None, DatasetMetadata]:
    """Scan a simple Parquet file."""
    _ = quiet  # unused but kept for API consistency
    # Extract metadata
    metadata = extract_parquet_metadata(path)

    # Scan with Ibis
    con = ibis.duckdb.connect()
    try:
        table = con.read_parquet(path)
        row_count = int(table.count().to_pyarrow().as_py())
        table_name = table.get_name()

        variables, actual_sample_size, freq_table = _build_with_sampling(
            con,
            table,
            row_count=row_count,
            dataset_id=dataset_id,
            infer_stats=infer_stats,
            freq_threshold=freq_threshold,
            sample_size=sample_size,
            table_name=table_name,
        )

        apply_column_descriptions(variables, metadata.column_descriptions)
        metadata.sample_size = actual_sample_size

        return variables, row_count, freq_table, metadata
    finally:
        con.disconnect()


def scan_delta(
    path: Path,
    dataset_id: str,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
    *,
    sample_size: int | None = None,
    quiet: bool = False,
) -> tuple[list[Variable], int, pa.Table | None, DatasetMetadata]:
    """Scan a Delta Lake table."""
    # Extract metadata using deltalake if available (optional, for metadata only)
    # DuckDB reads the data via its own delta extension
    metadata = DatasetMetadata()
    try:
        from deltalake import DeltaTable

        dt = DeltaTable(str(path))
        meta = dt.metadata()
        actions = dt.get_add_actions()
        data_size = sum(actions.column("size_bytes").to_pylist())
        metadata = DatasetMetadata(
            description=meta.description,
            name=meta.name,
            data_size=data_size,
        )
    except ImportError:
        log_warn(
            "deltalake not installed. Delta table metadata (name, description) "
            "will not be extracted. Install with: pip install datannurpy[delta]",
            quiet,
        )
    except Exception as e:
        log_error("delta_metadata", e, quiet)

    # Scan with Ibis
    con = ibis.duckdb.connect()
    try:
        table = con.read_delta(path)
        row_count = int(table.count().to_pyarrow().as_py())
        table_name = table.get_name()

        variables, actual_sample_size, freq_table = _build_with_sampling(
            con,
            table,
            row_count=row_count,
            dataset_id=dataset_id,
            infer_stats=infer_stats,
            freq_threshold=freq_threshold,
            sample_size=sample_size,
            table_name=table_name,
        )

        metadata.sample_size = actual_sample_size

        return variables, row_count, freq_table, metadata
    finally:
        con.disconnect()


def scan_hive(
    path: Path,
    dataset_id: str,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
    *,
    sample_size: int | None = None,
    quiet: bool = False,
) -> tuple[list[Variable], int, pa.Table | None, DatasetMetadata]:
    """Scan a Hive-partitioned Parquet dataset."""
    _ = quiet  # unused but kept for API consistency
    # Hive partitioned datasets don't have table-level metadata
    # Compute data_size by summing parquet file sizes
    pq_files = list(path.rglob("*.parquet")) + list(path.rglob("*.pq"))
    data_size = sum(f.stat().st_size for f in pq_files) if pq_files else 0
    metadata = DatasetMetadata(data_size=data_size)

    # Scan with Ibis using glob pattern
    con = ibis.duckdb.connect()
    try:
        glob_pattern = str(path / "**" / "*.parquet")
        table = con.read_parquet(glob_pattern, hive_partitioning=True)
        row_count = int(table.count().to_pyarrow().as_py())
        table_name = table.get_name()

        variables, actual_sample_size, freq_table = _build_with_sampling(
            con,
            table,
            row_count=row_count,
            dataset_id=dataset_id,
            infer_stats=infer_stats,
            freq_threshold=freq_threshold,
            sample_size=sample_size,
            table_name=table_name,
        )

        metadata.sample_size = actual_sample_size

        return variables, row_count, freq_table, metadata
    finally:
        con.disconnect()


def scan_iceberg(
    path: Path,
    dataset_id: str,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
    *,
    sample_size: int | None = None,
    quiet: bool = False,
) -> tuple[list[Variable], int, pa.Table | None, DatasetMetadata]:
    """Scan an Apache Iceberg table using PyIceberg."""
    _ = quiet  # unused but kept for API consistency
    try:
        from pyiceberg.table import StaticTable
    except ImportError as e:
        msg = "PyIceberg is required to scan Iceberg tables. Install with: pip install datannurpy[iceberg]"
        raise ImportError(msg) from e

    # Find the latest metadata file
    metadata_dir = path / "metadata"
    metadata_files = sorted(metadata_dir.glob("*.metadata.json"), reverse=True)
    if not metadata_files:
        msg = f"No Iceberg metadata files found in {metadata_dir}"
        raise FileNotFoundError(msg)

    # Load table via PyIceberg
    # PyIceberg resolves ALL relative paths (in the metadata JSON AND in the
    # binary avro manifests) via os.path.abspath(), i.e. against the cwd.
    # When the metadata contains a relative `location`, we temporarily chdir
    # to the root directory where that relative path is valid.  This is the
    # only approach that covers every level of path resolution (JSON, avro
    # manifest-list, manifests, data files) without reimplementing pyiceberg
    # internals.  The try/finally ensures cwd is always restored.
    import json
    import os

    meta_path = metadata_files[0].resolve()
    with open(meta_path, encoding="utf-8") as f:
        raw_meta = json.load(f)

    location = raw_meta.get("location", "")
    need_chdir = bool(location) and not Path(location).is_absolute()
    saved_cwd = os.getcwd()

    if need_chdir:
        resolved = path.resolve()
        rel = Path(location)
        root = resolved
        for _ in rel.parts:
            root = root.parent
        if (root / rel).resolve() == resolved:
            os.chdir(root)

    try:
        table = StaticTable.from_metadata(str(meta_path))

        # Extract metadata from PyIceberg schema
        description = table.metadata.properties.get("comment")
        column_descriptions = {
            field.name: field.doc for field in table.schema().fields if field.doc
        }

        scan = table.scan()
        data_size = sum(task.file.file_size_in_bytes for task in scan.plan_files())

        metadata = DatasetMetadata(
            description=description,
            column_descriptions=column_descriptions if column_descriptions else None,
            data_size=data_size,
        )

        # Read data as Arrow table
        arrow_table = scan.to_arrow()
    finally:
        if need_chdir:
            os.chdir(saved_cwd)

    row_count = len(arrow_table)

    # Use DuckDB for stats computation (streaming, supports reservoir sampling)
    con = ibis.duckdb.connect()
    try:
        table = con.create_table("_iceberg", arrow_table, temp=True)
        table_name = table.get_name()

        variables, actual_sample_size, freq_table = _build_with_sampling(
            con,
            table,
            row_count=row_count,
            dataset_id=dataset_id,
            infer_stats=infer_stats,
            freq_threshold=freq_threshold,
            sample_size=sample_size,
            table_name=table_name,
        )
    finally:
        con.disconnect()

    apply_column_descriptions(variables, metadata.column_descriptions)
    metadata.sample_size = actual_sample_size

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
    sample_size: int | None = None,
) -> tuple[list[Variable], int, pa.Table | None, DatasetMetadata]:
    """Scan a Parquet dataset based on its type."""
    scanner = SCANNERS[info.type]
    assert isinstance(info.path, Path)
    return scanner(
        info.path, dataset_id, infer_stats, freq_threshold, sample_size=sample_size
    )


def scan_parquet(
    path: str | Path,
    *,
    dataset_id: str,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
    sample_size: int | None = None,
    quiet: bool = False,
) -> tuple[list[Variable], int, pa.Table | None, DatasetMetadata]:
    """Scan a simple Parquet file and return (variables, row_count, freq_table, metadata)."""
    return scan_simple(
        Path(path),
        dataset_id,
        infer_stats,
        freq_threshold,
        sample_size=sample_size,
        quiet=quiet,
    )
