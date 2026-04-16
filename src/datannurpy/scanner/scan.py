"""Unified file scanner that dispatches to format-specific scanners."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path, PurePath
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.fs
import pyarrow.parquet as pq

from ..schema import Variable
from .csv import scan_csv
from .excel import (
    _MAX_PREVIEW_ROWS as _EXCEL_PREVIEW_ROWS,
    _read_preview_rows,
    is_valid_excel_dataset,
    scan_excel,
)
from .parquet import scan_parquet
from .parquet.core import scan_delta, scan_hive, scan_iceberg
from .statistical import scan_statistical
from .utils import build_variables_from_schema

if TYPE_CHECKING:
    from .filesystem import FileSystem


@dataclass
class ScanResult:
    """Result of scanning a file."""

    variables: list[Variable]
    nb_row: int | None
    freq_table: pa.Table | None = None
    description: str | None = None
    name: str | None = None  # Dataset name from metadata (Delta, Iceberg)
    data_size: int | None = None


def scan_file(
    path: PurePath,
    delivery_format: str,
    *,
    dataset_id: str,
    schema_only: bool = False,
    freq_threshold: int | None = None,
    csv_encoding: str | None = None,
    sample_size: int | None = None,
    csv_skip_copy: bool = False,
    fs: FileSystem | None = None,
    quiet: bool = False,
) -> ScanResult:
    """Scan a file and return variables, row count, and optional metadata.

    Args:
        schema_only: If True, only read schema (no data, no row count, no stats).
        fs: Optional FileSystem for remote file access. Non-streamable formats
            (CSV, Excel, SAS/SPSS/Stata) will be downloaded to a temp file.
    """
    # Schema-only mode: read metadata without scanning data
    if schema_only:
        return _scan_schema_only(path, delivery_format, dataset_id, csv_encoding, fs=fs)

    # Remote filesystem: use ensure_local for all formats
    if fs is not None and not fs.is_local:
        return _scan_with_ensure_local(
            path,
            delivery_format,
            dataset_id=dataset_id,
            freq_threshold=freq_threshold,
            csv_encoding=csv_encoding,
            sample_size=sample_size,
            csv_skip_copy=csv_skip_copy,
            fs=fs,
            quiet=quiet,
        )

    # Local path: concrete Path required for direct scanning
    assert isinstance(path, Path)
    return _scan_local(
        path,
        delivery_format,
        dataset_id=dataset_id,
        freq_threshold=freq_threshold,
        csv_encoding=csv_encoding,
        sample_size=sample_size,
        csv_skip_copy=csv_skip_copy,
        quiet=quiet,
    )


def _scan_local(
    path: Path,
    delivery_format: str,
    *,
    dataset_id: str,
    freq_threshold: int | None,
    csv_encoding: str | None,
    sample_size: int | None,
    csv_skip_copy: bool,
    quiet: bool = False,
) -> ScanResult:
    """Dispatch to format-specific scanner and build ScanResult."""
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
            freq_threshold=freq_threshold,
            sample_size=sample_size,
            quiet=quiet,
        )
        return ScanResult(
            variables=variables,
            nb_row=nb_row,
            freq_table=freq_table,
            description=metadata.description if metadata else None,
            name=metadata.name if metadata else None,
            data_size=metadata.data_size if metadata else None,
        )

    if delivery_format in ("sas", "spss", "stata"):
        variables, nb_row, _actual_sample_size, freq_table, metadata = scan_statistical(
            path,
            dataset_id=dataset_id,
            freq_threshold=freq_threshold,
            sample_size=sample_size,
            quiet=quiet,
        )
        return ScanResult(
            variables=variables,
            nb_row=nb_row,
            freq_table=freq_table,
            description=metadata.description if metadata else None,
        )

    if delivery_format == "csv":
        variables, nb_row, _actual_sample_size, freq_table = scan_csv(
            path,
            dataset_id=dataset_id,
            freq_threshold=freq_threshold,
            csv_encoding=csv_encoding,
            sample_size=sample_size,
            csv_skip_copy=csv_skip_copy,
            quiet=quiet,
        )
        return ScanResult(variables=variables, nb_row=nb_row, freq_table=freq_table)

    # Excel (xls, xlsx)
    variables, nb_row, freq_table = scan_excel(
        path,
        dataset_id=dataset_id,
        freq_threshold=freq_threshold,
        quiet=quiet,
    )
    return ScanResult(variables=variables, nb_row=nb_row, freq_table=freq_table)


def _scan_with_ensure_local(
    path: PurePath,
    delivery_format: str,
    *,
    dataset_id: str,
    freq_threshold: int | None,
    csv_encoding: str | None,
    sample_size: int | None,
    csv_skip_copy: bool,
    fs: FileSystem,
    quiet: bool = False,
) -> ScanResult:
    """Download remote file/directory and scan locally."""
    _DIR_FORMATS = ("delta", "hive", "iceberg")
    ctx = fs.ensure_local_dir if delivery_format in _DIR_FORMATS else fs.ensure_local
    with ctx(str(path)) as local_path:
        return _scan_local(
            local_path,
            delivery_format,
            dataset_id=dataset_id,
            freq_threshold=freq_threshold,
            csv_encoding=csv_encoding,
            sample_size=sample_size,
            csv_skip_copy=csv_skip_copy,
            quiet=quiet,
        )


def _scan_schema_only(
    path: PurePath,
    delivery_format: str,
    dataset_id: str,
    csv_encoding: str | None = None,
    fs: FileSystem | None = None,
) -> ScanResult:
    """Read schema only without scanning data (for depth='schema' mode)."""
    # Remote filesystem: use optimized partial downloads
    if fs is not None and not fs.is_local:
        return _scan_schema_only_remote(
            path, delivery_format, dataset_id, csv_encoding, fs
        )

    # Local: read schema directly
    assert isinstance(path, Path)
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

    return _scan_schema_only_local(path, delivery_format, dataset_id, csv_encoding)


def _scan_stat_schema_stream(
    path: PurePath,
    delivery_format: str,
    dataset_id: str,
    fs: FileSystem,
) -> ScanResult:
    """Read SAS/Stata schema via pandas streaming (avoids full file download)."""
    description: str | None = None
    names: list[str] = []
    label_map: dict[str, str] = {}
    type_map: dict[str, str] = {}

    with fs.open(str(path), "rb") as f:
        if delivery_format == "sas":
            from pandas.io.sas.sas7bdat import SAS7BDATReader

            with SAS7BDATReader(f) as sas_reader:
                names = [str(col.name) for col in sas_reader.columns]
                label_map = {
                    str(col.name): str(col.label)
                    for col in sas_reader.columns
                    if col.label
                }
                type_map = {
                    str(col.name): "float" if col.ctype == b"d" else "string"
                    for col in sas_reader.columns
                }
        else:  # stata
            from pandas.io.stata import StataReader

            with StataReader(f) as stata_reader:
                label_map = stata_reader.variable_labels()
                names = list(label_map.keys())
                description = stata_reader.data_label or None
                dt: Any = stata_reader._dtype
                if dt is not None and dt.names:
                    for i, name in enumerate(names):
                        kind = dt.fields[dt.names[i]][0].kind
                        type_map[name] = "float" if kind == "f" else "string"

    variables = [
        Variable(
            id=f"{dataset_id}---{name}",
            name=name,
            dataset_id=dataset_id,
            description=label_map.get(name) or None,
            type=type_map.get(name),
        )
        for name in names
    ]
    return ScanResult(variables=variables, nb_row=None, description=description)


def _scan_excel_schema_stream(
    path: PurePath,
    dataset_id: str,
    fs: FileSystem,
) -> ScanResult:
    """Read xlsx headers via openpyxl streaming (avoids full file download)."""
    with fs.open(str(path), "rb") as f:
        rows = _read_preview_rows(f)

    valid, _reason = is_valid_excel_dataset(rows)
    if not valid:
        return ScanResult(variables=[], nb_row=None)

    headers = [str(c) for c in rows[0] if c is not None]

    variables = [
        Variable(
            id=f"{dataset_id}---{name}",
            name=name,
            dataset_id=dataset_id,
        )
        for name in headers
    ]
    return ScanResult(variables=variables, nb_row=None)


def _scan_schema_only_remote(
    path: PurePath,
    delivery_format: str,
    dataset_id: str,
    csv_encoding: str | None,
    fs: FileSystem,
) -> ScanResult:
    """Optimized schema-only scan for remote files - minimal downloads."""
    # Parquet: PyArrow reads footer natively via fsspec (no full download)
    if delivery_format == "parquet":
        pa_fs = pyarrow.fs.PyFileSystem(pyarrow.fs.FSSpecHandler(fs.fs))
        full_path = fs._full_path(str(path))
        schema = pq.read_schema(full_path, filesystem=pa_fs)
        variables = build_variables_from_schema(schema, dataset_id)
        return ScanResult(variables=variables, nb_row=None)

    # Delta: download only _delta_log/ directory
    if delivery_format == "delta":
        delta_log_path = f"{path}/_delta_log"
        with fs.ensure_local_dir(delta_log_path) as local_log:
            from deltalake import DeltaTable

            # DeltaTable needs the parent directory containing _delta_log
            dt = DeltaTable(local_log.parent)
            schema = pa.schema(dt.schema().to_arrow())
            variables = build_variables_from_schema(schema, dataset_id)
            return ScanResult(variables=variables, nb_row=None)

    # Iceberg: download only metadata/ directory
    if delivery_format == "iceberg":
        metadata_path = f"{path}/metadata"
        with fs.ensure_local_dir(metadata_path) as local_meta:
            # Read schema from latest metadata file
            meta_files = sorted(local_meta.glob("*.metadata.json"), reverse=True)
            if meta_files:
                meta_content = json.loads(meta_files[0].read_text())
                # Parse Iceberg schema from JSON
                schema_fields = meta_content.get("schemas", [{}])[-1].get("fields", [])
                pa_fields = []
                for f in schema_fields:
                    pa_type = _iceberg_type_to_pyarrow(f.get("type", "string"))
                    pa_fields.append(pa.field(f["name"], pa_type))
                schema = pa.schema(pa_fields)
            else:
                schema = pa.schema([])
            variables = build_variables_from_schema(schema, dataset_id)
            return ScanResult(variables=variables, nb_row=None)

    # Hive: find one parquet file and read its schema
    if delivery_format == "hive":
        # List parquet files remotely
        parquet_files = fs.glob(f"{path}/**/*.parquet")
        if parquet_files:
            pa_fs = pyarrow.fs.PyFileSystem(pyarrow.fs.FSSpecHandler(fs.fs))
            schema = pq.read_schema(parquet_files[0], filesystem=pa_fs)
        else:
            schema = pa.schema([])
        variables = build_variables_from_schema(schema, dataset_id)
        return ScanResult(variables=variables, nb_row=None)

    # SAS/Stata: pandas streaming reads only header bytes (no full download)
    if delivery_format in ("sas", "stata"):
        return _scan_stat_schema_stream(path, delivery_format, dataset_id, fs)

    # SPSS: must download full file (pd.read_spss wraps pyreadstat, no streaming)
    if delivery_format == "spss":
        with fs.ensure_local(str(path)) as local_path:
            return _scan_schema_only_local(local_path, delivery_format, dataset_id)

    # CSV: stream only the header line (readline guarantees a complete line)
    if delivery_format == "csv":
        from .csv import _read_csv_header

        full_path = fs._full_path(str(path))
        with fs.fs.open(full_path, "rb") as f:
            header_bytes = f.readline()
        columns = _read_csv_header(header_bytes, csv_encoding)
        variables = [
            Variable(
                id=f"{dataset_id}---{col}",
                name=col,
                dataset_id=dataset_id,
            )
            for col in columns
        ]
        return ScanResult(variables=variables, nb_row=None)

    # Excel xlsx: openpyxl read_only streams only headers (no full download)
    # xls: must download full file (xlrd doesn't support streaming)
    suffix = PurePath(path).suffix.lower()
    if suffix != ".xls":
        return _scan_excel_schema_stream(path, dataset_id, fs)
    with fs.ensure_local(str(path)) as local_path:
        return _scan_schema_only_local(
            local_path, delivery_format, dataset_id, csv_encoding
        )


def _iceberg_type_to_pyarrow(iceberg_type: str | dict) -> pa.DataType:
    """Convert Iceberg type to PyArrow type."""
    if isinstance(iceberg_type, dict):
        type_name = iceberg_type.get("type", "string")
    else:
        type_name = iceberg_type

    type_map = {
        "boolean": pa.bool_(),
        "int": pa.int32(),
        "long": pa.int64(),
        "float": pa.float32(),
        "double": pa.float64(),
        "string": pa.string(),
        "binary": pa.binary(),
        "date": pa.date32(),
        "timestamp": pa.timestamp("us"),
        "timestamptz": pa.timestamp("us", tz="UTC"),
    }
    return type_map.get(type_name, pa.string())


def _scan_schema_only_local(
    path: Path,
    delivery_format: str,
    dataset_id: str,
    csv_encoding: str | None = None,
) -> ScanResult:
    """Schema-only scan for local files."""
    if delivery_format == "csv":
        from .csv import _read_csv_header

        with open(path, "rb") as f:
            columns = _read_csv_header(f.readline(), csv_encoding)
        variables = [
            Variable(
                id=f"{dataset_id}---{col}",
                name=col,
                dataset_id=dataset_id,
            )
            for col in columns
        ]
        return ScanResult(variables=variables, nb_row=None)

    if delivery_format == "excel":
        file_path = Path(path)
        suffix = file_path.suffix.lower()

        if suffix != ".xls":
            rows = _read_preview_rows(file_path)
            valid, _reason = is_valid_excel_dataset(rows)
            if not valid:
                return ScanResult(variables=[], nb_row=None)
            headers = [str(c) for c in rows[0] if c is not None]
        else:
            import pandas as pd

            engine = "xlrd"
            df = pd.read_excel(file_path, nrows=_EXCEL_PREVIEW_ROWS, engine=engine)
            header_row = tuple(df.columns)
            data_rows = [tuple(row) for row in df.itertuples(index=False)]
            valid, _reason = is_valid_excel_dataset([header_row, *data_rows])
            if not valid:
                return ScanResult(variables=[], nb_row=None)
            headers = [str(c) for c in df.columns if str(c).strip() != ""]

        variables = [
            Variable(
                id=f"{dataset_id}---{col}",
                name=col,
                dataset_id=dataset_id,
            )
            for col in headers
        ]
        return ScanResult(variables=variables, nb_row=None)

    # statistical formats
    variables, _, _, _, metadata = scan_statistical(
        path, dataset_id=dataset_id, infer_stats=False
    )
    return ScanResult(
        variables=variables,
        nb_row=None,
        description=metadata.description if metadata else None,
    )
