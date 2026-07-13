"""Unified file scanner that dispatches to format-specific scanners."""

from __future__ import annotations

import json
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path, PurePath, PurePosixPath
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.fs as pa_fs_module
import pyarrow.parquet as pq
import polars as pl

from ..schema import Variable
from .csv import scan_csv
from .geo import extract_geoparquet_geo
from .geo_raster import scan_geo_raster
from .geo_vector import scan_geo_vector
from .excel import (
    _MAX_PREVIEW_ROWS as _EXCEL_PREVIEW_ROWS,
    _PANDAS_ENGINES,
    _XLS_SNIFF_BYTES,
    _read_file_header,
    _read_preview_rows,
    _warn_html_xls,
    is_valid_tabular_dataset,
    _looks_like_html_xls_content,
    scan_excel,
)
from ..compression import (
    bounded_gzip_stream,
    compression_suffix,
    decompressed_cap,
    is_gzipped,
    strip_compression_suffix,
)
from .archive import local_member_from_zip, zip_csv_member_header
from .format_detect import canonical_extension
from .parquet import scan_parquet
from .parquet.core import scan_delta, scan_hive, scan_iceberg
from .statistical import scan_statistical
from .utils import FsPath, build_variables_from_schema, is_zip

_PA_PY_FILE_SYSTEM = getattr(pa_fs_module, "PyFileSystem")
_PA_FSSPEC_HANDLER = getattr(pa_fs_module, "FSSpecHandler")

# Vector geo formats scanned through pyogrio (optional ``geo`` extra). pyogrio picks
# the driver from the extension. Shapefile sidecars (.shx/.dbf/.prj/.cpg) are
# unmapped, so only the .shp becomes a dataset. Multi-layer sources (KML folders,
# GML, GPX waypoints/routes/tracks) scan their first layer.
_VECTOR_FORMATS = ("geojson", "shapefile", "gml", "kml", "gpx")
# Geo formats are read by dedicated scanners (not the tabular schema path), so they
# bypass schema-only mode — their metadata is cheap and wanted at every depth.
_GEO_FORMATS = (*_VECTOR_FORMATS, "geotiff")

if TYPE_CHECKING:
    from .filesystem import FileSystem


@dataclass
class ScanResult:
    """Result of scanning a file."""

    variables: list[Variable]
    nb_row: int | None
    sample_size: int | None = None
    freq_table: pa.Table | None = None
    description: str | None = None
    name: str | None = None  # Dataset name from metadata (Delta, Iceberg)
    data_size: int | None = None
    preview: pl.DataFrame | None = None
    # Geo metadata (GeoParquet, …); None for non-spatial datasets.
    crs: str | None = None
    geometry_type: str | None = None
    bbox: list[float] | None = None
    spatial_resolution: float | None = None


def scan_file(
    path: FsPath,
    delivery_format: str,
    *,
    dataset_id: str,
    schema_only: bool = False,
    freq_threshold: int | None = None,
    csv_encoding: str | None = None,
    sample_size: int | None = None,
    preview_rows: int = 0,
    csv_skip_copy: bool = False,
    fs: FileSystem | None = None,
    quiet: bool = False,
    path_label: str | None = None,
) -> ScanResult:
    """Scan a file and return variables, row count, and optional metadata.

    Args:
        schema_only: If True, only read schema (no data, no row count, no stats).
        fs: Optional FileSystem for remote file access. Non-streamable formats
            (CSV, Excel, SAS/SPSS/Stata) will be downloaded to a temp file.
    """
    with ExitStack() as stack:
        # Zip archive: extract the single scannable member (+ same-stem sidecars for
        # a Shapefile) to a temp dir and continue as a local file — every downstream
        # path (geo sidecars, schema-only, suffix-driven readers) is reused unchanged.
        # A .zip-named resource that is not actually an archive yields None and is
        # scanned as its declared format (a misnamed endpoint serving plain data).
        if is_zip(path_label or PurePosixPath(str(path)).name):
            # Schema-only zipped CSV: stream just the header out of the archive —
            # extracting a potentially huge member to list column names would turn
            # a metadata sweep into a full download per dataset.
            if schema_only and delivery_format == "csv":
                from .csv import _CSV_HEADER_SAMPLE_BYTES

                header = zip_csv_member_header(path, fs, _CSV_HEADER_SAMPLE_BYTES)
                if header is not None:
                    return _csv_header_result(header, csv_encoding, dataset_id)
            member_path = stack.enter_context(
                local_member_from_zip(path, fs, delivery_format)
            )
            if member_path is not None:
                path, fs = member_path, None

        # Schema-only mode: read metadata without scanning data. Geo formats have no
        # tabular schema path, so they always go through their own scanners.
        if schema_only and delivery_format not in _GEO_FORMATS:
            return _scan_schema_only(
                path,
                delivery_format,
                dataset_id,
                csv_encoding,
                fs=fs,
                quiet=quiet,
                path_label=path_label,
            )

        # Remote filesystem: use ensure_local for all formats
        if fs is not None and not fs.is_local:
            return _scan_with_ensure_local(
                path,
                delivery_format,
                dataset_id=dataset_id,
                freq_threshold=freq_threshold,
                csv_encoding=csv_encoding,
                sample_size=sample_size,
                preview_rows=preview_rows,
                csv_skip_copy=csv_skip_copy,
                fs=fs,
                quiet=quiet,
                path_label=path_label,
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
            preview_rows=preview_rows,
            csv_skip_copy=csv_skip_copy,
            quiet=quiet,
            path_label=path_label,
        )


def _csv_header_result(
    header: bytes, csv_encoding: str | None, dataset_id: str
) -> ScanResult:
    """Build a schema-only ScanResult from a CSV's raw header bytes."""
    from .csv import _read_csv_header

    return ScanResult(
        variables=[
            Variable(id=f"{dataset_id}---{col}", name=col, dataset_id=dataset_id)
            for col in _read_csv_header(header, csv_encoding)
        ],
        nb_row=None,
    )


def _scan_local(
    path: Path,
    delivery_format: str,
    *,
    dataset_id: str,
    freq_threshold: int | None,
    csv_encoding: str | None,
    sample_size: int | None,
    preview_rows: int,
    csv_skip_copy: bool,
    quiet: bool = False,
    path_label: str | None = None,
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
        variables, nb_row, freq_table, metadata, preview = parquet_scanners[
            delivery_format
        ](
            path,
            dataset_id=dataset_id,
            freq_threshold=freq_threshold,
            sample_size=sample_size,
            preview_rows=preview_rows,
            return_preview=True,
            quiet=quiet,
        )
        # GeoParquet keys (crs, geometry_type, bbox) match ScanResult's fields.
        geo = extract_geoparquet_geo(path) if delivery_format == "parquet" else None
        return ScanResult(
            variables=variables,
            nb_row=nb_row,
            sample_size=metadata.sample_size if metadata else None,
            freq_table=freq_table,
            description=metadata.description if metadata else None,
            name=metadata.name if metadata else None,
            data_size=metadata.data_size if metadata else None,
            preview=preview,
            **(geo or {}),
        )

    if delivery_format in ("sas", "spss", "stata"):
        variables, nb_row, actual_sample_size, freq_table, metadata, preview = (
            scan_statistical(
                path,
                dataset_id=dataset_id,
                freq_threshold=freq_threshold,
                sample_size=sample_size,
                preview_rows=preview_rows,
                return_preview=True,
                quiet=quiet,
                path_label=path_label,
            )
        )
        return ScanResult(
            variables=variables,
            nb_row=nb_row,
            sample_size=actual_sample_size,
            freq_table=freq_table,
            description=metadata.description if metadata else None,
            preview=preview,
        )

    if delivery_format in _VECTOR_FORMATS:
        variables, nb_row, freq_table, geo, preview = scan_geo_vector(
            path,
            dataset_id=dataset_id,
            freq_threshold=freq_threshold,
            preview_rows=preview_rows,
            return_preview=True,
            quiet=quiet,
            path_label=path_label,
        )
        return ScanResult(
            variables=variables,
            nb_row=nb_row,
            freq_table=freq_table,
            preview=preview,
            **(geo or {}),
        )

    if delivery_format == "geotiff":
        variables, nb_row, geo, spatial_resolution = scan_geo_raster(
            path, dataset_id=dataset_id, quiet=quiet, path_label=path_label
        )
        return ScanResult(
            variables=variables,
            nb_row=nb_row,
            spatial_resolution=spatial_resolution,
            **(geo or {}),
        )

    if delivery_format == "csv":
        variables, nb_row, actual_sample_size, freq_table, preview = scan_csv(
            path,
            dataset_id=dataset_id,
            freq_threshold=freq_threshold,
            csv_encoding=csv_encoding,
            sample_size=sample_size,
            preview_rows=preview_rows,
            return_preview=True,
            csv_skip_copy=csv_skip_copy,
            quiet=quiet,
            path_label=path_label,
        )
        return ScanResult(
            variables=variables,
            nb_row=nb_row,
            sample_size=actual_sample_size,
            freq_table=freq_table,
            preview=preview,
        )

    # Spreadsheets (xls, xlsx, ods)
    variables, nb_row, freq_table, preview = scan_excel(
        path,
        dataset_id=dataset_id,
        freq_threshold=freq_threshold,
        preview_rows=preview_rows,
        return_preview=True,
        quiet=quiet,
        path_label=path_label,
    )
    return ScanResult(
        variables=variables,
        nb_row=nb_row,
        freq_table=freq_table,
        preview=preview,
    )


def _temp_local_name(path: FsPath, delivery_format: str, path_label: str | None) -> str:
    """Safe temp filename for a downloaded remote file, carrying the resolved format's
    extension so suffix-sensitive readers (Excel engine, pyogrio) work even when the
    URL has no extension or a query string (which would corrupt the raw basename). A
    content-compression suffix is preserved (``…csv.gz``) so the local reader still sees
    it as gzipped and decompresses it in turn."""
    segment = path_label or PurePosixPath(str(path)).name
    inner = strip_compression_suffix(segment)
    return f"data{canonical_extension(inner, delivery_format)}{compression_suffix(segment)}"


def _scan_with_ensure_local(
    path: FsPath,
    delivery_format: str,
    *,
    dataset_id: str,
    freq_threshold: int | None,
    csv_encoding: str | None,
    sample_size: int | None,
    preview_rows: int,
    csv_skip_copy: bool,
    fs: FileSystem,
    quiet: bool = False,
    path_label: str | None = None,
) -> ScanResult:
    """Download remote file/directory and scan locally."""
    _DIR_FORMATS = ("delta", "hive", "iceberg")
    if delivery_format == "excel" and PurePath(path).suffix.lower() == ".xls":
        with fs.open(str(path), "rb") as f:
            if _looks_like_html_xls_content(f.read(_XLS_SNIFF_BYTES)):
                _warn_html_xls(path_label or PurePath(path).name, quiet)
                return ScanResult(variables=[], nb_row=None)
    if delivery_format == "shapefile":
        # .shp needs its .shx/.dbf/.prj companions, kept under their real names.
        cm = fs.ensure_local_siblings(str(path))
    elif delivery_format in _DIR_FORMATS:
        cm = fs.ensure_local_dir(str(path))
    else:
        cm = fs.ensure_local(
            str(path), _temp_local_name(path, delivery_format, path_label)
        )
    with cm as local_path:
        return _scan_local(
            local_path,
            delivery_format,
            dataset_id=dataset_id,
            freq_threshold=freq_threshold,
            csv_encoding=csv_encoding,
            sample_size=sample_size,
            preview_rows=preview_rows,
            csv_skip_copy=csv_skip_copy,
            quiet=quiet,
            path_label=path_label,
        )


def _scan_schema_only(
    path: FsPath,
    delivery_format: str,
    dataset_id: str,
    csv_encoding: str | None = None,
    fs: FileSystem | None = None,
    quiet: bool = False,
    path_label: str | None = None,
) -> ScanResult:
    """Read schema only without scanning data (for depth='schema' mode)."""
    # Remote filesystem: use optimized partial downloads
    if fs is not None and not fs.is_local:
        return _scan_schema_only_remote(
            path, delivery_format, dataset_id, csv_encoding, fs, quiet, path_label
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

    return _scan_schema_only_local(
        path, delivery_format, dataset_id, csv_encoding, quiet, path_label
    )


def _scan_stat_schema_stream(
    path: FsPath,
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


def _scan_schema_only_remote(
    path: FsPath,
    delivery_format: str,
    dataset_id: str,
    csv_encoding: str | None,
    fs: FileSystem,
    quiet: bool = False,
    path_label: str | None = None,
) -> ScanResult:
    """Optimized schema-only scan for remote files."""
    # Parquet: PyArrow reads footer natively via fsspec (no full download)
    if delivery_format == "parquet":
        pa_fs = _PA_PY_FILE_SYSTEM(_PA_FSSPEC_HANDLER(fs.fs))
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
            pa_fs = _PA_PY_FILE_SYSTEM(_PA_FSSPEC_HANDLER(fs.fs))
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
        name = _temp_local_name(path, delivery_format, path_label)
        with fs.ensure_local(str(path), name) as local_path:
            return _scan_schema_only_local(
                local_path, delivery_format, dataset_id, path_label=path_label
            )

    # CSV: stream only the header line (readline guarantees a complete line)
    if delivery_format == "csv":
        from .csv import _CSV_HEADER_SAMPLE_BYTES

        full_path = fs._full_path(str(path))
        source_name = path_label or PurePosixPath(str(path)).name
        with fs.fs.open(full_path, "rb") as f:
            # A .gz header decompresses to well under the sample bound, so the read is
            # self-limiting; the cap only matters on full-file decompression.
            src = (
                bounded_gzip_stream(f, decompressed_cap(0))
                if is_gzipped(source_name)
                else f
            )
            header_bytes = src.read(_CSV_HEADER_SAMPLE_BYTES)
        return _csv_header_result(header_bytes, csv_encoding, dataset_id)

    # Excel xlsx: download first so ZIP/XML access stays local.
    # xls: must download full file (xlrd doesn't support streaming)
    name = _temp_local_name(path, delivery_format, path_label)
    suffix = PurePath(path).suffix.lower()
    if suffix != ".xls":
        with fs.ensure_local(str(path), name) as local_path:
            return _scan_schema_only_local(
                local_path,
                delivery_format,
                dataset_id,
                csv_encoding,
                quiet,
                path_label,
            )
    with fs.open(str(path), "rb") as f:
        if _looks_like_html_xls_content(f.read(_XLS_SNIFF_BYTES)):
            _warn_html_xls(path_label or PurePath(path).name, quiet)
            return ScanResult(variables=[], nb_row=None)
    with fs.ensure_local(str(path), name) as local_path:
        return _scan_schema_only_local(
            local_path, delivery_format, dataset_id, csv_encoding, quiet, path_label
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
    quiet: bool = False,
    path_label: str | None = None,
) -> ScanResult:
    """Schema-only scan for local files."""
    if delivery_format == "csv":
        from .csv import _CSV_HEADER_SAMPLE_BYTES

        with open(path, "rb") as f:
            src = (
                bounded_gzip_stream(f, decompressed_cap(path.stat().st_size))
                if is_gzipped(path.name)
                else f
            )
            header_bytes = src.read(_CSV_HEADER_SAMPLE_BYTES)
        return _csv_header_result(header_bytes, csv_encoding, dataset_id)

    if delivery_format in ("excel", "ods"):
        file_path = Path(path)
        suffix = file_path.suffix.lower()

        if suffix not in _PANDAS_ENGINES:
            rows = _read_preview_rows(
                file_path, quiet=quiet, path_label=path_label or file_path.name
            )
            valid, _reason = is_valid_tabular_dataset(rows)
            if not valid:
                return ScanResult(variables=[], nb_row=None)
            headers = [str(c) for c in rows[0] if c is not None]
        else:
            if suffix == ".xls" and _looks_like_html_xls_content(
                _read_file_header(file_path)
            ):
                _warn_html_xls(path_label or file_path.name, quiet)
                return ScanResult(variables=[], nb_row=None)
            import pandas as pd

            engine = _PANDAS_ENGINES[suffix]
            from .excel import _capture_excel_diagnostics

            with _capture_excel_diagnostics(path_label or file_path.name, quiet):
                df = pd.read_excel(file_path, nrows=_EXCEL_PREVIEW_ROWS, engine=engine)
            header_row = tuple(df.columns)
            data_rows = [tuple(row) for row in df.itertuples(index=False)]
            valid, _reason = is_valid_tabular_dataset([header_row, *data_rows])
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
        path, dataset_id=dataset_id, infer_stats=False, path_label=path_label
    )
    return ScanResult(
        variables=variables,
        nb_row=None,
        description=metadata.description if metadata else None,
    )
