"""Common utilities for scanners."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path, PurePath, PurePosixPath
from typing import TYPE_CHECKING, Any

import ibis
import ibis.expr.datatypes as dt
import pyarrow as pa

from ..schema import Variable

if TYPE_CHECKING:
    from .filesystem import FileSystem


def _to_float(val: Any) -> float | None:
    """Convert a raw aggregation result to float, or None if null."""
    if val is None:
        return None
    return float(val)


def _round6(val: Any) -> float | None:
    """Convert to float rounded to 6 decimals, or None if null."""
    if val is None:
        return None
    return round(float(val), 6)


# Supported file formats: suffix -> delivery_format
SUPPORTED_FORMATS: dict[str, str] = {
    ".csv": "csv",
    ".xlsx": "excel",
    ".xls": "excel",
    ".parquet": "parquet",
    ".pq": "parquet",
    ".sas7bdat": "sas",
    ".sav": "spss",
    ".dta": "stata",
}

# Directories and patterns to always exclude
DEFAULT_EXCLUDE_DIRS = {
    # Version control
    ".git",
    ".svn",
    ".hg",
    # Python/Node environments
    ".venv",
    "env",
    "__pycache__",
    "node_modules",
    # System/IDE artifacts
    "__MACOSX",
    ".ipynb_checkpoints",
}
DEFAULT_EXCLUDE_PREFIXES = ("~$", ".~lock.")  # Office/LibreOffice temp/lock files


def get_mtime_iso(path: PurePath, fs: FileSystem | None = None) -> str:
    """Get file modification time as YYYY/MM/DD."""
    if fs is not None:
        info = fs.info(str(path))
        mtime = info.get("mtime") or info.get("modified", 0)
        # SFTP returns datetime, others return float
        if isinstance(mtime, datetime):
            return mtime.strftime("%Y/%m/%d")
    else:
        assert isinstance(path, Path)
        mtime = path.stat().st_mtime
    dt_obj = datetime.fromtimestamp(mtime, tz=timezone.utc)
    return dt_obj.strftime("%Y/%m/%d")


def get_mtime_timestamp(path: PurePath, fs: FileSystem | None = None) -> int:
    """Get file modification time as Unix timestamp (seconds)."""
    if fs is not None:
        info = fs.info(str(path))
        mtime = info.get("mtime") or info.get("modified", 0)
        # SFTP returns datetime, others return float
        if isinstance(mtime, datetime):
            return int(mtime.timestamp())
    else:
        assert isinstance(path, Path)
        mtime = path.stat().st_mtime
    return int(mtime)


def get_data_size(path: PurePath, fs: FileSystem | None = None) -> int:
    """Get file size in bytes."""
    if fs is not None:
        info = fs.info(str(path))
        return int(info.get("size", 0))
    assert isinstance(path, Path)
    return path.stat().st_size


def get_dir_data_size(path: PurePath, fs: FileSystem | None = None) -> int:
    """Get total size of parquet files in a directory tree."""
    if fs is not None:
        path_str = str(path)
        files = fs.glob(f"{path_str}/**/*.parquet") + fs.glob(f"{path_str}/**/*.pq")
        return sum(int(fs.info(f).get("size", 0)) for f in files)
    assert isinstance(path, Path)
    files = list(path.rglob("*.parquet")) + list(path.rglob("*.pq"))
    return sum(f.stat().st_size for f in files)


def find_files(
    root: PurePath,
    include: Sequence[str] | None,
    exclude: Sequence[str] | None,
    recursive: bool,
    fs: FileSystem | None = None,
) -> list[PurePath]:
    """Find files matching include/exclude patterns."""
    # Use FileSystem if provided, otherwise use pathlib directly
    if fs is not None:
        return _find_files_with_fs(fs, root, include, exclude, recursive)
    assert isinstance(root, Path)

    if include is None:
        pattern = "**/*" if recursive else "*"
        candidates = [
            f for f in root.glob(pattern) if f.suffix.lower() in SUPPORTED_FORMATS
        ]
    else:
        candidates = []
        for pat in include:
            # Handle patterns like "folder/**" - also match files directly in folder
            if pat.endswith("/**"):
                base = pat[:-3]  # Remove /**
                # Match files in the directory and subdirectories
                candidates.extend(root.glob(f"{base}/*"))
                candidates.extend(root.glob(f"{base}/**/*"))
            elif recursive and "**" not in pat:
                candidates.extend(root.glob(f"**/{pat}"))
            else:
                candidates.extend(root.glob(pat))

    candidates = [f for f in candidates if f.is_file()]

    # Apply default exclusions
    candidates = [
        f
        for f in candidates
        if not f.name.startswith(DEFAULT_EXCLUDE_PREFIXES)
        and not any(d in f.parts for d in DEFAULT_EXCLUDE_DIRS)
    ]

    if exclude:
        excluded = set()
        for pat in exclude:
            pat = pat.rstrip("/")
            target = root / pat
            # If it's a directory, exclude all files inside
            if target.is_dir():
                for f in candidates:
                    if target.resolve() in f.resolve().parents:
                        excluded.add(f.resolve())
            # Otherwise use glob for patterns with wildcards
            elif "*" in pat:
                for f in root.glob(f"**/{pat}" if recursive else pat):
                    excluded.add(f.resolve())
            # Exact file match
            elif target.is_file():
                excluded.add(target.resolve())
        candidates = [f for f in candidates if f.resolve() not in excluded]

    return list(candidates)  # type: ignore[return-value]  # Path is PurePath


def _find_files_with_fs(
    fs: FileSystem,
    root: PurePath,
    include: Sequence[str] | None,
    exclude: Sequence[str] | None,
    recursive: bool,
) -> list[PurePath]:
    """Find files using FileSystem abstraction (for remote storage support)."""
    root_str = root.as_posix()

    if include is None:
        pattern = "**/*" if recursive else "*"
        all_paths = fs.glob(f"{root_str}/{pattern}")
        candidates = [
            p for p in all_paths if PurePosixPath(p).suffix.lower() in SUPPORTED_FORMATS
        ]
    else:
        candidates_set: set[str] = set()
        for pat in include:
            if pat.endswith("/**"):
                base = pat[:-3]
                candidates_set.update(fs.glob(f"{root_str}/{base}/*"))
                candidates_set.update(fs.glob(f"{root_str}/{base}/**/*"))
            elif recursive and "**" not in pat:
                candidates_set.update(fs.glob(f"{root_str}/**/{pat}"))
            else:
                candidates_set.update(fs.glob(f"{root_str}/{pat}"))
        candidates = list(candidates_set)

    # Filter to files only and supported formats
    candidates = [
        p
        for p in candidates
        if fs.isfile(p) and PurePosixPath(p).suffix.lower() in SUPPORTED_FORMATS
    ]

    # Apply default exclusions
    candidates = [
        p
        for p in candidates
        if not PurePosixPath(p).name.startswith(DEFAULT_EXCLUDE_PREFIXES)
        and not any(d in PurePosixPath(p).parts for d in DEFAULT_EXCLUDE_DIRS)
    ]

    if exclude:
        excluded: set[str] = set()
        for pat in exclude:
            pat = pat.rstrip("/")
            target = f"{root_str}/{pat}"
            if fs.isdir(target):
                # Exclude all files inside this directory
                for f in candidates:
                    if f.startswith(target + "/"):
                        excluded.add(f)
            elif "*" in pat:
                pattern = f"{root_str}/**/{pat}" if recursive else f"{root_str}/{pat}"
                for f in fs.glob(pattern):
                    excluded.add(f)
            elif fs.isfile(target):
                excluded.add(target)
        candidates = [f for f in candidates if f not in excluded]

    # Use PurePosixPath to preserve forward slashes for remote paths
    return sorted(PurePosixPath(p) for p in candidates)


# Geometry keywords from OGC SQL/MM standard (matched case-insensitively)
_GEOMETRY_KEYWORDS = {
    "point",
    "linestring",
    "polygon",
    "multipoint",
    "multilinestring",
    "multipolygon",
    "geometrycollection",
    "geometry",
}


def ibis_type_to_str(dtype: dt.DataType) -> str:
    """Convert Ibis dtype to string."""
    if isinstance(dtype, (dt.Int8, dt.Int16, dt.Int32, dt.Int64)):
        return "integer"
    if isinstance(dtype, (dt.UInt8, dt.UInt16, dt.UInt32, dt.UInt64)):
        return "integer"
    if isinstance(dtype, (dt.Float32, dt.Float64, dt.Decimal)):
        return "float"
    if isinstance(dtype, dt.Boolean):
        return "boolean"
    if isinstance(dtype, dt.String):
        return "string"
    if isinstance(dtype, dt.Date):
        return "date"
    if isinstance(dtype, dt.Timestamp):
        return "datetime"
    if isinstance(dtype, dt.Time):
        return "time"
    if isinstance(dtype, dt.Interval):
        return "duration"
    if isinstance(dtype, dt.GeoSpatial):
        return "geometry"
    if isinstance(dtype, dt.Null):
        return "null"
    if (
        isinstance(dtype, dt.Unknown)
        and str(dtype.raw_type).lower() in _GEOMETRY_KEYWORDS
    ):
        return "geometry"
    return "unknown"


def build_variables(
    table: ibis.Table,
    *,
    nb_rows: int,
    dataset_id: str,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
    skip_stats_columns: set[str] | None = None,
    full_table: ibis.Table | None = None,
    full_nb_rows: int | None = None,
) -> tuple[list[Variable], pa.Table | None]:
    """Build Variable entities from Ibis Table, return (variables, freq_table as PyArrow)."""
    schema = table.schema()
    columns = [c for c in schema if c.strip() != ""]
    skip_cols = set(skip_stats_columns) if skip_stats_columns else set()

    # Auto-detect columns that can't be aggregated or cast to string
    # (Binary for BLOB, Unknown for geometry types like POINT/POLYGON, GeoSpatial for GEOMETRY)
    for col_name, col_type in schema.items():
        if isinstance(col_type, (dt.Binary, dt.Unknown, dt.GeoSpatial)):
            skip_cols.add(col_name)

    # Determine which columns support min/max/mean/std
    _NUMERIC_TYPES = (
        dt.Int8,
        dt.Int16,
        dt.Int32,
        dt.Int64,
        dt.UInt8,
        dt.UInt16,
        dt.UInt32,
        dt.UInt64,
        dt.Float32,
        dt.Float64,
        dt.Decimal,
    )
    _DATE_TYPES = (dt.Date, dt.Timestamp)

    # Compute stats only if needed
    stats: dict[str, tuple[int, int, int]] = {}
    extra_stats: dict[
        str, tuple[float | None, float | None, float | None, float | None]
    ] = {}
    if infer_stats and nb_rows > 0:
        # Exclude columns that don't support aggregation (e.g., CLOB)
        cols_for_stats = [c for c in columns if c not in skip_cols]
        cols_with_extra: list[str] = []

        # Detect which columns support min/max/mean/std
        col_extra_exprs: dict[str, str] = {}  # col -> "numeric"|"string"|"date"
        for col in cols_for_stats:
            col_type = schema[col]
            if isinstance(col_type, _NUMERIC_TYPES):
                col_extra_exprs[col] = "numeric"
                cols_with_extra.append(col)
            elif isinstance(col_type, dt.String):
                col_extra_exprs[col] = "string"
                cols_with_extra.append(col)
            elif isinstance(col_type, _DATE_TYPES) and col not in skip_cols:
                col_extra_exprs[col] = "date"
                cols_with_extra.append(col)

        # When full_table is provided (sampling mode):
        #   - Streaming aggregates (count, min, max, mean, std) on full_table
        #   - Cardinality aggregates (nunique) on table (=sample memtable)
        # Otherwise: everything on table (current behavior)
        streaming_source = full_table if full_table is not None else table
        streaming_nb_rows = full_nb_rows if full_nb_rows is not None else nb_rows

        # Build streaming aggregation expressions (count, min, max, mean, std)
        streaming_aggs: list[Any] = []
        for col in cols_for_stats:
            streaming_aggs.append(
                streaming_source[col].count().name(f"{col}__non_null")
            )
            kind = col_extra_exprs.get(col)
            expr: Any = None
            if kind == "numeric":
                expr = streaming_source[col].cast("float64")
            elif kind == "string":
                str_col: Any = streaming_source[col]
                expr = str_col.length().cast("float64")
            elif kind == "date":
                date_col: Any = streaming_source[col]
                expr = date_col.epoch_seconds().cast("float64")
            if expr is not None:
                streaming_aggs.append(expr.min().name(f"{col}__min"))
                streaming_aggs.append(expr.max().name(f"{col}__max"))
                streaming_aggs.append(expr.mean().name(f"{col}__mean"))
                if streaming_nb_rows > 1:
                    streaming_aggs.append(expr.std().name(f"{col}__std"))

        # Build cardinality aggregation (nunique)
        # With full_table (sampling): approx on full data (HyperLogLog, streaming)
        # Without full_table: exact on table (all in memory)
        cardinality_aggs: list[Any] = []
        if full_table is not None:
            for col in cols_for_stats:
                streaming_aggs.append(
                    streaming_source[col].approx_nunique().name(f"{col}__distinct")
                )
        else:
            for col in cols_for_stats:
                cardinality_aggs.append(table[col].nunique().name(f"{col}__distinct"))

        if streaming_aggs or cardinality_aggs:
            try:
                streaming_row: dict[str, Any] = {}

                if full_table is None:
                    # No sampling: single combined query on table
                    all_aggs = streaming_aggs + cardinality_aggs
                    agg_table = table.aggregate(all_aggs)
                    try:
                        streaming_row = agg_table.to_pyarrow().to_pylist()[0]
                    except pa.ArrowInvalid:
                        # Oracle: Decimal values can't convert via PyArrow
                        streaming_row = dict(agg_table.execute().iloc[0])
                else:
                    # Sampling: streaming aggs + approx_nunique on full_table
                    agg_table = streaming_source.aggregate(streaming_aggs)
                    try:
                        streaming_row = agg_table.to_pyarrow().to_pylist()[0]
                    except pa.ArrowInvalid:  # pragma: no cover
                        streaming_row = dict(agg_table.execute().iloc[0])

                for col in cols_for_stats:
                    nb_distinct = int(streaming_row[f"{col}__distinct"])
                    nb_non_null = int(streaming_row[f"{col}__non_null"])
                    nb_missing = streaming_nb_rows - nb_non_null
                    nb_duplicate = nb_rows - nb_distinct
                    stats[col] = (nb_distinct, nb_duplicate, nb_missing)
                for col in cols_with_extra:
                    nb_distinct = stats[col][0] if col in stats else 0
                    nb_non_null = streaming_nb_rows - (
                        stats[col][2] if col in stats else streaming_nb_rows
                    )
                    if nb_non_null == 0:
                        extra_stats[col] = (None, None, None, None)
                        continue
                    raw_min = streaming_row[f"{col}__min"]
                    raw_max = streaming_row[f"{col}__max"]
                    raw_mean = streaming_row[f"{col}__mean"]
                    raw_std = streaming_row.get(f"{col}__std")
                    extra_stats[col] = (
                        _to_float(raw_min),
                        _to_float(raw_max),
                        _round6(raw_mean),
                        _round6(raw_std) if nb_distinct > 1 else None,
                    )
            except Exception as e:
                # Oracle ORA-22849: CLOB columns don't support COUNT DISTINCT
                if "ORA-22849" in str(e):
                    pass  # stats remains empty, all stats will be None
                else:
                    raise

    # Compute freq if threshold is set
    freq_table: pa.Table | None = None
    if freq_threshold is not None and stats:
        eligible_cols = [
            col
            for col, (nb_distinct, _, _) in stats.items()
            if 0 <= nb_distinct <= freq_threshold
        ]
        if eligible_cols:
            freq_tables: list[ibis.Table] = []
            for col in eligible_cols:
                # Value counts: group by column, count occurrences
                grouped = table.group_by(col).agg(freq=table.count())
                vc = grouped.select(
                    ibis.literal(col).name("variable_id"),
                    grouped[col].cast("string").name("value"),
                    grouped["freq"],
                )
                freq_tables.append(vc)
            # Materialize to PyArrow to allow closing the connection
            freq_table = ibis.union(*freq_tables).to_pyarrow()

    def get_stat(col: str, idx: int) -> int | None:
        """Get stat value, returning None if not computed or -1 (unknown)."""
        if not stats or col not in stats:
            return None
        val = stats[col][idx]
        return val if val >= 0 else None

    def get_extra(col: str, idx: int) -> float | None:
        """Get extra stat (min/max/mean/std), returning None if not computed."""
        if col not in extra_stats:
            return None
        return extra_stats[col][idx]

    variables = [
        Variable(
            id=col_name,
            name=col_name,
            dataset_id=dataset_id,
            type=ibis_type_to_str(schema[col_name]),
            nb_distinct=get_stat(col_name, 0),
            nb_duplicate=get_stat(col_name, 1),
            nb_missing=get_stat(col_name, 2),
            min=get_extra(col_name, 0),
            max=get_extra(col_name, 1),
            mean=get_extra(col_name, 2),
            std=get_extra(col_name, 3),
        )
        for col_name in columns
    ]

    return variables, freq_table


def build_variables_from_schema(
    schema: pa.Schema,
    dataset_id: str,
) -> list[Variable]:
    """Build Variable entities from PyArrow schema (no stats, no data read)."""
    return [
        Variable(
            id=field.name,
            name=field.name,
            dataset_id=dataset_id,
            type=pyarrow_type_to_str(field.type),
        )
        for field in schema
        if field.name.strip() != ""
    ]


def pyarrow_type_to_str(dtype: pa.DataType) -> str:
    """Convert PyArrow dtype to string."""
    if pa.types.is_integer(dtype):
        return "integer"
    if pa.types.is_floating(dtype):
        return "number"
    if pa.types.is_boolean(dtype):
        return "boolean"
    if pa.types.is_date(dtype):
        return "date"
    if pa.types.is_timestamp(dtype):
        return "datetime"
    if pa.types.is_time(dtype):
        return "time"
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return "string"
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
        return "binary"
    if pa.types.is_null(dtype):
        return "null"
    return "unknown"
