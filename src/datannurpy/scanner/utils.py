"""Common utilities for scanners."""

from __future__ import annotations

import errno
import fnmatch
import math
import os
from collections.abc import Sequence
from contextlib import suppress
from datetime import datetime
from email.utils import parsedate_to_datetime
from functools import lru_cache
from pathlib import Path, PurePath, PurePosixPath
from typing import TYPE_CHECKING, Any, Union

import ibis
import ibis.expr.datatypes as dt

import duckdb as _duckdb

from ..compression import strip_compression_suffix
from ..schema import Variable
from ..utils.log import log_warn
from ..utils.time import timestamp_to_iso

if TYPE_CHECKING:
    from collections.abc import Iterator

    import pyarrow as pa

    from .filesystem import FileSystem

# A filesystem path carrier. Local paths are concrete ``Path`` objects; remote paths
# are whatever fsspec expects as a string. Most remote backends expose a ``/``-rooted
# POSIX path (safely a ``PurePosixPath``), but URL-rooted backends (http/https) expose
# a full URL that ``PurePosixPath`` would corrupt by collapsing the ``//`` after the
# scheme, so those are kept as the raw ``str``. Callers only ever ``str()`` a remote
# path (local-only operations stay guarded behind ``isinstance(path, Path)``).
FsPath = Union[str, PurePath]


def _to_float(val: Any) -> float | None:
    """Convert a raw aggregation result to float, or None if null or non-finite
    (an AVG over extreme magnitudes silently overflows to inf, which strict JSON
    cannot carry)."""
    if val is None:
        return None
    result = float(val)
    return result if math.isfinite(result) else None


def _round6(val: Any) -> float | None:
    """Convert to float rounded to 6 decimals, or None if null or non-finite."""
    if val is None:
        return None
    result = float(val)
    return round(result, 6) if math.isfinite(result) else None


def _table_to_arrow(table: ibis.Table) -> pa.Table:
    """Convert an Ibis table to Arrow, falling back through pandas when needed."""
    import pyarrow as pa

    try:
        return table.to_pyarrow()
    except Exception:
        result = table.execute()
        if isinstance(result, pa.Table):
            return result
        return pa.Table.from_pandas(result, preserve_index=False)


# Maximum row count above which a remote/file-backed table is *not*
# materialized into memory before per-column value-level passes (autotag,
# frequency, pattern). Below this threshold materialization avoids re-scanning
# the source once per column, which dominates wall time on wide datasets.
_MATERIALIZE_MAX_ROWS = 1_000_000


# Supported file formats: suffix -> delivery_format
SUPPORTED_FORMATS: dict[str, str] = {
    ".csv": "csv",
    ".xlsx": "excel",
    ".xls": "excel",
    ".ods": "ods",
    ".parquet": "parquet",
    ".pq": "parquet",
    ".sas7bdat": "sas",
    ".sav": "spss",
    ".dta": "stata",
    ".geojson": "geojson",
    ".shp": "shapefile",
    ".gml": "gml",
    ".kml": "kml",
    ".gpx": "gpx",
    ".tif": "geotiff",
    ".tiff": "geotiff",
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
_FS_DIR_TYPES = {"directory", "dir"}
_FS_FILE_TYPES = {"file"}


# delivery_formats whose scanner transparently decompresses a gzip source. Only these
# may be reached through a ``.gz`` suffix; a ``data.parquet.gz`` stays unsupported rather
# than being admitted and then failing to scan as raw gzip bytes.
GZIP_INNER_FORMATS = frozenset({"csv"})


def supported_format_for(name: str) -> str | None:
    """delivery_format for a filename, seeing through a *decompressible* content
    compression suffix (``sales.csv.gz`` → ``csv``). None when the inner extension is
    unsupported, or when a gzip wraps a format we don't decompress (``x.parquet.gz``)."""
    base = PurePosixPath(name).name
    inner = strip_compression_suffix(base)
    fmt = SUPPORTED_FORMATS.get(PurePosixPath(inner).suffix.lower())
    if inner != base and fmt not in GZIP_INNER_FORMATS:
        return None
    return fmt


def is_zip(name: str) -> bool:
    """Whether ``name`` denotes a zip archive (``*.zip``), ignoring any URL query
    string or fragment (``data.zip?token=…``)."""
    clean = name.split("?", 1)[0].split("#", 1)[0]
    return PurePosixPath(clean).suffix.lower() == ".zip"


def is_geopackage(name: str) -> bool:
    """Whether ``name`` denotes a GeoPackage (``*.gpkg``) — a SQLite container
    scanned through the database machinery, not a ``scan_file`` format."""
    return PurePosixPath(name).suffix.lower() == ".gpkg"


def fs_info_is_dir(fs: FileSystem, path: str) -> bool:
    """Return whether fsspec info identifies a directory."""
    info = fs.info(path)
    if isinstance(info, dict):
        path_type = info.get("type")
        if path_type is not None:
            return str(path_type).lower() in _FS_DIR_TYPES
    if not fs.exists(path):
        raise FileNotFoundError(path)
    return fs.isdir(path)


# ---------------------------------------------------------------------------
# Permission-tolerant traversal helpers
#
# Directory listings can fail on directories the user can `stat`/traverse but
# not list (typical on SFTP with restrictive ACLs, or on local filesystems
# with `chmod 0`). The helpers below log a warning and skip the offending
# path instead of letting `PermissionError` propagate and abort the scan.
# ---------------------------------------------------------------------------


def _is_permission_error(exc: BaseException) -> bool:
    """Return True for errors that mean 'cannot list this directory'."""
    if isinstance(exc, PermissionError):
        return True
    return bool(isinstance(exc, OSError) and exc.errno in (errno.EACCES, errno.EPERM))


def _warn_permission(path: Any, exc: BaseException) -> None:
    """Emit a warning that a path was skipped because it is not listable."""
    log_warn(f"{path}: skipped (permission denied: {exc})", quiet=False)


def safe_iterdir_fs(fs: FileSystem, path: str) -> Iterator[str]:
    """Iterate over `fs.iterdir(path)`, skipping with a warning on EACCES."""
    try:
        yield from fs.iterdir(path)
    except OSError as exc:
        if _is_permission_error(exc):
            _warn_permission(path, exc)
            return
        raise


def safe_iterdir_detailed_fs(
    fs: FileSystem, path: str
) -> Iterator[tuple[str, dict[str, Any]]]:
    """Iterate `fs.iterdir_detailed(path)`, skipping with a warning on EACCES."""
    try:
        yield from fs.iterdir_detailed(path)
    except OSError as exc:
        if _is_permission_error(exc):
            _warn_permission(path, exc)
            return
        raise


def safe_iterdir_local(path: Path) -> Iterator[Path]:
    """Iterate over `path.iterdir()`, skipping with a warning on EACCES."""
    try:
        yield from path.iterdir()
    except OSError as exc:
        if _is_permission_error(exc):
            _warn_permission(path, exc)
            return
        raise


def safe_is_dir_fs(fs: FileSystem, path: str) -> bool:
    """Return `fs.isdir(path)`, treating permission errors as not a directory."""
    try:
        return fs.isdir(path)
    except OSError as exc:
        if _is_permission_error(exc):
            _warn_permission(path, exc)
            return False
        raise


def safe_is_file_fs(fs: FileSystem, path: str) -> bool:
    """Return `fs.isfile(path)`, treating permission errors as not a file."""
    try:
        return fs.isfile(path)
    except OSError as exc:
        if _is_permission_error(exc):
            _warn_permission(path, exc)
            return False
        raise


def safe_glob_fs(fs: FileSystem, pattern: str) -> list[str]:
    """`fs.glob(pattern)` returning [] with a warning on EACCES."""
    try:
        return fs.glob(pattern)
    except OSError as exc:
        if _is_permission_error(exc):
            _warn_permission(pattern, exc)
            return []
        raise


def _scandir_walk_local(root: Path, recursive: bool) -> Iterator[os.DirEntry[str]]:
    """Yield a ``DirEntry`` for every file under ``root``, pruning always-excluded
    dirs and skipping unreadable subtrees with a warning.

    ``scandir`` classifies entries from the single directory read (no per-entry
    ``stat``) and carries each file's metadata, so the caller reads its mtime without
    a second lookup — folded into ``readdir`` on network mounts (NFS/SMB). Directory
    classification and recursion mirror ``os.walk(followlinks=False)``: a symlinked
    directory is not descended, and a symlink to a file is yielded like any file.
    """
    try:
        scandir_it = os.scandir(root)
    except OSError as exc:
        if _is_permission_error(exc):
            _warn_permission(root, exc)
            return
        raise
    with scandir_it:
        entries = list(scandir_it)
    for entry in entries:
        try:
            is_dir = entry.is_dir()
        except OSError as exc:
            if _is_permission_error(exc):
                _warn_permission(entry.path, exc)
                continue
            raise
        if is_dir:
            if (
                recursive
                and entry.name not in DEFAULT_EXCLUDE_DIRS
                and not entry.is_symlink()
            ):
                yield from _scandir_walk_local(Path(entry.path), recursive)
        else:
            yield entry


def safe_walk_local(root: Path) -> Iterator[Path]:
    """Yield every file under `root`, skipping unreadable subtrees."""
    for entry in _scandir_walk_local(root, recursive=True):
        yield Path(entry.path)


def _fs_entry_kind(info: dict[str, Any]) -> str | None:
    """Classify a listing entry as ``"dir"``/``"file"`` from its type, or ``None``
    when the backend reports a type (e.g. a symlink) that must be resolved by stat."""
    entry_type = str(info.get("type", "")).lower()
    if entry_type in _FS_DIR_TYPES:
        return "dir"
    if entry_type in _FS_FILE_TYPES:
        return "file"
    return None


def _probe_fs_entry_kind(fs: FileSystem, path: str) -> str | None:
    """Resolve an entry the listing left ambiguous (e.g. a symlink) with a stat,
    following it as the isdir/isfile walk did."""
    if safe_is_dir_fs(fs, path):
        return "dir"
    if safe_is_file_fs(fs, path):
        return "file"
    return None


def safe_walk_fs(fs: FileSystem, root: str, recursive: bool) -> Iterator[str]:
    """Yield files under `root`, skipping unreadable and default-excluded dirs.

    Consumes the single per-directory listing (type + mtime already in hand) instead
    of re-probing each entry with isdir/isfile, so a remote walk costs one round-trip
    per directory rather than several per file.
    """
    for entry, info in safe_iterdir_detailed_fs(fs, root):
        kind = _fs_entry_kind(info) or _probe_fs_entry_kind(fs, entry)
        if kind == "dir":
            if PurePosixPath(entry).name in DEFAULT_EXCLUDE_DIRS or not recursive:
                continue
            yield from safe_walk_fs(fs, entry, recursive=True)
        elif kind == "file":
            yield entry


def safe_glob_local(root: Path, pattern: str) -> list[Path]:
    """`root.glob(pattern)` returning partial results with a warning on EACCES."""
    results: list[Path] = []
    try:
        results.extend(root.glob(pattern))
    except OSError as exc:
        if _is_permission_error(exc):
            _warn_permission(root, exc)
            return results
        raise
    return results


def _normalize_scan_pattern(pattern: str) -> str:
    """Normalize user include/exclude patterns to relative POSIX-like paths."""
    normalized = pattern.replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized.lstrip("/")


def _has_glob_magic(pattern: str) -> bool:
    """Return True when a pattern contains glob syntax."""
    return any(char in pattern for char in "*?[")


@lru_cache(maxsize=8192)
def _match_segments(
    pattern_parts: tuple[str, ...], path_parts: tuple[str, ...]
) -> bool:
    """Match POSIX path segments where `**` spans zero or more segments."""
    if not pattern_parts:
        return not path_parts
    head = pattern_parts[0]
    tail = pattern_parts[1:]
    if head == "**":
        return _match_segments(tail, path_parts) or bool(
            path_parts and _match_segments(pattern_parts, path_parts[1:])
        )
    return bool(
        path_parts
        and fnmatch.fnmatchcase(path_parts[0], head)
        and _match_segments(tail, path_parts[1:])
    )


def _match_file_pattern(rel_path: str, pattern: str) -> bool:
    """Match a non-directory pattern against a normalized relative file path."""
    if "/" not in pattern and _has_glob_magic(pattern):
        return fnmatch.fnmatchcase(PurePosixPath(rel_path).name, pattern)
    return _match_segments(tuple(pattern.split("/")), tuple(rel_path.split("/")))


def _match_dir_pattern(rel_path: str, pattern: str) -> bool:
    """Match a directory pattern against a file's containing directories."""
    dir_pattern = pattern.rstrip("/")
    pattern_parts = tuple(dir_pattern.split("/"))
    parent_parts = tuple(PurePosixPath(rel_path).parent.parts)
    for depth in range(1, len(parent_parts) + 1):
        if _match_segments(pattern_parts, parent_parts[:depth]):
            return True
    return False


def _match_scan_pattern(rel_path: str, pattern: str) -> bool:
    """Match a user pattern against a normalized relative file path."""
    normalized = _normalize_scan_pattern(pattern)
    if not normalized:
        return False
    if normalized.endswith("/"):
        return _match_dir_pattern(rel_path, normalized)
    return _match_file_pattern(rel_path, normalized)


def _matches_any_scan_pattern(rel_path: str, patterns: Sequence[str] | None) -> bool:
    """Return True when any pattern matches a normalized relative path."""
    return bool(patterns) and any(_match_scan_pattern(rel_path, p) for p in patterns)


def _relative_local_path(root: Path, path: Path) -> str:
    """Return a normalized POSIX relative path for a local file."""
    return path.relative_to(root).as_posix()


def _relative_fs_path(fs: FileSystem, path: str) -> str:
    """Return a normalized POSIX relative path for a filesystem file."""
    return fs.relative_to_root(path).replace("\\", "/").lstrip("/")


def _remote_mtime(fs: FileSystem, path: FsPath) -> float | None:
    """Modification time (epoch seconds) of a remote path, or None when the backend
    exposes none.

    fsspec surfaces it inconsistently: a float/int or a ``datetime`` under ``mtime`` /
    ``modified`` (local-like backends, S3, SFTP), or only the raw ``Last-Modified`` HTTP
    header on a plain web server. A dynamic HTTP endpoint that sends no such header (nor
    a malformed one) has no reliable modification time at all.
    """
    info = fs.info(str(path))
    raw = info.get("mtime") or info.get("modified")
    if raw is None:
        header = next(
            (v for k, v in info.items() if k.lower() == "last-modified"), None
        )
        if not header:
            return None
        try:
            raw = parsedate_to_datetime(header)
        except (TypeError, ValueError):
            return None
    if isinstance(raw, datetime):
        return raw.timestamp()
    return float(raw)


def get_mtime_iso(path: FsPath, fs: FileSystem | None = None) -> str | None:
    """Get file modification time as a UTC date-time string (None if unknown)."""
    if fs is not None:
        mtime = _remote_mtime(fs, path)
        if mtime is None:
            return None
    else:
        assert isinstance(path, Path)
        mtime = path.stat().st_mtime
    return timestamp_to_iso(mtime)


def get_mtime_timestamp(path: FsPath, fs: FileSystem | None = None) -> int:
    """Get file modification time as a Unix timestamp, or 0 when unknown."""
    if fs is not None:
        mtime = _remote_mtime(fs, path)
        if mtime is None:
            return 0
    else:
        assert isinstance(path, Path)
        mtime = path.stat().st_mtime
    return int(mtime)


def get_data_size(path: FsPath, fs: FileSystem | None = None) -> int | None:
    """Get file size in bytes, or None when the backend reports no size (e.g. an HTTP
    endpoint sending no Content-Length) — None means "unknown", not an empty file."""
    if fs is not None:
        size = fs.info(str(path)).get("size")
        return int(size) if size is not None else None
    assert isinstance(path, Path)
    return path.stat().st_size


def get_content_signature(path: FsPath, fs: FileSystem | None = None) -> str | None:
    """A content signature for incremental skip when no reliable mtime exists: the
    backend's ETag (HTTP/S3 expose it in ``info()``). None for local paths or backends
    without one. Reuses the memoized ``info()``, so it adds no request."""
    if fs is None:
        return None
    info = fs.info(str(path))
    etag = next((v for k, v in info.items() if k.lower() == "etag"), None)
    return str(etag) if etag is not None else None


def get_dir_data_size(path: PurePath, fs: FileSystem | None = None) -> int:
    """Get total size of parquet files in a directory tree."""
    if fs is not None:
        path_str = str(path)
        files = safe_glob_fs(fs, f"{path_str}/**/*.parquet") + safe_glob_fs(
            fs, f"{path_str}/**/*.pq"
        )
        return sum(int(fs.info(f).get("size", 0)) for f in files)
    assert isinstance(path, Path)
    files = safe_glob_local(path, "**/*.parquet") + safe_glob_local(path, "**/*.pq")
    return sum(f.stat().st_size for f in files)


def find_files(
    root: PurePath,
    include: Sequence[str] | None,
    exclude: Sequence[str] | None,
    recursive: bool,
    fs: FileSystem | None = None,
) -> list[PurePath]:
    """Find files matching include/exclude patterns."""
    return [
        path
        for path, _mtime in find_files_with_mtime(
            root, include, exclude, recursive, fs=fs
        )
    ]


def find_files_with_mtime(
    root: PurePath,
    include: Sequence[str] | None,
    exclude: Sequence[str] | None,
    recursive: bool,
    fs: FileSystem | None = None,
) -> list[tuple[PurePath, int]]:
    """Find supported files, each paired with its mtime captured during the same
    walk — so an incremental run needs no extra ``stat``/``info`` per file. Locally
    the mtime comes from the ``scandir`` ``DirEntry``; remotely it is served from the
    listing the walk already primed into the info cache."""
    # Normalize str → [str] to avoid iterating over characters
    if isinstance(include, str):
        include = [include]
    if isinstance(exclude, str):
        exclude = [exclude]
    # Use FileSystem if provided, otherwise use pathlib directly
    if fs is not None:
        return _find_files_with_fs(fs, root, include, exclude, recursive)
    assert isinstance(root, Path)

    result: list[tuple[PurePath, int]] = []
    for entry in _scandir_walk_local(root, recursive):
        name = entry.name
        # .zip archives and .gpkg containers are admitted alongside natively
        # supported formats; both are classified by content at scan time.
        if (
            supported_format_for(name) is None
            and not is_zip(name)
            and not is_geopackage(name)
        ):
            continue
        if name.startswith(DEFAULT_EXCLUDE_PREFIXES):
            continue
        # Excluded directories are pruned by the walk, so no parts check is needed.
        path = Path(entry.path)
        rel_path = _relative_local_path(root, path)
        if include is not None and not _matches_any_scan_pattern(rel_path, include):
            continue
        if exclude and _matches_any_scan_pattern(rel_path, exclude):
            continue
        result.append((path, int(entry.stat().st_mtime)))
    return result


def _find_files_with_fs(
    fs: FileSystem,
    root: PurePath,
    include: Sequence[str] | None,
    exclude: Sequence[str] | None,
    recursive: bool,
) -> list[tuple[PurePath, int]]:
    """Find files using FileSystem abstraction (for remote storage support)."""
    root_str = root.as_posix()

    candidates = [
        p
        for p in safe_walk_fs(fs, root_str, recursive)
        # .zip archives and .gpkg containers are admitted alongside natively
        # supported formats; both are classified by content at scan time.
        if supported_format_for(PurePosixPath(p).name) is not None
        or is_zip(PurePosixPath(p).name)
        or is_geopackage(PurePosixPath(p).name)
    ]

    # Apply default exclusions (excluded directories are pruned by the walk).
    candidates = [
        p
        for p in candidates
        if not PurePosixPath(p).name.startswith(DEFAULT_EXCLUDE_PREFIXES)
    ]

    if include is not None:
        candidates = [
            p
            for p in candidates
            if _matches_any_scan_pattern(_relative_fs_path(fs, p), include)
        ]

    if exclude:
        candidates = [
            p
            for p in candidates
            if not _matches_any_scan_pattern(_relative_fs_path(fs, p), exclude)
        ]

    # Use PurePosixPath to preserve forward slashes for remote paths. The mtime is a
    # cache hit: safe_walk_fs already primed the info cache from the directory listing.
    return [
        (path, get_mtime_timestamp(path, fs=fs))
        for path in sorted(PurePosixPath(p) for p in candidates)
    ]


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

# Mapping from Unknown raw_type to our type strings
_UNKNOWN_RAW_TYPE_MAP: dict[str, str] = {
    "double": "float",
    "udouble": "float",
    "float": "float",
    "tinyint": "integer",
    "utinyint": "integer",
    "smallint": "integer",
    "usmallint": "integer",
    "mediumint": "integer",
    "umediumint": "integer",
    "int": "integer",
    "uint": "integer",
    "bigint": "integer",
    "ubigint": "integer",
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
    if isinstance(dtype, dt.Binary):
        return "binary"
    if isinstance(dtype, dt.Null):
        return "null"
    if isinstance(dtype, dt.Unknown):
        raw = str(dtype.raw_type).split("(")[0].lower()
        if raw in _GEOMETRY_KEYWORDS:
            return "geometry"
        if raw in _UNKNOWN_RAW_TYPE_MAP:
            return _UNKNOWN_RAW_TYPE_MAP[raw]
    return "unknown"


def _cast_float(expr: Any, *, use_multiply: bool) -> Any:
    """Cast expression to float64, using multiplication for old MySQL compat."""
    return expr * 1.0 if use_multiply else expr.cast("float64")


# Column types supporting min/max/mean/std
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


def _skip_stat_columns(
    schema: ibis.Schema, skip_stats_columns: set[str] | None
) -> set[str]:
    """Columns that can't be aggregated or cast to string (Binary for BLOB,
    GeoSpatial for GEOMETRY, Unknown raw types like POINT/POLYGON with no known
    mapping)."""
    skip_cols = set(skip_stats_columns) if skip_stats_columns else set()
    for col_name, col_type in schema.items():
        if isinstance(col_type, (dt.Binary, dt.GeoSpatial)):
            skip_cols.add(col_name)
        elif isinstance(col_type, dt.Unknown):
            raw = str(col_type.raw_type).split("(")[0].lower()
            if raw not in _UNKNOWN_RAW_TYPE_MAP:
                skip_cols.add(col_name)
    return skip_cols


def _extra_stat_kinds(schema: ibis.Schema, cols_for_stats: list[str]) -> dict[str, str]:
    """Columns supporting min/max/mean/std: col -> "numeric" | "string" | "date"."""
    kinds: dict[str, str] = {}
    for col in cols_for_stats:
        col_type = schema[col]
        if isinstance(col_type, _NUMERIC_TYPES):
            kinds[col] = "numeric"
        elif isinstance(col_type, dt.String):
            kinds[col] = "string"
        elif isinstance(col_type, _DATE_TYPES):
            kinds[col] = "date"
    return kinds


def _extra_stat_expr(
    source: ibis.Table, col: str, kind: str | None, *, use_multiply: bool
) -> Any:
    """The float expression aggregated for min/max/mean/std, or None when the
    column kind has no such stats."""
    if kind == "numeric":
        return _cast_float(source[col], use_multiply=use_multiply)
    if kind == "string":
        str_col: Any = source[col]
        return _cast_float(str_col.length(), use_multiply=use_multiply)
    if kind == "date":
        date_col: Any = source[col]
        return _cast_float(date_col.epoch_seconds(), use_multiply=use_multiply)
    return None


def _stat_agg_exprs(
    streaming_source: ibis.Table,
    table: ibis.Table,
    cols_for_stats: list[str],
    kinds: dict[str, str],
    *,
    streaming_nb_rows: int,
    sampling: bool,
) -> tuple[list[Any], list[Any]]:
    """Build (streaming_aggs, cardinality_aggs) for the stats query.

    Streaming aggregates (count, min, max, mean, std) run on the streaming
    source. Cardinality (nunique): when sampling, approx on the full data
    (HyperLogLog, streaming, appended to streaming_aggs); otherwise exact on
    the in-memory table."""
    # MySQL < 8.0.17 doesn't support CAST(... AS DOUBLE)
    try:
        backend_name = streaming_source._find_backend().name
    except Exception:  # pragma: no cover
        backend_name = ""
    mul = backend_name == "mysql"

    streaming_aggs: list[Any] = []
    for col in cols_for_stats:
        streaming_aggs.append(streaming_source[col].count().name(f"{col}__non_null"))
        expr = _extra_stat_expr(streaming_source, col, kinds.get(col), use_multiply=mul)
        if expr is not None:
            streaming_aggs.append(expr.min().name(f"{col}__min"))
            streaming_aggs.append(expr.max().name(f"{col}__max"))
            streaming_aggs.append(expr.mean().name(f"{col}__mean"))
            if streaming_nb_rows > 1:
                streaming_aggs.append(expr.std().name(f"{col}__std"))

    cardinality_aggs: list[Any] = []
    if sampling:
        streaming_aggs.extend(
            streaming_source[col].approx_nunique().name(f"{col}__distinct")
            for col in cols_for_stats
        )
    else:
        cardinality_aggs.extend(
            table[col].nunique().name(f"{col}__distinct") for col in cols_for_stats
        )
    return streaming_aggs, cardinality_aggs


def _execute_stat_aggs(
    streaming_aggs: list[Any],
    cardinality_aggs: list[Any],
    table: ibis.Table,
    streaming_source: ibis.Table,
    *,
    sampling: bool,
    dataset_id: str,
) -> dict[str, Any]:
    """Run the stats aggregation, degrading on numeric overflow."""

    def _execute(aggs: list[Any]) -> dict[str, Any]:
        if sampling:
            # Sampling: streaming aggs + approx_nunique on full_table
            agg_table = streaming_source.aggregate(aggs)
        else:
            # No sampling: single combined query on table
            agg_table = table.aggregate(aggs + cardinality_aggs)
        try:
            return agg_table.to_pyarrow().to_pylist()[0]
        except _duckdb.OutOfRangeException:
            raise
        except Exception:
            # Oracle: Decimal values can't convert via PyArrow
            return dict(agg_table.execute().iloc[0])

    try:
        return _execute(streaming_aggs)
    except _duckdb.OutOfRangeException:
        # One extreme column must not cost the dataset. STDDEV is the only
        # aggregate that raises on overflow (AVG yields inf, nulled by
        # _to_float) — retry without it; the file's std stays empty,
        # everything else survives.
        log_warn(
            f"{dataset_id}: numeric overflow computing the standard "
            f"deviation; std left empty",
            quiet=False,
        )
        return _execute(
            [agg for agg in streaming_aggs if not agg.get_name().endswith("__std")]
        )


def _parse_stat_row(
    streaming_row: dict[str, Any],
    cols_for_stats: list[str],
    cols_with_extra: list[str],
    *,
    nb_rows: int,
    streaming_nb_rows: int,
) -> tuple[
    dict[str, tuple[int, int, int]],
    dict[str, tuple[float | None, float | None, float | None, float | None]],
]:
    """Decode the aggregation row into per-column counts and extra stats."""
    stats: dict[str, tuple[int, int, int]] = {}
    extra_stats: dict[
        str, tuple[float | None, float | None, float | None, float | None]
    ] = {}
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
        raw_std = streaming_row.get(f"{col}__std")
        extra_stats[col] = (
            _to_float(streaming_row[f"{col}__min"]),
            _to_float(streaming_row[f"{col}__max"]),
            _round6(streaming_row[f"{col}__mean"]),
            _round6(raw_std) if nb_distinct > 1 else None,
        )
    return stats, extra_stats


def _compute_stats(
    table: ibis.Table,
    *,
    schema: ibis.Schema,
    columns: list[str],
    skip_cols: set[str],
    nb_rows: int,
    dataset_id: str,
    full_table: ibis.Table | None,
    full_nb_rows: int | None,
) -> tuple[
    dict[str, tuple[int, int, int]],
    dict[str, tuple[float | None, float | None, float | None, float | None]],
    ibis.Table,
]:
    """Compute per-column stats in one aggregation query.

    Returns (stats, extra_stats, table); the returned table has empty strings
    nulled out so the downstream value-level passes share the same
    missing-value semantics.

    When full_table is provided (sampling mode), streaming aggregates (count,
    min, max, mean, std) run on full_table while cardinality runs on the
    sample table."""
    stats: dict[str, tuple[int, int, int]] = {}
    extra_stats: dict[
        str, tuple[float | None, float | None, float | None, float | None]
    ] = {}
    # Exclude columns that don't support aggregation (e.g., CLOB)
    cols_for_stats = [c for c in columns if c not in skip_cols]
    kinds = _extra_stat_kinds(schema, cols_for_stats)

    streaming_source = full_table if full_table is not None else table
    streaming_nb_rows = full_nb_rows if full_nb_rows is not None else nb_rows

    # Treat empty strings as NULL for consistent missing-value semantics
    string_cols = [c for c, k in kinds.items() if k == "string"]
    if string_cols:
        _empty = ibis.literal("")
        table = table.mutate(**{c: table[c].nullif(_empty) for c in string_cols})
        if full_table is not None:
            streaming_source = streaming_source.mutate(
                **{c: streaming_source[c].nullif(_empty) for c in string_cols}
            )
        else:
            streaming_source = table

    streaming_aggs, cardinality_aggs = _stat_agg_exprs(
        streaming_source,
        table,
        cols_for_stats,
        kinds,
        streaming_nb_rows=streaming_nb_rows,
        sampling=full_table is not None,
    )
    if not (streaming_aggs or cardinality_aggs):
        return stats, extra_stats, table

    try:
        streaming_row = _execute_stat_aggs(
            streaming_aggs,
            cardinality_aggs,
            table,
            streaming_source,
            sampling=full_table is not None,
            dataset_id=dataset_id,
        )
        stats, extra_stats = _parse_stat_row(
            streaming_row,
            cols_for_stats,
            list(kinds),
            nb_rows=nb_rows,
            streaming_nb_rows=streaming_nb_rows,
        )
    except Exception as e:
        # Oracle ORA-22849: CLOB columns don't support COUNT DISTINCT;
        # stats stays empty, all stats will be None
        if "ORA-22849" not in str(e):
            raise
    return stats, extra_stats, table


def _materialize_for_freq(
    table: ibis.Table, *, nb_rows: int, dataset_id: str
) -> ibis.Table:
    """Materialize a file/DB-backed table to an in-memory Arrow buffer before
    the per-column value-level passes (autotag, frequency, pattern). Each of
    these phases issues one aggregation per eligible column; on a remote view
    (e.g. ``con.read_csv`` / ``con.read_parquet`` / a database table) this
    would otherwise re-scan the source N times — catastrophic for wide
    datasets. Bounded by ``_MATERIALIZE_MAX_ROWS`` to keep RAM usage
    predictable."""
    from ibis.expr.operations import InMemoryTable, PhysicalTable

    physical = list(table.op().find(PhysicalTable))
    is_remote = bool(physical) and not all(
        isinstance(p, InMemoryTable) for p in physical
    )
    if is_remote and nb_rows <= _MATERIALIZE_MAX_ROWS:
        with suppress(Exception):  # on failure, fall back to the remote table
            table = ibis.memtable(_table_to_arrow(table))
    elif is_remote:
        # Above the materialization cap on a remote source: per-column
        # passes will re-scan the source once per eligible column. Surface
        # a warning so users can configure ``sample_size`` if desired.
        log_warn(
            f"{dataset_id}: {nb_rows} rows exceeds the in-memory frequency "
            f"materialization cap ({_MATERIALIZE_MAX_ROWS}); per-column "
            f"frequency passes will re-scan the source. Configure "
            f"sample_size to bound this cost.",
            quiet=False,
        )
    return table


def _auto_tag_string_columns(
    table: ibis.Table, schema: ibis.Schema, columns: list[str]
) -> tuple[dict[str, str], set[str]]:
    """Auto-tag string columns; returns (col -> tag_id, security-tagged cols)."""
    string_cols = [col for col in columns if ibis_type_to_str(schema[col]) == "string"]
    if not string_cols:
        return {}, set()
    from .autotag import _SECURITY_TAGS, compute_auto_tags

    auto_tag_map = compute_auto_tags(table, string_cols)
    security_cols = {col for col, tag in auto_tag_map.items() if tag in _SECURITY_TAGS}
    return auto_tag_map, security_cols


def _value_freq_table(
    table: ibis.Table,
    stats: dict[str, tuple[int, int, int]],
    *,
    freq_threshold: int,
    security_cols: set[str],
) -> pa.Table | None:
    """Value-frequency table for low-cardinality, non-security columns."""
    eligible_cols = [
        col
        for col, (nb_distinct, _, _) in stats.items()
        if 0 <= nb_distinct <= freq_threshold and col not in security_cols
    ]
    if not eligible_cols:
        return None
    # Compute value counts via PyArrow directly on the in-memory Arrow
    # buffer. The previous Ibis-union path issued one DuckDB query per
    # column even when the table was already an in-memory memtable —
    # gratuitous SQL round-trips. PyArrow's vectorised value_counts on
    # the same buffer is ~25× faster on wide datasets.
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.types as pat

    arrow_buf = _table_to_arrow(table.select(eligible_cols))
    parts: list[pa.Table] = []
    for col in eligible_cols:
        arr = arrow_buf.column(col).combine_chunks().drop_null()
        if len(arr) == 0:
            continue
        try:
            vc = arr.value_counts()
        except (pa.ArrowNotImplementedError, pa.ArrowInvalid):
            # Skip types value_counts can't hash (nested, etc.).
            continue
        raw_values = vc.field("values")
        values = raw_values.cast(pa.string())
        if pat.is_timestamp(raw_values.type) or pat.is_time(raw_values.type):
            # PyArrow always renders timestamps/times with their full
            # sub-second precision (``...000000`` or ``...000000000``);
            # trim trailing zero fractional parts for a cleaner UI.
            values = pc.replace_substring_regex(values, r"\.0+$", "")  # pyright: ignore[reportAttributeAccessIssue]
        counts = vc.field("counts")
        n = len(values)
        parts.append(
            pa.table(
                {
                    "variable_id": pa.array([col] * n, type=pa.string()),
                    "value": values,
                    "frequency": counts,
                }
            )
        )
    return pa.concat_tables(parts, promote_options="default") if parts else None


def _pattern_freq_table(
    table: ibis.Table,
    schema: ibis.Schema,
    stats: dict[str, tuple[int, int, int]],
    *,
    freq_threshold: int,
    security_cols: set[str],
) -> tuple[pa.Table | None, dict[str, str]]:
    """Pattern frequency for high-cardinality string columns and
    security-tagged columns; returns (freq table, col -> pattern tag_id)."""
    pattern_cols = [
        col
        for col, (nb_distinct, _, _) in stats.items()
        if ibis_type_to_str(schema[col]) == "string"
        and nb_distinct > 0
        and (
            (freq_threshold > 0 and nb_distinct > freq_threshold)
            or col in security_cols
        )
    ]
    if not pattern_cols:
        return None, {}
    from .pattern import compute_pattern_freqs

    pattern_freq_table, pattern_info = compute_pattern_freqs(table, pattern_cols)
    # pattern_cols only contains cols with nb_distinct > 0, so always non-None
    assert pattern_freq_table is not None
    return pattern_freq_table, pattern_info


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
    schema = full_table.schema() if full_table is not None else table.schema()
    columns = [c for c in schema if c.strip() != ""]
    skip_cols = _skip_stat_columns(schema, skip_stats_columns)

    # Compute stats only if needed
    stats: dict[str, tuple[int, int, int]] = {}
    extra_stats: dict[
        str, tuple[float | None, float | None, float | None, float | None]
    ] = {}
    if infer_stats and nb_rows > 0:
        stats, extra_stats, table = _compute_stats(
            table,
            schema=schema,
            columns=columns,
            skip_cols=skip_cols,
            nb_rows=nb_rows,
            dataset_id=dataset_id,
            full_table=full_table,
            full_nb_rows=full_nb_rows,
        )

    # Materialize before the per-column value-level passes when that pays off
    if freq_threshold is not None and nb_rows > 0:
        table = _materialize_for_freq(table, nb_rows=nb_rows, dataset_id=dataset_id)

    # Auto-tag string columns BEFORE frequency (security tags suppress raw frequency values)
    auto_tag_map: dict[str, str] = {}
    security_cols: set[str] = set()
    if freq_threshold is not None:
        auto_tag_map, security_cols = _auto_tag_string_columns(table, schema, columns)

    # Compute frequency if threshold is set
    freq_table: pa.Table | None = None
    pattern_info: dict[str, str] = {}
    if freq_threshold is not None and stats:
        freq_table = _value_freq_table(
            table, stats, freq_threshold=freq_threshold, security_cols=security_cols
        )
        # Pattern frequency for high-cardinality string columns + security-tagged columns
        pattern_freq, pattern_info = _pattern_freq_table(
            table,
            schema,
            stats,
            freq_threshold=freq_threshold,
            security_cols=security_cols,
        )
        if pattern_freq is not None:
            import pyarrow as pa

            freq_table = (
                pa.concat_tables([freq_table, pattern_freq])
                if freq_table is not None
                else pattern_freq
            )

    # Merge pattern classification into auto_tag_map (for cols without a specific tag)
    for col, tag_id in pattern_info.items():
        auto_tag_map.setdefault(col, tag_id)

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
            is_pattern=True if col_name in pattern_info else None,
            tag_ids=[auto_tag_map[col_name]] if col_name in auto_tag_map else [],
        )
        for col_name in columns
    ]

    return variables, freq_table


def build_variables_from_schema(
    schema: pa.Schema,
    dataset_id: str,
) -> list[Variable]:
    """Build Variable entities from PyArrow schema (no stats, no data read)."""
    ibis_schema = ibis.Schema.from_pyarrow(schema)
    return [
        Variable(
            id=name,
            name=name,
            dataset_id=dataset_id,
            type=ibis_type_to_str(dtype),
        )
        for name, dtype in ibis_schema.items()
        if name.strip() != ""
    ]
