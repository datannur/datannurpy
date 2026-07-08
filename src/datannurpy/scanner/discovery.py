"""Discovery classes and scan planning."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import PurePath
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from .parquet import discover_parquet_datasets
from .timeseries import group_time_series, series_match_normalized_path
from .utils import find_files, get_mtime_timestamp, supported_format_for
from ..utils import iso_to_timestamp

if TYPE_CHECKING:
    from ..catalog import Catalog
    from .filesystem import FileSystem


@dataclass
class DatasetInfo:
    """Information about a discovered dataset."""

    path: PurePath
    format: str  # csv, parquet, delta, hive, iceberg, sas, spss, stata, excel
    mtime: int
    resource_count: int = 1
    series_files: list[tuple[str, PurePath]] | None = None  # [(period, path), ...]
    series_normalized_path: str | None = None  # Refined normalized path from group
    series_id_suffix: str | None = None


@dataclass
class ScanPlan:
    """Plan for which datasets to scan or skip."""

    to_scan: list[DatasetInfo]
    to_skip: list[DatasetInfo]
    existing_by_path: dict[str, Any] = field(default_factory=dict)


@dataclass
class DiscoveryResult:
    """Result of dataset discovery."""

    datasets: list[DatasetInfo]
    excluded_dirs: set[PurePath]


def discover_datasets(
    root: PurePath,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    recursive: bool = True,
    time_series: bool = True,
    fs: FileSystem | None = None,
) -> DiscoveryResult:
    """Discover all datasets (parquet and other formats) in a directory."""
    result: list[DatasetInfo] = []

    # 1. Discover Parquet datasets (Delta, Hive, Iceberg, simple)
    parquet_result = discover_parquet_datasets(root, include, exclude, recursive, fs=fs)
    parquet_files: set[PurePath] = set()

    for pq_info in parquet_result.datasets:
        parquet_files.update(pq_info.files)
        result.append(
            DatasetInfo(
                path=pq_info.path,
                format=pq_info.type.value,
                mtime=get_mtime_timestamp(pq_info.path, fs=fs),
                resource_count=len(pq_info.files),
            )
        )

    # 2. Find non-parquet files (CSV, Excel, statistical)
    all_files = find_files(root, include, exclude, recursive, fs=fs)

    for file_path in all_files:
        if file_path in parquet_files:
            continue

        # Check if file is inside an excluded parquet directory
        # (explicit loop for coverage.py compatibility with Python 3.9)
        in_excluded = False
        for excl in parquet_result.excluded_dirs:
            if file_path.parent == excl or excl in file_path.parent.parents:
                in_excluded = True
                break
        if in_excluded:
            continue

        # find_files already guaranteed support (through any .gz suffix); resolve the
        # logical format so a .csv.gz is catalogued as csv, not skipped on a KeyError.
        fmt = supported_format_for(file_path.name)
        assert fmt is not None  # guaranteed by find_files

        result.append(
            DatasetInfo(
                path=file_path,
                format=fmt,
                mtime=get_mtime_timestamp(file_path, fs=fs),
                resource_count=1,
            )
        )

    # 3. Group files into time series if enabled
    if time_series:
        result = _apply_time_series_grouping(result, root)

    return DiscoveryResult(
        datasets=sorted(result, key=lambda d: d.path),
        excluded_dirs=parquet_result.excluded_dirs,
    )


def _apply_time_series_grouping(
    datasets: list[DatasetInfo],
    root: PurePath,
) -> list[DatasetInfo]:
    """Group single-file datasets into time series where applicable."""
    # Separate groupable files from non-groupable (partitioned datasets)
    groupable: list[tuple[PurePath, int, str]] = []  # (path, mtime, format)
    non_groupable: list[DatasetInfo] = []

    # Group simple file formats (exclude Delta/Iceberg/Hive which have their own partitioning)
    groupable_formats = {"csv", "excel", "sas", "spss", "stata", "parquet"}

    for info in datasets:
        if info.format in groupable_formats:
            groupable.append((info.path, info.mtime, info.format))
        else:
            non_groupable.append(info)

    if not groupable:
        return datasets

    # Group by format first, then by normalized path
    from collections import defaultdict

    by_format: dict[str, list[tuple[PurePath, int]]] = defaultdict(list)
    for path, mtime, fmt in groupable:
        by_format[fmt].append((path, mtime))

    result = list(non_groupable)

    for fmt, files in by_format.items():
        series_groups, singles = group_time_series(files, root)

        # Add time series groups
        for group in series_groups:
            # Use the last file's path as the dataset path
            last_period, last_path = group.files[-1]
            result.append(
                DatasetInfo(
                    path=last_path,
                    format=fmt,
                    mtime=group.max_mtime,
                    resource_count=len(group.files),
                    series_files=group.files,
                    series_normalized_path=group.normalized_path,
                    series_id_suffix=group.id_suffix,
                )
            )

        # Add single files
        for path, mtime in singles:
            result.append(
                DatasetInfo(
                    path=path,
                    format=fmt,
                    mtime=mtime,
                    resource_count=1,
                )
            )

    return result


def compute_scan_plan(
    datasets: list[DatasetInfo],
    catalog: Catalog,
    refresh: bool,
    root: PurePath | None = None,
) -> ScanPlan:
    """Compute which datasets need scanning based on mtime."""
    to_scan: list[DatasetInfo] = []
    to_skip: list[DatasetInfo] = []

    existing_by_path: dict[str, Any] = {}
    for ds in catalog.dataset.all():
        if ds._match_path:
            for key in _match_path_index_keys(ds._match_path):
                existing_by_path[key] = ds

    for info in datasets:
        match_path = str(info.path)
        existing = None
        for key in _match_path_keys(info, root):
            existing = existing_by_path.get(key)
            if existing is not None:
                existing_by_path[match_path] = existing
                break
        if (
            existing is None
            or refresh
            or iso_to_timestamp(existing.last_update_date) != info.mtime
        ):
            to_scan.append(info)
        else:
            to_skip.append(info)

    return ScanPlan(to_scan=to_scan, to_skip=to_skip, existing_by_path=existing_by_path)


def _normalize_match_key(key: str) -> str:
    """Normalize path separators so index and lookup keys compare equal."""
    return key.replace("\\", "/")


def _match_path_index_keys(match_path: str) -> list[str]:
    """Return lookup variants for a persisted runtime match path.

    A URL match path (e.g. ``sftp://host/shared/data/series_[...].csv``) also
    indexes its path-only form, so a discovered series whose runtime key is the
    root-relative/absolute path can find it without reconstructing the URL.
    """
    key = _normalize_match_key(match_path)
    # Skip URL parsing for the common case of plain local/UNC paths.
    if "://" not in match_path:
        return [key]
    parts = urlsplit(match_path)
    if not (parts.scheme and parts.netloc and parts.path):
        return [key]
    return [key, _normalize_match_key(parts.path)]


def _match_path_keys(info: DatasetInfo, root: PurePath | None) -> list[str]:
    """Return persisted and runtime match keys for a discovered dataset.

    For a time series, the persisted ``_match_path`` is the normalized,
    frequency-tagged pattern (e.g. ``series_---PERIOD-Y---.csv``) rather than
    the latest physical file, so that key is included alongside the concrete
    path candidates.
    """
    path = info.path
    keys = [str(path)]
    if root is not None:
        try:
            rel_path = path.relative_to(root).as_posix()
        except ValueError:
            pass
        else:
            keys.append("" if rel_path == "." else rel_path)
    if info.series_normalized_path is not None:
        series_path = info.series_normalized_path
        if info.series_files is not None:
            periods = [period for period, _ in info.series_files]
            series_path = series_match_normalized_path(series_path, periods)
        keys.append(series_path)
        if root is not None:
            keys.append(str(root / series_path))
    return [_normalize_match_key(key) for key in dict.fromkeys(keys)]
