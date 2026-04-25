"""Discovery classes and scan planning."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import PurePath
from typing import TYPE_CHECKING

from .parquet import discover_parquet_datasets
from .timeseries import group_time_series
from .utils import SUPPORTED_FORMATS, find_files, get_mtime_timestamp

if TYPE_CHECKING:
    from ..catalog import Catalog
    from .filesystem import FileSystem


@dataclass
class DatasetInfo:
    """Information about a discovered dataset."""

    path: PurePath
    format: str  # csv, parquet, delta, hive, iceberg, sas, spss, stata, excel
    mtime: int
    series_files: list[tuple[str, PurePath]] | None = None  # [(period, path), ...]
    series_normalized_path: str | None = None  # Refined normalized path from group


@dataclass
class ScanPlan:
    """Plan for which datasets to scan or skip."""

    to_scan: list[DatasetInfo]
    to_skip: list[DatasetInfo]


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

        suffix = file_path.suffix.lower()
        fmt = SUPPORTED_FORMATS.get(suffix)
        if fmt is None:
            continue  # Skip unknown file types (include pattern may match more)

        result.append(
            DatasetInfo(
                path=file_path,
                format=fmt,
                mtime=get_mtime_timestamp(file_path, fs=fs),
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
                    series_files=group.files,
                    series_normalized_path=group.normalized_path,
                )
            )

        # Add single files
        for path, mtime in singles:
            result.append(
                DatasetInfo(
                    path=path,
                    format=fmt,
                    mtime=mtime,
                )
            )

    return result


def compute_scan_plan(
    datasets: list[DatasetInfo],
    catalog: Catalog,
    refresh: bool,
) -> ScanPlan:
    """Compute which datasets need scanning based on mtime."""
    to_scan: list[DatasetInfo] = []
    to_skip: list[DatasetInfo] = []

    for info in datasets:
        existing = catalog.dataset.get_by("_match_path", str(info.path))
        if existing is None or refresh or existing.last_update_timestamp != info.mtime:
            to_scan.append(info)
        else:
            to_skip.append(info)

    return ScanPlan(to_scan=to_scan, to_skip=to_skip)
