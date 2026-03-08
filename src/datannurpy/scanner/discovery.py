"""Discovery classes and scan planning."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from .parquet import discover_parquet_datasets
from .utils import SUPPORTED_FORMATS, find_files, get_mtime_timestamp

if TYPE_CHECKING:
    from ..catalog import Catalog
    from .filesystem import FileSystem


@dataclass
class DatasetInfo:
    """Information about a discovered dataset."""

    path: Path
    format: str  # csv, parquet, delta, hive, iceberg, sas, spss, stata, excel
    mtime: int


@dataclass
class ScanPlan:
    """Plan for which datasets to scan or skip."""

    to_scan: list[DatasetInfo]
    to_skip: list[DatasetInfo]


@dataclass
class DiscoveryResult:
    """Result of dataset discovery."""

    datasets: list[DatasetInfo]
    excluded_dirs: set[Path]


def discover_datasets(
    root: Path,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    recursive: bool = True,
    fs: FileSystem | None = None,
) -> DiscoveryResult:
    """Discover all datasets (parquet and other formats) in a directory."""
    result: list[DatasetInfo] = []

    # 1. Discover Parquet datasets (Delta, Hive, Iceberg, simple)
    parquet_result = discover_parquet_datasets(root, include, exclude, recursive, fs=fs)
    parquet_files: set[Path] = set()

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

    return DiscoveryResult(
        datasets=sorted(result, key=lambda d: d.path),
        excluded_dirs=parquet_result.excluded_dirs,
    )


def compute_scan_plan(
    datasets: list[DatasetInfo],
    catalog: Catalog,
    refresh: bool,
) -> ScanPlan:
    """Compute which datasets need scanning based on mtime."""
    to_scan: list[DatasetInfo] = []
    to_skip: list[DatasetInfo] = []

    for info in datasets:
        existing = catalog.dataset.get_by("data_path", str(info.path))
        if existing is None or refresh or existing.last_update_timestamp != info.mtime:
            to_scan.append(info)
        else:
            to_skip.append(info)

    return ScanPlan(to_scan=to_scan, to_skip=to_skip)
