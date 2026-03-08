"""Parquet dataset discovery logic."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Sequence

from ..utils import find_files

if TYPE_CHECKING:
    from ..filesystem import FileSystem


class DatasetType(Enum):
    """Type of Parquet dataset."""

    SIMPLE = "parquet"  # Single file
    DELTA = "delta"  # Delta Lake table
    HIVE = "hive"  # Hive-partitioned dataset
    ICEBERG = "iceberg"  # Apache Iceberg table


@dataclass
class ParquetDatasetInfo:
    """Information about a discovered Parquet dataset."""

    path: Path  # Root path (file for SIMPLE, directory for DELTA/HIVE)
    type: DatasetType
    files: list[Path] = field(default_factory=list)  # All parquet files


@dataclass
class DiscoveryResult:
    """Result of Parquet dataset discovery."""

    datasets: list[ParquetDatasetInfo]
    excluded_dirs: set[Path]  # Directories that are datasets, not folders


# Pattern for Hive partition directories: key=value
_HIVE_PARTITION_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*=.+$")


def is_delta_table(path: Path | str, fs: FileSystem | None = None) -> bool:
    """Check if a directory is a Delta Lake table."""
    if fs is not None:
        path_str = str(path)
        delta_log = f"{path_str}/_delta_log"
        return fs.isdir(delta_log) and bool(fs.glob(f"{delta_log}/*.json"))
    # Local path fallback
    path_obj = Path(path) if isinstance(path, str) else path
    delta_log_path = path_obj / "_delta_log"
    if not delta_log_path.is_dir():
        return False
    return any(delta_log_path.glob("*.json"))


def is_iceberg_table(path: Path | str, fs: FileSystem | None = None) -> bool:
    """Check if a directory is an Apache Iceberg table."""
    if fs is not None:
        path_str = str(path)
        metadata_dir = f"{path_str}/metadata"
        if not fs.isdir(metadata_dir):
            return False
        return bool(fs.glob(f"{metadata_dir}/*.metadata.json")) or bool(
            fs.glob(f"{metadata_dir}/v*.metadata.json")
        )
    # Local path fallback
    path_obj = Path(path) if isinstance(path, str) else path
    metadata_dir_path = path_obj / "metadata"
    if not metadata_dir_path.is_dir():
        return False
    return any(metadata_dir_path.glob("*.metadata.json")) or any(
        metadata_dir_path.glob("v*.metadata.json")
    )


def is_hive_partitioned(path: Path | str, fs: FileSystem | None = None) -> bool:
    """Check if a directory contains Hive-style partitions."""
    if fs is not None:
        path_str = str(path)
        if not fs.isdir(path_str):
            return False
        for child_path in fs.iterdir(path_str):
            child_name = child_path.rsplit("/", 1)[-1]
            if fs.isdir(child_path) and _HIVE_PARTITION_PATTERN.match(child_name):
                if fs.glob(f"{child_path}/**/*.parquet") or fs.glob(
                    f"{child_path}/**/*.pq"
                ):
                    return True
        return False
    # Local path fallback
    path_obj = Path(path) if isinstance(path, str) else path
    if not path_obj.is_dir():
        return False
    for child in path_obj.iterdir():
        if child.is_dir() and _HIVE_PARTITION_PATTERN.match(child.name):
            if list(child.rglob("*.parquet")) or list(child.rglob("*.pq")):
                return True
    return False


def has_hive_partition_in_path(file_path: Path, root: Path) -> Path | None:
    """Check if a file's path contains Hive partitions. Returns the partition root."""
    rel_parts = file_path.relative_to(root).parts

    # Find the first partition directory in the path
    current = root
    for i, part in enumerate(rel_parts[:-1]):  # Exclude the file itself
        current = current / part
        if _HIVE_PARTITION_PATTERN.match(part):
            # Found a partition, return the parent (partition root)
            partition_root = root
            for p in rel_parts[:i]:
                partition_root = partition_root / p
            return partition_root
    return None


def find_parquet_files(
    root: Path,
    include: Sequence[str] | None,
    exclude: Sequence[str] | None,
    recursive: bool,
    fs: FileSystem | None = None,
) -> list[Path]:
    """Find all parquet files matching the patterns."""
    # Get all files, then filter to parquet only
    all_files = find_files(root, include, exclude, recursive, fs=fs)
    return [f for f in all_files if f.suffix.lower() in (".parquet", ".pq")]


def discover_parquet_datasets(
    root: Path,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    recursive: bool = True,
    fs: FileSystem | None = None,
) -> DiscoveryResult:
    """Discover all Parquet datasets in a directory.

    Returns datasets and directories to exclude from folder creation.
    """
    parquet_files = find_parquet_files(root, include, exclude, recursive, fs=fs)

    datasets: list[ParquetDatasetInfo] = []
    excluded_dirs: set[Path] = set()
    processed_files: set[Path] = set()

    # Group files by parent directory for multi-file detection
    files_by_parent: dict[Path, list[Path]] = {}
    for f in parquet_files:
        parent = f.parent
        files_by_parent.setdefault(parent, []).append(f)

    is_remote = fs is not None and not fs.is_local

    def glob_parquet_files(directory: Path) -> list[Path]:
        """Get all parquet files in a directory, using fs.glob for remote."""
        if is_remote and fs is not None:
            pq_files = fs.glob(str(directory / "**/*.parquet"))
            pq_files += fs.glob(str(directory / "**/*.pq"))
            return [Path(f) for f in pq_files]
        return list(directory.rglob("*.parquet")) + list(directory.rglob("*.pq"))

    # First pass: detect Delta Lake tables
    delta_roots: set[Path] = set()
    for parent in files_by_parent:
        if is_delta_table(parent, fs=fs) and parent not in delta_roots:
            delta_roots.add(parent)
            excluded_dirs.add(parent)
            files_in_delta = glob_parquet_files(parent)
            datasets.append(
                ParquetDatasetInfo(
                    path=parent,
                    type=DatasetType.DELTA,
                    files=files_in_delta,
                )
            )
            processed_files.update(files_in_delta)

    # Second pass: detect Iceberg tables
    # Iceberg stores parquet files in a data/ subdirectory, so we check parent paths
    iceberg_roots: set[Path] = set()
    for parent in files_by_parent:
        if any(f in processed_files for f in files_by_parent[parent]):
            continue
        # Check parent and ancestors for Iceberg metadata
        check_path = parent
        while check_path >= root:
            if is_iceberg_table(check_path, fs=fs) and check_path not in iceberg_roots:
                iceberg_roots.add(check_path)
                excluded_dirs.add(check_path)
                files_in_iceberg = glob_parquet_files(check_path)
                datasets.append(
                    ParquetDatasetInfo(
                        path=check_path,
                        type=DatasetType.ICEBERG,
                        files=files_in_iceberg,
                    )
                )
                processed_files.update(files_in_iceberg)
                break
            check_path = check_path.parent

    # Third pass: detect Hive-partitioned datasets
    hive_roots: set[Path] = set()
    for f in parquet_files:
        if f in processed_files:
            continue

        partition_root = has_hive_partition_in_path(f, root)
        if partition_root and partition_root not in hive_roots:
            hive_roots.add(partition_root)
            files_in_hive = glob_parquet_files(partition_root)
            datasets.append(
                ParquetDatasetInfo(
                    path=partition_root,
                    type=DatasetType.HIVE,
                    files=files_in_hive,
                )
            )
            excluded_dirs.add(partition_root)
            processed_files.update(files_in_hive)

    # Fourth pass: simple parquet files
    for f in parquet_files:
        if f in processed_files:
            continue

        datasets.append(
            ParquetDatasetInfo(
                path=f,
                type=DatasetType.SIMPLE,
                files=[f],
            )
        )

    return DiscoveryResult(datasets=datasets, excluded_dirs=excluded_dirs)
