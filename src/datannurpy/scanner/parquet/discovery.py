"""Parquet dataset discovery logic."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Sequence


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


def is_delta_table(path: Path) -> bool:
    """Check if a directory is a Delta Lake table."""
    delta_log = path / "_delta_log"
    if not delta_log.is_dir():
        return False
    return any(delta_log.glob("*.json"))


def is_iceberg_table(path: Path) -> bool:
    """Check if a directory is an Apache Iceberg table."""
    metadata_dir = path / "metadata"
    if not metadata_dir.is_dir():
        return False
    # Iceberg tables have .json and .avro metadata files
    return any(metadata_dir.glob("*.metadata.json")) or any(
        metadata_dir.glob("v*.metadata.json")
    )


def is_hive_partitioned(path: Path) -> bool:
    """Check if a directory contains Hive-style partitions."""
    if not path.is_dir():
        return False

    for child in path.iterdir():
        if child.is_dir() and _HIVE_PARTITION_PATTERN.match(child.name):
            # Verify there are parquet files inside
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
) -> list[Path]:
    """Find all parquet files matching the patterns."""
    from ..utils import find_files

    # Get all files, then filter to parquet only
    all_files = find_files(root, include, exclude, recursive)
    return [f for f in all_files if f.suffix.lower() in (".parquet", ".pq")]


def discover_parquet_datasets(
    root: Path,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    recursive: bool = True,
) -> DiscoveryResult:
    """Discover all Parquet datasets in a directory.

    Returns datasets and directories to exclude from folder creation.
    """
    parquet_files = find_parquet_files(root, include, exclude, recursive)

    datasets: list[ParquetDatasetInfo] = []
    excluded_dirs: set[Path] = set()
    processed_files: set[Path] = set()

    # Group files by parent directory for multi-file detection
    files_by_parent: dict[Path, list[Path]] = {}
    for f in parquet_files:
        parent = f.parent
        files_by_parent.setdefault(parent, []).append(f)

    # First pass: detect Delta Lake tables
    delta_roots: set[Path] = set()
    for parent in files_by_parent:
        if is_delta_table(parent) and parent not in delta_roots:
            delta_roots.add(parent)
            files_in_delta = list(parent.rglob("*.parquet")) + list(
                parent.rglob("*.pq")
            )
            datasets.append(
                ParquetDatasetInfo(
                    path=parent,
                    type=DatasetType.DELTA,
                    files=files_in_delta,
                )
            )
            excluded_dirs.add(parent)
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
            if is_iceberg_table(check_path) and check_path not in iceberg_roots:
                iceberg_roots.add(check_path)
                files_in_iceberg = list(check_path.rglob("*.parquet")) + list(
                    check_path.rglob("*.pq")
                )
                datasets.append(
                    ParquetDatasetInfo(
                        path=check_path,
                        type=DatasetType.ICEBERG,
                        files=files_in_iceberg,
                    )
                )
                excluded_dirs.add(check_path)
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
            files_in_hive = list(partition_root.rglob("*.parquet")) + list(
                partition_root.rglob("*.pq")
            )
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
