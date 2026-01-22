"""Add folder to catalog."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

from ._ids import (
    build_dataset_id_name,
    get_folder_id,
    make_id,
    sanitize_id,
)
from .entities import Dataset, Folder
from .readers._utils import (
    SUPPORTED_FORMATS,
    find_files,
    find_subdirs,
    get_mtime_iso,
)
from .readers.parquet import (
    discover_parquet_datasets,
    scan_parquet_dataset,
)

if TYPE_CHECKING:
    from .catalog import Catalog


def add_folder(
    catalog: Catalog,
    path: str | Path,
    folder: Folder | None = None,
    *,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    recursive: bool = True,
    infer_stats: bool = True,
) -> None:
    """Scan a folder and add its contents to the catalog."""
    root = Path(path).resolve()

    if not root.exists():
        raise FileNotFoundError(f"Folder not found: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"Not a directory: {root}")

    # Create default folder from directory name if not provided
    if folder is None:
        folder = Folder(id=sanitize_id(root.name), name=root.name)

    # Set data_path for root folder
    folder.data_path = str(root)
    folder.last_update_date = get_mtime_iso(root)
    folder.type = "filesystem"

    # Add root folder
    catalog.folders.append(folder)
    prefix = folder.id

    # Discover Parquet datasets (Delta, Hive, simple)
    parquet_result = discover_parquet_datasets(root, include, exclude, recursive)

    # Find all files and subdirectories
    files = find_files(root, include, exclude, recursive)
    subdirs = find_subdirs(root, files)

    # Filter out directories that are Parquet datasets
    subdirs = {
        d
        for d in subdirs
        if not any(
            d == excl or excl in d.parents for excl in parquet_result.excluded_dirs
        )
    }

    # Create sub-folders
    subdir_ids: dict[Path, str] = {}
    for subdir in sorted(subdirs):
        rel_path = subdir.relative_to(root)
        parts = [sanitize_id(p) for p in rel_path.parts]
        folder_id = make_id(prefix, *parts)

        # Find parent
        parent_path = subdir.parent
        if parent_path == root:
            parent_id = prefix
        else:
            parent_id = subdir_ids.get(parent_path, prefix)

        sub_folder = Folder(
            id=folder_id,
            name=subdir.name,
            parent_id=parent_id,
            type="filesystem",
            data_path=str(subdir),
            last_update_date=get_mtime_iso(subdir),
        )
        catalog.folders.append(sub_folder)
        subdir_ids[subdir] = folder_id

    freq_threshold = catalog.freq_threshold if catalog.freq_threshold else None

    # Process Parquet datasets (simple files, Delta tables, Hive partitioned)
    for info in parquet_result.datasets:
        dataset_id, dataset_name = build_dataset_id_name(info.path, root, prefix)
        folder_id = get_folder_id(info.path, root, prefix, subdir_ids)

        # Scan dataset
        variables, nb_row, freq_table, metadata = scan_parquet_dataset(
            info, infer_stats=infer_stats, freq_threshold=freq_threshold
        )

        # Create dataset
        dataset = Dataset(
            id=dataset_id,
            name=metadata.name or dataset_name,
            folder_id=folder_id,
            data_path=str(info.path),
            last_update_date=get_mtime_iso(info.path),
            delivery_format=info.type.value,
            description=metadata.description,
        )
        catalog.datasets.append(dataset)

        dataset.nb_row = nb_row
        catalog._finalize_variables(variables, dataset, freq_table)

    # Process non-Parquet files (CSV, Excel)
    parquet_files = {f for ds in parquet_result.datasets for f in ds.files}

    for file_path in sorted(files):
        # Skip parquet files (already processed)
        if file_path in parquet_files:
            continue

        parent_dir = file_path.parent
        suffix = file_path.suffix.lower()

        # Skip files inside excluded directories
        if any(
            parent_dir == excl or excl in parent_dir.parents
            for excl in parquet_result.excluded_dirs
        ):
            continue

        # Get format info (only CSV/Excel at this point)
        delivery_format = SUPPORTED_FORMATS.get(suffix)
        if delivery_format is None or delivery_format == "parquet":
            continue

        dataset_id, dataset_name = build_dataset_id_name(file_path, root, prefix)
        folder_id = get_folder_id(file_path, root, prefix, subdir_ids)

        # Create dataset
        dataset = Dataset(
            id=dataset_id,
            name=dataset_name,
            folder_id=folder_id,
            data_path=str(file_path),
            last_update_date=get_mtime_iso(file_path),
            delivery_format=delivery_format,
        )
        catalog.datasets.append(dataset)

        catalog._process_file(
            file_path,
            dataset,
            infer_stats=infer_stats,
            freq_threshold=freq_threshold,
        )
