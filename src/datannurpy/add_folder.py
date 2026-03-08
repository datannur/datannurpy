"""Add folder to catalog."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from .utils import (
    build_dataset_id_name,
    build_variable_ids,
    get_folder_id,
    log_done,
    log_section,
    log_skip,
    log_start,
    log_summary,
    log_warn,
    make_id,
    sanitize_id,
    upsert_folder,
)
from .schema import Dataset, Folder
from .scanner.discovery import compute_scan_plan, discover_datasets
from .scanner.utils import get_mtime_iso
from .scanner.parquet.discovery import (
    is_delta_table,
    is_hive_partitioned,
    is_iceberg_table,
)
from .scanner.scan import scan_file

if TYPE_CHECKING:
    from .catalog import Catalog


def add_folder(
    catalog: Catalog,
    path: str | Path,
    folder: Folder | None = None,
    *,
    depth: Literal["structure", "schema", "full"] | None = None,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    recursive: bool = True,
    infer_stats: bool = True,
    csv_encoding: str | None = None,
    quiet: bool | None = None,
    refresh: bool | None = None,
) -> None:
    """Scan a folder and add its contents to the catalog."""
    catalog._has_scanned = True
    q = quiet if quiet is not None else catalog.quiet
    do_refresh = refresh if refresh is not None else catalog.refresh
    resolved_depth = depth if depth is not None else catalog.depth
    root = Path(path).resolve()

    if not root.exists():
        raise FileNotFoundError(f"Folder not found: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"Not a directory: {root}")

    # Reject if path is a dataset (Delta/Hive/Iceberg) - use add_dataset instead
    if is_delta_table(root) or is_hive_partitioned(root) or is_iceberg_table(root):
        raise ValueError(
            f"Path is a dataset, not a folder: {root}. Use add_dataset() instead."
        )

    start_time = log_section("add_folder", str(root), q)
    datasets_before = catalog.dataset.count
    vars_before = catalog.variable.count

    # Create default folder from directory name if not provided
    if folder is None:
        folder = Folder(id=sanitize_id(root.name), name=root.name)

    # Set data_path for root folder
    folder.data_path = str(root)
    folder.last_update_date = get_mtime_iso(root)
    folder.type = "filesystem"

    # Add or update root folder
    upsert_folder(catalog, folder)
    prefix = folder.id

    # Discover all datasets (parquet + other formats)
    discovery = discover_datasets(root, include, exclude, recursive)

    # Extract subdirs from discovered datasets
    subdirs: set[Path] = set()
    for info in discovery.datasets:
        parent = info.path.parent
        while parent != root and parent not in discovery.excluded_dirs:
            if parent not in subdirs:
                subdirs.add(parent)
            parent = parent.parent

    # Create sub-folders
    subdir_ids: dict[Path, str] = {}
    for subdir in sorted(subdirs):
        rel_path = subdir.relative_to(root)
        parts = [sanitize_id(p) for p in rel_path.parts]
        folder_id = make_id(prefix, *parts)

        parent_path = subdir.parent
        parent_id = (
            prefix if parent_path == root else subdir_ids.get(parent_path, prefix)
        )

        upsert_folder(
            catalog,
            Folder(
                id=folder_id,
                name=subdir.name,
                parent_id=parent_id,
                type="filesystem",
                data_path=str(subdir),
                last_update_date=get_mtime_iso(subdir),
            ),
        )
        subdir_ids[subdir] = folder_id

    # Compute scan plan (what to scan vs skip)
    plan = compute_scan_plan(discovery.datasets, catalog, do_refresh)

    # Structure-only mode: create/update datasets without scanning
    if resolved_depth == "structure":
        # Skip unchanged datasets
        for info in plan.to_skip:
            existing = catalog._get_dataset_by_path(str(info.path))
            assert existing is not None
            catalog.dataset.update(existing.id, _seen=True)

        # Create or update modified datasets
        for info in plan.to_scan:
            data_path_str = str(info.path)
            existing = catalog._get_dataset_by_path(data_path_str)
            if existing:
                # Update metadata for modified dataset
                catalog.dataset.update(
                    existing.id,
                    last_update_date=get_mtime_iso(info.path),
                    last_update_timestamp=info.mtime,
                    _seen=True,
                )
                continue

            dataset_id, dataset_name = build_dataset_id_name(info.path, root, prefix)
            folder_id = get_folder_id(info.path, root, prefix, subdir_ids)
            dataset = Dataset(
                id=dataset_id,
                name=dataset_name,
                folder_id=folder_id,
                data_path=data_path_str,
                last_update_date=get_mtime_iso(info.path),
                last_update_timestamp=info.mtime,
                delivery_format=info.format,
                _seen=True,
            )
            catalog.dataset.add(dataset)

        datasets_added = catalog.dataset.count - datasets_before
        log_summary(datasets_added, 0, q, start_time)
        return

    # Handle skipped datasets (mark as seen)
    for info in plan.to_skip:
        existing = catalog._get_dataset_by_path(str(info.path))
        assert existing is not None  # compute_scan_plan guarantees this
        catalog.dataset.update(existing.id, _seen=True)
        catalog._mark_dataset_modalities_seen(existing)
        log_skip(info.path.name, q)

    # Process datasets to scan
    freq_threshold = catalog.freq_threshold if catalog.freq_threshold else None
    resolved_encoding = (
        csv_encoding if csv_encoding is not None else catalog.csv_encoding
    )
    schema_only = resolved_depth == "schema"

    for info in plan.to_scan:
        data_path_str = str(info.path)

        # Remove old dataset if exists (modified or refresh)
        existing = catalog._get_dataset_by_path(data_path_str)
        if existing:
            catalog._remove_dataset_cascade(existing)

        log_start(info.path.name, q)
        dataset_id, dataset_name = build_dataset_id_name(info.path, root, prefix)
        folder_id = get_folder_id(info.path, root, prefix, subdir_ids)

        # Scan dataset
        result = scan_file(
            info.path,
            info.format,
            dataset_id=dataset_id,
            schema_only=schema_only,
            infer_stats=infer_stats,
            freq_threshold=freq_threshold,
            csv_encoding=resolved_encoding,
        )

        # Create dataset
        dataset = Dataset(
            id=dataset_id,
            name=result.name or dataset_name,
            folder_id=folder_id,
            data_path=data_path_str,
            last_update_date=get_mtime_iso(info.path),
            last_update_timestamp=info.mtime,
            delivery_format=info.format,
            description=result.description,
            nb_row=result.nb_row,
            _seen=True,
        )
        catalog.dataset.add(dataset)

        var_id_mapping = build_variable_ids(result.variables, dataset.id)
        if not schema_only:
            catalog.modality_manager.assign_from_freq(
                result.variables, result.freq_table, var_id_mapping
            )
        catalog._add_variables(result.variables, dataset.id)

        # Log result
        if schema_only:
            log_done(f"{info.path.name} ({len(result.variables)} vars)", q)
        elif result.nb_row and result.nb_row > 0:
            log_done(
                f"{info.path.name} ({result.nb_row:,} rows, {len(result.variables)} vars)",
                q,
            )
        else:
            log_warn(f"{info.path.name}: empty file", q)

    datasets_added = catalog.dataset.count - datasets_before
    vars_added = catalog.variable.count - vars_before
    log_summary(datasets_added, vars_added, q, start_time)
