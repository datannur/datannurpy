"""Add folder to catalog."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path, PurePath, PurePosixPath
from typing import TYPE_CHECKING, Any, Literal

from .utils import (
    build_dataset_id_name,
    build_variable_ids,
    get_folder_id,
    log_done,
    log_error,
    log_section,
    log_skip,
    log_start,
    log_summary,
    log_warn,
    make_id,
    sanitize_id,
    upsert_folder,
)
from .utils.params import validate_params
from .errors import ConfigError
from .finalize import remove_dataset_cascade
from .schema import Dataset, Folder
from .scanner.discovery import DatasetInfo, compute_scan_plan, discover_datasets
from .scanner.filesystem import FileSystem, is_remote_url
from .scanner.timeseries import (
    build_series_dataset_id,
    build_series_dataset_name,
    compute_variable_periods,
    get_series_folder_parts,
    normalize_path,
)
from .scanner.utils import get_mtime_iso
from .scanner.parquet.discovery import (
    is_delta_table,
    is_hive_partitioned,
    is_iceberg_table,
)
from .scanner.scan import scan_file

if TYPE_CHECKING:
    from .catalog import Catalog


def _build_series_folder_id(normalized: str, prefix: str) -> str:
    """Build folder_id for a time series using non-temporal parent folders."""
    non_temporal_parts = get_series_folder_parts(normalized)
    if non_temporal_parts:
        return make_id(prefix, *[sanitize_id(p) for p in non_temporal_parts])
    return prefix


@validate_params
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
    time_series: bool = True,
    csv_encoding: str | None = None,
    quiet: bool | None = None,
    refresh: bool | None = None,
    storage_options: dict[str, Any] | None = None,
) -> None:
    """Scan a folder and add its contents to the catalog."""
    catalog._has_scanned = True
    q = quiet if quiet is not None else catalog.quiet
    do_refresh = refresh if refresh is not None else catalog.refresh
    resolved_depth = depth if depth is not None else catalog.depth

    # Handle remote URLs vs local paths
    is_remote = is_remote_url(path)
    fs: FileSystem | None = None

    if is_remote or storage_options:
        fs = FileSystem(path, storage_options)
        if not fs.exists(fs.root):
            raise ConfigError(f"Folder not found: {path}")
        if not fs.isdir(fs.root):
            raise ConfigError(f"Not a directory: {path}")
        # Use PurePosixPath to preserve forward slashes on Windows
        root = PurePosixPath(fs.root)
        root_name = fs.root.rstrip("/").rsplit("/", 1)[-1]
    else:
        root = Path(path).resolve()
        root_name = root.name
        if not root.exists():
            raise ConfigError(f"Folder not found: {root}")
        if not root.is_dir():
            raise ConfigError(f"Not a directory: {root}")

    # Reject if path is a dataset (Delta/Hive/Iceberg) - use add_dataset instead
    if (
        is_delta_table(root, fs=fs)
        or is_hive_partitioned(root, fs=fs)
        or is_iceberg_table(root, fs=fs)
    ):
        raise ConfigError(
            f"Path is a dataset, not a folder: {root}. Use add_dataset() instead."
        )

    start_time = log_section("add_folder", str(root), q)
    datasets_before = catalog.dataset.count
    vars_before = catalog.variable.count

    # Create default folder from directory name if not provided
    if folder is None:
        folder = Folder(id=sanitize_id(root_name), name=root_name)

    # Set data_path for root folder
    folder.data_path = str(root)
    folder.type = "filesystem"

    # Add or update root folder
    upsert_folder(catalog, folder)
    prefix = folder.id

    # Discover all datasets (parquet + other formats)
    discovery = discover_datasets(
        root, include, exclude, recursive, time_series=time_series, fs=fs
    )

    # Extract subdirs from discovered datasets (skip time series temporal paths)
    subdirs: set[PurePath] = set()
    for info in discovery.datasets:
        # Determine starting parent for folder traversal
        start_parent: PurePath | None = None
        if info.series_files is not None:
            # For time series: only add non-temporal parent folders
            normalized = normalize_path(info.path, root)
            non_temporal_parts = get_series_folder_parts(normalized)
            if non_temporal_parts:
                start_parent = root / "/".join(non_temporal_parts)
        else:
            start_parent = info.path.parent

        # Add parent folders up to root
        if start_parent is not None:
            parent = start_parent
            while parent != root and parent not in discovery.excluded_dirs:
                if parent not in subdirs:
                    subdirs.add(parent)
                parent = parent.parent

    # Create sub-folders
    subdir_ids: dict[PurePath, str] = {}
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
            ),
        )
        subdir_ids[subdir] = folder_id

    # Compute scan plan (what to scan vs skip)
    plan = compute_scan_plan(discovery.datasets, catalog, do_refresh)

    # Structure-only mode: create/update datasets without scanning
    if resolved_depth == "structure":
        # Skip unchanged datasets
        for info in plan.to_skip:
            existing = catalog.dataset.get_by("data_path", str(info.path))
            assert existing is not None
            catalog.dataset.update(existing.id, _seen=True)

        # Create or update modified datasets
        for info in plan.to_scan:
            data_path_str = str(info.path)
            existing = catalog.dataset.get_by("data_path", data_path_str)
            if existing:
                # Update metadata for modified dataset
                catalog.dataset.update(
                    existing.id,
                    last_update_date=get_mtime_iso(info.path, fs=fs),
                    last_update_timestamp=info.mtime,
                    _seen=True,
                )
                continue

            # Handle time series vs single file
            if info.series_files is not None:
                periods = [period for period, _ in info.series_files]
                normalized = normalize_path(info.path, root)
                dataset_id = build_series_dataset_id(normalized, prefix)
                dataset_name = build_series_dataset_name(normalized, periods)
                nb_files = len(info.series_files)
                start_date = periods[0]
                end_date = periods[-1]
                folder_id = _build_series_folder_id(normalized, prefix)
            else:
                dataset_id, dataset_name = build_dataset_id_name(
                    info.path, root, prefix
                )
                nb_files = None
                start_date = None
                end_date = None
                folder_id = get_folder_id(info.path, root, prefix, subdir_ids)

            dataset = Dataset(
                id=dataset_id,
                name=dataset_name,
                folder_id=folder_id,
                data_path=data_path_str,
                last_update_date=get_mtime_iso(info.path, fs=fs),
                last_update_timestamp=info.mtime,
                delivery_format=info.format,
                nb_files=nb_files,
                start_date=start_date,
                end_date=end_date,
                _seen=True,
            )
            catalog.dataset.add(dataset)

        datasets_added = catalog.dataset.count - datasets_before
        log_summary(datasets_added, 0, q, start_time)
        return

    # Handle skipped datasets (mark as seen)
    for info in plan.to_skip:
        existing = catalog.dataset.get_by("data_path", str(info.path))
        assert existing is not None  # compute_scan_plan guarantees this
        catalog.dataset.update(existing.id, _seen=True)
        catalog.modality_manager.mark_dataset_seen(existing.id)
        log_skip(info.path.name, q)

    # Process datasets to scan
    freq_threshold = catalog.freq_threshold if catalog.freq_threshold else None
    resolved_encoding = (
        csv_encoding if csv_encoding is not None else catalog.csv_encoding
    )
    schema_only = resolved_depth == "schema"

    scan_errors = 0

    for info in plan.to_scan:
        # Time series: special handling
        if info.series_files is not None:
            try:
                _scan_time_series(
                    catalog=catalog,
                    info=info,
                    root=root,
                    prefix=prefix,
                    schema_only=schema_only,
                    infer_stats=infer_stats,
                    freq_threshold=freq_threshold,
                    csv_encoding=resolved_encoding,
                    quiet=q,
                    fs=fs,
                )
            except Exception as exc:
                log_error(info.path.name, exc, q)
                scan_errors += 1
            continue

        # Single file: standard handling
        data_path_str = str(info.path)

        log_start(info.path.name, q)
        dataset_id, dataset_name = build_dataset_id_name(info.path, root, prefix)
        folder_id = get_folder_id(info.path, root, prefix, subdir_ids)

        # Scan dataset
        try:
            result = scan_file(
                info.path,
                info.format,
                dataset_id=dataset_id,
                schema_only=schema_only,
                infer_stats=infer_stats,
                freq_threshold=freq_threshold,
                csv_encoding=resolved_encoding,
                fs=fs,
                quiet=q,
            )
        except Exception as exc:
            log_error(info.path.name, exc, q)
            scan_errors += 1
            continue

        # Remove old dataset only after successful scan
        existing = catalog.dataset.get_by("data_path", data_path_str)
        if existing:
            remove_dataset_cascade(catalog, existing)

        # Create dataset
        dataset = Dataset(
            id=dataset_id,
            name=result.name or dataset_name,
            folder_id=folder_id,
            data_path=data_path_str,
            last_update_date=get_mtime_iso(info.path, fs=fs),
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
        catalog.variable.add_all(result.variables)

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
    log_summary(datasets_added, vars_added, q, start_time, scan_errors)


def _scan_time_series(
    catalog: Catalog,
    info: DatasetInfo,
    root: PurePath,
    prefix: str,
    schema_only: bool,
    infer_stats: bool,
    freq_threshold: int | None,
    csv_encoding: str | None,
    quiet: bool,
    fs: FileSystem | None,
) -> None:
    """Scan a time series dataset (multiple files with temporal pattern)."""
    assert info.series_files is not None
    series_files = info.series_files
    periods = [period for period, _ in series_files]
    first_period = periods[0]
    last_period, last_path = series_files[-1]

    # Build dataset ID using normalized path
    normalized = normalize_path(info.path, root)
    dataset_id = build_series_dataset_id(normalized, prefix)

    # Build dataset name from normalized path
    dataset_name = build_series_dataset_name(normalized, periods)

    # Folder ID: use non-temporal parent folders only
    folder_id = _build_series_folder_id(normalized, prefix)

    # Remove old dataset if exists
    existing = catalog.dataset.get_by("data_path", str(last_path))
    if existing:
        remove_dataset_cascade(catalog, existing)

    log_start(f"{dataset_name} ({len(series_files)} files)", quiet)

    # Step 1: Schema-only scan on all files to get columns per period
    columns_by_period: dict[str, list[str]] = {}
    for period, file_path in series_files:
        schema_result = scan_file(
            file_path,
            info.format,
            dataset_id=dataset_id,
            schema_only=True,  # Always schema-only for older files
            infer_stats=False,
            freq_threshold=None,
            csv_encoding=csv_encoding,
            fs=fs,
            quiet=quiet,
        )
        columns_by_period[period] = [v.name for v in schema_result.variables]

    # Step 2: Compute variable periods (start_date/end_date)
    var_periods = compute_variable_periods(columns_by_period)

    # Step 3: Full scan on the latest file only (unless schema_only mode)
    result = scan_file(
        last_path,
        info.format,
        dataset_id=dataset_id,
        schema_only=schema_only,
        infer_stats=infer_stats,
        freq_threshold=freq_threshold,
        csv_encoding=csv_encoding,
        fs=fs,
        quiet=quiet,
    )

    # Step 4: Create dataset
    dataset = Dataset(
        id=dataset_id,
        name=result.name or dataset_name,
        folder_id=folder_id,
        data_path=str(last_path),
        last_update_date=get_mtime_iso(last_path, fs=fs),
        last_update_timestamp=info.mtime,
        delivery_format=info.format,
        description=result.description,
        nb_row=result.nb_row,
        nb_files=len(series_files),
        start_date=first_period,
        end_date=last_period,
        _seen=True,
    )
    catalog.dataset.add(dataset)

    # Step 5: Build variables with start_date/end_date from var_periods
    # Union all variables from all periods (some may not be in last file)
    all_var_names = set(var_periods.keys())
    vars_in_last = {v.name for v in result.variables}

    # Variables from last file - add start_date/end_date
    for var in result.variables:
        start_date, end_date = var_periods.get(var.name, (None, None))
        var.start_date = start_date
        var.end_date = end_date

    # Variables not in last file (were removed) - create skeleton variables
    removed_vars = all_var_names - vars_in_last
    from .schema import Variable

    for var_name in removed_vars:
        start_date, end_date = var_periods.get(var_name, (None, None))
        var = Variable(
            id="",  # Will be set by build_variable_ids
            name=var_name,
            dataset_id=dataset_id,
            start_date=start_date,
            end_date=end_date,
        )
        result.variables.append(var)

    # Build variable IDs and assign modalities
    var_id_mapping = build_variable_ids(result.variables, dataset.id)
    if not schema_only:
        catalog.modality_manager.assign_from_freq(
            result.variables, result.freq_table, var_id_mapping
        )
    catalog.variable.add_all(result.variables)

    # Log result
    if schema_only:
        log_done(f"{dataset_name} ({len(result.variables)} vars)", quiet)
    elif result.nb_row and result.nb_row > 0:
        log_done(
            f"{dataset_name} ({result.nb_row:,} rows, {len(result.variables)} vars, {len(series_files)} files)",
            quiet,
        )
    else:
        log_warn(f"{dataset_name}: empty file", quiet)
