"""Add folder to catalog."""

from __future__ import annotations

import stat
from collections.abc import Sequence
from pathlib import Path, PurePath, PurePosixPath
from typing import TYPE_CHECKING, Any, Literal, NoReturn

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
    timestamp_to_iso,
    upsert_folder,
)
from .utils.params import _UNSET, validate_params
from .add_metadata import (
    LoadedDatasetRef,
    find_loaded_dataset_by_match_paths,
)
from .errors import ConfigError
from .finalize import remove_dataset_cascade
from .preview import (
    PreviewRows,
    effective_preview_rows,
    remember_preview,
    resolve_preview_rows,
)
from .schema import Dataset, EntityMetadata, Folder, folder_from_metadata
from .scanner.discovery import DatasetInfo, compute_scan_plan, discover_datasets
from .scanner.filesystem import FileSystem, is_remote_url
from .scanner.timeseries import (
    _build_series_dataset_id_with_suffix,
    build_series_dataset_name,
    compute_variable_periods,
    get_series_folder_parts,
    period_sort_key,
    series_match_normalized_path,
)
from .scanner.utils import (
    fs_info_is_dir,
    get_data_size,
    get_dir_data_size,
)
from .scanner.parquet.discovery import (
    is_delta_table,
    is_hive_partitioned,
    is_iceberg_table,
)
from .scanner.scan import scan_file

if TYPE_CHECKING:
    from .catalog import Catalog, Depth

_DIR_FORMATS = {"delta", "hive", "iceberg"}

OnUnmatched = Literal["skip", "warn", "error"]


def _raise_folder_config_error(message: str, quiet: bool) -> NoReturn:
    """Log a clean root-folder validation error, then raise ``ConfigError``.

    The error is logged before being raised, so ``log_error`` sees no traceback
    and writes a single clean line; ``raise ... from None`` keeps the propagated
    error free of any low-level filesystem/SFTP context.
    """
    error = ConfigError(message)
    log_error("add_folder", error, quiet)
    raise error from None


def _build_series_folder_id(normalized: str, prefix: str) -> str:
    """Build folder_id for a time series using non-temporal parent folders."""
    non_temporal_parts = get_series_folder_parts(normalized)
    if non_temporal_parts:
        return make_id(prefix, *[sanitize_id(p) for p in non_temporal_parts])
    return prefix


def _display_path(path: PurePath, root: PurePath) -> str:
    """Return a log-friendly path relative to the scanned root when possible."""
    try:
        rel_path = path.relative_to(root).as_posix()
    except ValueError:
        return path.name
    return "" if rel_path == "." else rel_path


def _public_data_path(path: PurePath, root: PurePath, fs: FileSystem | None) -> str:
    """Return the exported data_path while keeping local scan paths portable."""
    if fs is not None and not fs.is_local:
        return str(path)
    return _display_path(path, root)


def _match_path_candidates(
    path: PurePath,
    fs: FileSystem | None,
    *,
    series_normalized_path: str | None = None,
    root: PurePath | None = None,
) -> list[str]:
    """Return scan keys for metadata-first matching without filesystem I/O."""
    candidates = [str(path)]
    if series_normalized_path is not None:
        candidates.insert(0, series_normalized_path)
    if fs is not None and not fs.is_local:
        remote_path = fs.canonical_url_for_path(path)
        if remote_path is not None:
            candidates.append(remote_path)
        if series_normalized_path is not None:
            remote_series = fs.canonical_url_for_path(series_normalized_path)
            if remote_series is not None:
                candidates.insert(0, remote_series)
    elif series_normalized_path is not None and root is not None:
        # Local/UNC: add the absolute normalized series candidate so metadata
        # `_match_path` values like `\\SERVER\SHARE\data\series_[YYYY].csv`
        # match, mirroring the canonical remote URL candidate added above.
        candidates.insert(0, str(root / series_normalized_path))
    return list(dict.fromkeys(candidates))


def _display_dataset_path(info: DatasetInfo, root: PurePath) -> str:
    """Return the path label to use in add_folder logs."""
    if info.series_files is not None:
        return _display_path(info.series_files[-1][1], root)
    return _display_path(info.path, root)


def _display_dataset_label(info: DatasetInfo, root: PurePath) -> str:
    """Return the dataset log label used in folder scans."""
    path_label = _display_dataset_path(info, root)
    if info.series_files is None:
        return path_label
    return f"{path_label} ({len(info.series_files)} files)"


def _canonicalize_time_series_columns(
    columns_by_period: dict[str, list[str]],
) -> dict[str, list[str]]:
    """Merge time-series header aliases using the sanitized variable key."""
    if not columns_by_period:
        return columns_by_period

    sorted_periods = sorted(columns_by_period, key=period_sort_key)
    canonical_name_by_key: dict[str, str] = {}

    # Keep the latest label seen for each sanitized key.
    for period in sorted_periods:
        for column_name in columns_by_period[period]:
            canonical_name_by_key[sanitize_id(column_name)] = column_name

    canonical_columns_by_period: dict[str, list[str]] = {}
    for period in sorted_periods:
        canonical_columns: list[str] = []
        seen_columns: set[str] = set()
        for column_name in columns_by_period[period]:
            canonical_name = canonical_name_by_key[sanitize_id(column_name)]
            if canonical_name in seen_columns:
                continue
            canonical_columns.append(canonical_name)
            seen_columns.add(canonical_name)
        canonical_columns_by_period[period] = canonical_columns

    return canonical_columns_by_period


def _handle_unmatched(
    path_label: str,
    on_unmatched: OnUnmatched,
    quiet: bool,
) -> None:
    """Apply the configured policy for a scanned file with no metadata match."""
    if on_unmatched == "error":
        raise ConfigError(
            f"No metadata match for {path_label} (create_folders=False, "
            f'on_unmatched="error")'
        )
    if on_unmatched == "warn":
        log_warn(f"{path_label}: no metadata match, skipped", quiet)


def _resolve_ids_from_peek(
    peek: LoadedDatasetRef | None,
    fallback_id: str,
    fallback_folder_id: str,
    create_folders: bool,
) -> tuple[str, str | None]:
    """Resolve (dataset_id, folder_id) from peek with fallback to scan-derived ids."""
    if peek is None:
        return fallback_id, fallback_folder_id
    folder_id = peek.folder_id
    if folder_id is None and create_folders:
        folder_id = fallback_folder_id
    return peek.id, folder_id


@validate_params
def add_folder(
    catalog: Catalog,
    path: str | Path | Sequence[str | Path],
    metadata: EntityMetadata | None = None,
    *,
    depth: Depth | None = None,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    recursive: bool = True,
    time_series: bool = True,
    csv_encoding: str | None = None,
    sample_size: int | None = _UNSET,
    auto_enumerations: bool | None = None,
    preview_rows: PreviewRows = None,
    csv_skip_copy: bool | None = None,
    quiet: bool | None = None,
    refresh: bool | None = None,
    storage_options: dict[str, Any] | None = None,
    create_folders: bool = True,
    on_unmatched: OnUnmatched = "warn",
) -> None:
    """Scan a folder and add its contents to the catalog."""
    if not create_folders and metadata is not None:
        raise ConfigError(
            "create_folders=False is incompatible with metadata= (no folder is created)"
        )
    if isinstance(path, list):
        kwargs = {k: v for k, v in locals().items() if k not in ("catalog", "path")}
        for p in path:
            add_folder(catalog, p, **kwargs)
        return
    assert not isinstance(path, Sequence) or isinstance(path, (str, Path))

    catalog._has_scanned = True
    if (depth if depth is not None else catalog.depth) == "value":
        from .scanner.autotag import ensure_auto_tags

        ensure_auto_tags(catalog)
    q = quiet if quiet is not None else catalog.quiet
    do_refresh = refresh if refresh is not None else catalog.refresh
    resolved_depth = depth if depth is not None else catalog.depth
    resolved_auto_enumerations = (
        auto_enumerations
        if auto_enumerations is not None
        else catalog.auto_enumerations
    )
    preview_limit = effective_preview_rows(
        resolve_preview_rows(preview_rows, catalog.preview_rows), resolved_depth
    )

    # Handle remote URLs vs local paths
    is_remote = is_remote_url(path)
    fs: FileSystem | None = None

    if is_remote or storage_options:
        fs = FileSystem(path, storage_options)
        try:
            is_dir = fs_info_is_dir(fs, fs.root)
        except FileNotFoundError:
            _raise_folder_config_error(f"Folder not found: {path}", q)
        if not is_dir:
            _raise_folder_config_error(f"Not a directory: {path}", q)
        # Use PurePosixPath to preserve forward slashes on Windows
        root = PurePosixPath(fs.root)
        root_name = fs.root.rstrip("/").rsplit("/", 1)[-1]
    else:
        root = Path(path).resolve()
        root_name = root.name
        try:
            root_stat = root.stat()
        except FileNotFoundError:
            _raise_folder_config_error(f"Folder not found: {root}", q)
        if not stat.S_ISDIR(root_stat.st_mode):
            _raise_folder_config_error(f"Not a directory: {root}", q)

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
    vars_before = catalog.variable.count

    # Discover all datasets (parquet + other formats)
    discovery = discover_datasets(
        root, include, exclude, recursive, time_series=time_series, fs=fs
    )

    if create_folders:
        # Create default folder from directory name if not provided
        if metadata is None:
            folder = Folder(id=sanitize_id(root_name), name=root_name)
        else:
            folder = folder_from_metadata(
                metadata,
                default_id=sanitize_id(root_name),
                default_name=root_name,
            )

        # Set data_path for root folder
        folder.data_path = _public_data_path(root, root, fs)
        folder.type = folder.type or "filesystem"

        # Add or update root folder
        upsert_folder(catalog, folder)
        prefix = folder.id

        # Extract subdirs from discovered datasets (skip time series temporal paths)
        subdirs: set[PurePath] = set()
        for info in discovery.datasets:
            # Determine starting parent for folder traversal
            start_parent: PurePath | None = None
            if info.series_files is not None:
                # For time series: only add non-temporal parent folders
                assert info.series_normalized_path is not None
                non_temporal_parts = get_series_folder_parts(
                    info.series_normalized_path
                )
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
                    data_path=_public_data_path(subdir, root, fs),
                ),
            )
            subdir_ids[subdir] = folder_id
    else:
        # Metadata-first mode: do not create any folder; rely on peek for ids.
        prefix = ""
        subdir_ids = {}

    # Compute scan plan (what to scan vs skip)
    plan = compute_scan_plan(discovery.datasets, catalog, do_refresh, root=root)
    resource_count = sum(info.resource_count for info in discovery.datasets)

    # Structure-only mode: create/update datasets without scanning
    if resolved_depth == "dataset":
        # Skip unchanged datasets (single batch upsert instead of per-dataset update)
        skipped: list[Dataset] = []
        for info in plan.to_skip:
            existing = plan.existing_by_path.get(str(info.path))
            assert existing is not None
            existing._seen = True
            existing.preview_rows = 0
            existing._match_path = str(info.path)
            skipped.append(existing)
            log_skip(_display_dataset_label(info, root), q)
        if skipped:
            catalog.dataset.upsert_all(skipped)

        # Create or update modified datasets. Count only datasets actually
        # touched — an unmatched file (create_folders=False) is skipped, not
        # scanned, so it must not inflate the tally.
        scanned = 0
        for info in plan.to_scan:
            display_label = _display_dataset_label(info, root)
            t0 = log_start(display_label, q)
            data_path_str = str(info.path)
            existing = plan.existing_by_path.get(data_path_str)
            if existing:
                # Update metadata for modified dataset
                data_size = (
                    get_dir_data_size(info.path, fs=fs)
                    if info.format in _DIR_FORMATS
                    else get_data_size(info.path, fs=fs)
                )
                catalog.dataset.update(
                    existing.id,
                    last_update_date=timestamp_to_iso(info.mtime),
                    data_size=data_size,
                    preview_rows=0,
                    _seen=True,
                    _match_path=data_path_str,
                )
                log_done(display_label, q, t0)
                scanned += 1
                continue

            series_normalized_path: str | None = None
            if info.series_files is not None:
                assert info.series_normalized_path is not None
                series_normalized_path = series_match_normalized_path(
                    info.series_normalized_path,
                    [period for period, _ in info.series_files],
                )
            # Metadata-first peek: reuse pre-loaded id/folder_id if available.
            peek = find_loaded_dataset_by_match_paths(
                catalog,
                _match_path_candidates(
                    info.path,
                    fs,
                    series_normalized_path=series_normalized_path,
                    root=root,
                ),
            )
            if peek is None and not create_folders:
                _handle_unmatched(display_label, on_unmatched, q)
                continue

            # Compute name and time-series fields (always needed)
            if info.series_files is not None:
                periods = [period for period, _ in info.series_files]
                assert info.series_normalized_path is not None
                normalized = info.series_normalized_path
                dataset_name = build_series_dataset_name(normalized, periods)
                nb_resources = len(info.series_files)
                start_date = periods[0]
                end_date = periods[-1]
                fallback_id = _build_series_dataset_id_with_suffix(
                    normalized,
                    prefix,
                    info.series_id_suffix,
                )
                fallback_folder_id = _build_series_folder_id(normalized, prefix)
            else:
                fallback_id, dataset_name = build_dataset_id_name(
                    info.path, root, prefix
                )
                nb_resources = None
                start_date = None
                end_date = None
                fallback_folder_id = get_folder_id(info.path, root, prefix, subdir_ids)

            dataset_id, folder_id = _resolve_ids_from_peek(
                peek, fallback_id, fallback_folder_id, create_folders
            )

            dataset = Dataset(
                id=dataset_id,
                name=dataset_name,
                folder_id=folder_id,
                data_path=_public_data_path(info.path, root, fs),
                last_update_date=timestamp_to_iso(info.mtime),
                delivery_format=info.format,
                nb_resources=nb_resources,
                preview_rows=0,
                data_size=(
                    get_dir_data_size(info.path, fs=fs)
                    if info.format in _DIR_FORMATS
                    else get_data_size(info.path, fs=fs)
                ),
                start_date=start_date,
                end_date=end_date,
                _seen=True,
                _match_path=data_path_str,
            )
            catalog.dataset.add(dataset)
            log_done(display_label, q, t0)
            scanned += 1

        unchanged = len(plan.to_skip)
        catalog._tally_scan(scanned, unchanged)
        log_summary(
            scanned,
            None,
            q,
            start_time,
            resource_count=resource_count,
            resource_label="files",
            unchanged=unchanged,
        )
        return

    # Handle skipped datasets (single batch upsert instead of per-dataset update)
    skip_seen_ids: list[str] = []
    skipped: list[Dataset] = []
    for info in plan.to_skip:
        existing = plan.existing_by_path.get(str(info.path))
        assert existing is not None  # compute_scan_plan guarantees this
        existing._seen = True
        existing.preview_rows = preview_limit
        existing._match_path = str(info.path)
        skip_seen_ids.append(existing.id)
        skipped.append(existing)
        log_skip(_display_dataset_label(info, root), q)
    if skipped:
        catalog.dataset.upsert_all(skipped)
        catalog.enumeration_manager.mark_datasets_seen(skip_seen_ids)

    # Process datasets to scan
    schema_only = resolved_depth == "variable"
    freq_threshold = catalog.freq_threshold if resolved_depth == "value" else None
    resolved_encoding = (
        csv_encoding if csv_encoding is not None else catalog.csv_encoding
    )
    resolved_csv_skip_copy = (
        csv_skip_copy if csv_skip_copy is not None else catalog.csv_skip_copy
    )
    resolved_sample_size: int | None
    if resolved_depth == "value":
        resolved_sample_size = (
            sample_size if sample_size is not _UNSET else catalog.sample_size
        )
    else:
        resolved_sample_size = None

    scan_errors = 0
    # Count only datasets actually created/updated. An unmatched file
    # (create_folders=False) is skipped before any scan, so it is neither an
    # error nor a scan and must not inflate the tally.
    scanned = 0

    for info in plan.to_scan:
        display_path = _display_dataset_path(info, root)
        # Time series: special handling
        if info.series_files is not None:
            try:
                if _scan_time_series(
                    catalog=catalog,
                    info=info,
                    root=root,
                    prefix=prefix,
                    schema_only=schema_only,
                    freq_threshold=freq_threshold,
                    csv_encoding=resolved_encoding,
                    sample_size=resolved_sample_size,
                    auto_enumerations=resolved_auto_enumerations,
                    preview_rows=preview_limit,
                    csv_skip_copy=resolved_csv_skip_copy,
                    quiet=q,
                    fs=fs,
                    create_folders=create_folders,
                    on_unmatched=on_unmatched,
                ):
                    scanned += 1
            except Exception as exc:
                log_error(display_path, exc, q)
                scan_errors += 1
            continue

        # Single file: standard handling
        data_path_str = str(info.path)

        t0 = log_start(display_path, q)

        # Metadata-first peek: reuse pre-loaded id/folder_id if available.
        peek = find_loaded_dataset_by_match_paths(
            catalog, _match_path_candidates(info.path, fs)
        )
        if peek is None and not create_folders:
            _handle_unmatched(display_path, on_unmatched, q)
            continue

        fallback_id, dataset_name = build_dataset_id_name(info.path, root, prefix)
        fallback_folder_id = get_folder_id(info.path, root, prefix, subdir_ids)
        dataset_id, folder_id = _resolve_ids_from_peek(
            peek, fallback_id, fallback_folder_id, create_folders
        )

        # Scan dataset
        try:
            result = scan_file(
                info.path,
                info.format,
                dataset_id=dataset_id,
                schema_only=schema_only,
                freq_threshold=freq_threshold,
                csv_encoding=resolved_encoding,
                sample_size=resolved_sample_size,
                preview_rows=preview_limit,
                csv_skip_copy=resolved_csv_skip_copy,
                fs=fs,
                quiet=q,
                path_label=display_path,
            )
        except Exception as exc:
            log_error(display_path, exc, q)
            scan_errors += 1
            continue

        # Remove old dataset only after successful scan
        existing = plan.existing_by_path.get(data_path_str)
        if existing:
            remove_dataset_cascade(catalog, existing)

        # Create dataset
        dataset = Dataset(
            id=dataset_id,
            name=result.name or dataset_name,
            folder_id=folder_id,
            data_path=_public_data_path(info.path, root, fs),
            last_update_date=timestamp_to_iso(info.mtime),
            delivery_format=info.format,
            description=result.description,
            nb_row=result.nb_row,
            sample_size=result.sample_size,
            preview_rows=preview_limit,
            data_size=(
                result.data_size
                if info.format in _DIR_FORMATS
                else get_data_size(info.path, fs=fs)
            ),
            _seen=True,
            _match_path=data_path_str,
        )
        catalog.dataset.add(dataset)
        scanned += 1
        remember_preview(
            catalog,
            dataset.id,
            result.preview,
            label=display_path,
            variables=result.variables,
        )

        var_id_mapping = build_variable_ids(result.variables, dataset.id)
        if result.freq_table is not None:
            catalog.enumeration_manager.assign_from_freq(
                result.variables,
                result.freq_table,
                var_id_mapping,
                auto_enumerations=resolved_auto_enumerations,
            )
        catalog.variable.add_all(result.variables)

        # Log result
        if schema_only:
            log_done(f"{display_path} ({len(result.variables)} vars)", q, t0)
        elif result.nb_row is None:
            # Scanner already emitted a warning explaining why the file
            # could not be scanned (untreatable, malformed, etc.).
            pass
        elif result.nb_row > 0:
            log_done(
                f"{display_path} ({result.nb_row:,} rows, {len(result.variables)} vars)",
                q,
                t0,
            )
        else:
            log_warn(f"{display_path}: empty file", q)

    vars_added = catalog.variable.count - vars_before
    unchanged = len(plan.to_skip)
    catalog._tally_scan(scanned, unchanged, scan_errors)
    log_summary(
        scanned,
        vars_added,
        q,
        start_time,
        scan_errors,
        resource_count=resource_count,
        resource_label="files",
        unchanged=unchanged,
    )


def _scan_time_series(
    catalog: Catalog,
    info: DatasetInfo,
    root: PurePath,
    prefix: str,
    schema_only: bool,
    freq_threshold: int | None,
    csv_encoding: str | None,
    sample_size: int | None,
    auto_enumerations: bool,
    preview_rows: int,
    csv_skip_copy: bool,
    quiet: bool,
    fs: FileSystem | None,
    create_folders: bool,
    on_unmatched: OnUnmatched,
) -> bool:
    """Scan a time series dataset (multiple files with temporal pattern).

    Returns ``True`` when a dataset was created, ``False`` when the series was
    skipped for want of a metadata match (``create_folders=False``).
    """
    assert info.series_files is not None
    assert info.series_normalized_path is not None
    series_files = info.series_files
    display_path = _display_dataset_path(info, root)
    periods = [period for period, _ in series_files]
    first_period = periods[0]
    last_period, last_path = series_files[-1]

    # Build dataset ID using normalized path
    normalized = info.series_normalized_path
    fallback_id = _build_series_dataset_id_with_suffix(
        normalized,
        prefix,
        info.series_id_suffix,
    )
    # Build dataset name from normalized path
    dataset_name = build_series_dataset_name(normalized, periods)

    # Folder ID: use non-temporal parent folders only
    fallback_folder_id = _build_series_folder_id(normalized, prefix)

    # Metadata-first peek: reuse pre-loaded id/folder_id if available.
    # Time series match on the latest period (the canonical data_path).
    peek = find_loaded_dataset_by_match_paths(
        catalog,
        _match_path_candidates(
            last_path,
            fs,
            series_normalized_path=series_match_normalized_path(normalized, periods),
            root=root,
        ),
    )
    if peek is None and not create_folders:
        _handle_unmatched(display_path, on_unmatched, quiet)
        return False

    dataset_id, folder_id = _resolve_ids_from_peek(
        peek, fallback_id, fallback_folder_id, create_folders
    )

    # Remove old dataset if exists. A reloaded series is stored under its
    # normalized metadata `_match_path`, not the latest physical file, so fall
    # back to the resolved dataset id when path-based lookup misses.
    last_match_path = str(last_path)
    existing = catalog.dataset.get_by("_match_path", last_match_path)
    if existing is None:
        existing = catalog.dataset.get_by(
            "_match_path", _public_data_path(last_path, root, fs)
        )
    if existing is None:
        existing = catalog.dataset.get(dataset_id)
    if existing:
        remove_dataset_cascade(catalog, existing)

    t0 = log_start(f"{display_path} ({len(series_files)} files)", quiet)

    # Step 1: Schema-only scan on all files to get columns per period
    columns_by_period: dict[str, list[str]] = {}
    latest_schema_result = None
    for period, file_path in series_files:
        member_path_label = _display_path(file_path, root)
        schema_result = scan_file(
            file_path,
            info.format,
            dataset_id=dataset_id,
            schema_only=True,  # Always schema-only for older files
            freq_threshold=None,
            csv_encoding=csv_encoding,
            fs=fs,
            quiet=quiet,
            path_label=member_path_label,
        )
        columns_by_period[period] = [v.name for v in schema_result.variables]
        if file_path == last_path:
            latest_schema_result = schema_result

    # Step 2: Compute variable periods (start_date/end_date)
    var_periods = compute_variable_periods(
        _canonicalize_time_series_columns(columns_by_period)
    )

    # Step 3: Full scan on the latest file only (reuse schema scan in schema_only mode)
    if schema_only:
        assert latest_schema_result is not None
        result = latest_schema_result
    else:
        result = scan_file(
            last_path,
            info.format,
            dataset_id=dataset_id,
            schema_only=schema_only,
            freq_threshold=freq_threshold,
            csv_encoding=csv_encoding,
            sample_size=sample_size,
            preview_rows=preview_rows,
            csv_skip_copy=csv_skip_copy,
            fs=fs,
            quiet=quiet,
            path_label=_display_path(last_path, root),
        )

    # Step 4: Create dataset
    dataset = Dataset(
        id=dataset_id,
        name=result.name or dataset_name,
        folder_id=folder_id,
        data_path=_public_data_path(last_path, root, fs),
        last_update_date=timestamp_to_iso(info.mtime),
        delivery_format=info.format,
        description=result.description,
        nb_row=result.nb_row,
        sample_size=result.sample_size,
        preview_rows=preview_rows,
        nb_resources=len(series_files),
        data_size=get_data_size(last_path, fs=fs),
        start_date=first_period,
        end_date=last_period,
        _seen=True,
        _match_path=last_match_path,
    )
    catalog.dataset.add(dataset)
    remember_preview(
        catalog,
        dataset.id,
        result.preview,
        label=display_path,
        variables=result.variables,
    )

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

    # Build variable IDs and assign enumerations
    var_id_mapping = build_variable_ids(result.variables, dataset.id)
    if result.freq_table is not None:
        catalog.enumeration_manager.assign_from_freq(
            result.variables,
            result.freq_table,
            var_id_mapping,
            auto_enumerations=auto_enumerations,
        )
    catalog.variable.add_all(result.variables)

    # Log result
    if schema_only:
        log_done(
            f"{display_path} ({len(result.variables)} vars, {len(series_files)} files)",
            quiet,
            t0,
        )
    elif result.nb_row and result.nb_row > 0:
        log_done(
            f"{display_path} ({result.nb_row:,} rows, {len(result.variables)} vars, {len(series_files)} files)",
            quiet,
            t0,
        )
    else:
        log_warn(f"{display_path}: empty file", quiet)

    return True
