"""Add folder to catalog."""

from __future__ import annotations

import stat
from collections.abc import Sequence
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path, PurePath, PurePosixPath
from typing import TYPE_CHECKING, Any, Literal, NoReturn

from .utils import (
    build_dataset_id_name,
    build_variable_ids,
    error_count,
    get_folder_id,
    log_debug,
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
from .dataset_scan import finalize_scanned_dataset, skip_unchanged
from .errors import ConfigError
from .finalize import remove_dataset_cascade
from .preview import (
    PreviewRows,
    effective_preview_rows,
    remember_preview,
    resolve_preview_rows,
)
from .schema import Dataset, EntityMetadata, Folder, folder_from_metadata
from .scanner.discovery import (
    DatasetInfo,
    DiscoveryResult,
    ScanPlan,
    compute_scan_plan,
    discover_datasets,
)
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
from .scanner.archive import (
    unsupported_zip_error,
    zip_member_list,
    zip_scannable_member,
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


def _no_rows_message(display_path: str, data_size: int | None) -> str:
    """A truthful zero-row warning: "empty file" only when the file really is
    empty on disk; a header-only or unparsed-to-zero file says "no data rows"."""
    if data_size == 0:
        return f"{display_path}: empty file (0 bytes)"
    return f"{display_path}: no data rows"


def _resolve_zip_format(
    path: PurePath, fs: FileSystem | None, display_path: str, quiet: bool
) -> str | None:
    """Classify a discovered ``.zip`` by its single scannable member.

    Returns the member's delivery format, or None — with a per-file warning —
    when the archive holds no single scannable data file or is not actually a
    zip. The folder-scan counterpart of the ``dataset:`` classification, which
    raises ``ConfigError`` instead: in a folder sweep one unsupported archive
    must not abort the run, mirroring how unsupported extensions are skipped."""
    names = zip_member_list(path, fs)
    if names is None:
        log_warn(f"{display_path}: not a zip archive, skipped", quiet)
        return None
    selected = zip_scannable_member(names)
    if selected is None:
        log_warn(f"{unsupported_zip_error(display_path, names)} Skipped.", quiet)
        return None
    return selected[1]


_SQLITE_MAGIC = b"SQLite format 3\x00"


def _is_sqlite_file(path: PurePath, fs: FileSystem | None) -> bool:
    """Cheap magic-byte check so a misnamed ``.gpkg`` is skipped, not errored."""
    try:
        if fs is not None and not fs.is_local:
            with fs.open(str(path), "rb") as f:
                header = f.read(len(_SQLITE_MAGIC))
        else:
            with open(path, "rb") as f:
                header = f.read(len(_SQLITE_MAGIC))
    except OSError:
        return False
    return header == _SQLITE_MAGIC


def _scan_geopackage_metadata_first(
    catalog: Catalog,
    info: DatasetInfo,
    *,
    root: PurePath,
    fs: FileSystem | None,
    depth: Depth,
    on_unmatched: OnUnmatched,
    quiet: bool,
    refresh: bool,
    preview_rows: int,
    freq_threshold: int | None,
    sample_size: int | None,
    auto_enumerations: bool,
) -> None:
    """Scan a folder-discovered GeoPackage without creating any folder.

    Metadata-first counterpart of the container delegation: each layer/table
    attaches like a sibling scanned file. A layer reuses a pre-loaded dataset
    row matched on ``<file path>::<layer>``; otherwise the file's own row —
    matched like any scanned file — anchors it (id ``<file id>---<layer>``,
    same folder). A file with no match at all follows the on_unmatched policy.
    """
    from .scanner.database import (
        close_connection,
        connect,
        get_table_data_size,
        list_tables,
        scan_table,
    )
    from .scanner.geopackage import extract_geopackage_geo

    display = _display_path(info.path, root)
    if not _is_sqlite_file(info.path, fs):
        log_warn(f"{display}: not a SQLite/GeoPackage file, skipped", quiet)
        return
    candidates = _match_path_candidates(info.path, fs)
    file_peek = find_loaded_dataset_by_match_paths(catalog, candidates)

    local_ctx = (
        fs.ensure_local(str(info.path))
        if fs is not None and not fs.is_local
        else nullcontext(info.path)
    )
    scanned = unchanged = errors = 0
    with local_ctx as local_path:
        con, _ = connect(f"sqlite:///{local_path}")
        try:
            tables = list_tables(con, None, None, None, "sqlite")
            layer_peeks = {
                table: find_loaded_dataset_by_match_paths(
                    catalog, [f"{c}::{table}" for c in candidates]
                )
                for table in tables
            }
            if file_peek is None and not any(layer_peeks.values()):
                _handle_unmatched(display, on_unmatched, quiet)
                return
            geo_by_table = extract_geopackage_geo(con)
            public_path = _public_data_path(info.path, root, fs)
            last_update = timestamp_to_iso(info.mtime)
            for table in tables:
                peek = layer_peeks[table]
                if peek is not None:
                    dataset_id, folder_id = peek.id, peek.folder_id
                elif file_peek is not None:
                    dataset_id = make_id(file_peek.id, sanitize_id(table))
                    folder_id = file_peek.folder_id
                else:
                    _handle_unmatched(f"{display}::{table}", on_unmatched, quiet)
                    continue
                label = f"{display}::{table}"
                match_path = f"{info.path}::{table}"
                data_path = f"{public_path}/{table}"
                if skip_unchanged(
                    catalog,
                    match_path,
                    data_path,
                    info.mtime,
                    refresh=refresh,
                    preview_rows=preview_rows,
                    quiet=quiet,
                    label=label,
                ):
                    unchanged += 1
                    continue
                t0 = log_start(label, quiet)
                variables: list[Any] = []
                nb_row = actual_sample_size = freq_table = preview = None
                if depth != "dataset":
                    try:
                        variables, nb_row, actual_sample_size, freq_table, preview = (
                            scan_table(
                                con,
                                table,
                                schema=None,
                                dataset_id=dataset_id,
                                infer_stats=depth in ("stat", "value"),
                                freq_threshold=freq_threshold,
                                sample_size=sample_size,
                                preview_rows=preview_rows,
                                return_preview=True,
                                quiet=quiet,
                            )
                        )
                    except Exception as exc:
                        log_error(label, exc, quiet)
                        errors += 1
                        continue
                geo = geo_by_table.get(table) or {}
                dataset = Dataset(
                    id=dataset_id,
                    name=table,
                    folder_id=folder_id,
                    data_path=data_path,
                    last_update_date=last_update,
                    delivery_format="geopackage",
                    nb_row=nb_row,
                    sample_size=actual_sample_size,
                    preview_rows=preview_rows,
                    data_size=get_table_data_size(con, table, None),
                    crs=geo.get("crs"),
                    geometry_type=geo.get("geometry_type"),
                    bbox=geo.get("bbox"),
                    _seen=True,
                    _match_path=match_path,
                )
                finalize_scanned_dataset(
                    catalog,
                    dataset,
                    variables=variables,
                    freq_table=freq_table,
                    preview=preview,
                    label=label,
                    auto_enumerations=auto_enumerations,
                )
                scanned += 1
                if nb_row is not None:
                    log_done(
                        f"{label} ({nb_row:,} rows, {len(variables)} vars)", quiet, t0
                    )
                elif variables:
                    log_done(f"{label} ({len(variables)} vars)", quiet, t0)
                else:
                    log_done(label, quiet, t0)
        finally:
            close_connection(con)
    catalog._tally_scan(scanned, unchanged, errors)


def _delegate_geopackage(
    catalog: Catalog,
    info: DatasetInfo,
    *,
    root: PurePath,
    fs: FileSystem | None,
    prefix: str,
    subdir_ids: dict[PurePath, str],
    create_folders: bool,
    depth: Depth,
    on_unmatched: OnUnmatched,
    quiet: bool,
    refresh: bool,
    storage_options: dict[str, Any] | None,
    preview_rows: int,
    freq_threshold: int | None,
    sample_size: int | None,
    auto_enumerations: bool,
) -> None:
    """Scan a folder-discovered GeoPackage through the database machinery.

    Exactly what an explicit ``database: sqlite:///….gpkg`` entry does — one
    dataset per layer/table, ``gpkg_*``/``rtree_*`` system tables filtered,
    CRS / geometry type / bbox enrichment — with the container folder nested
    under the scan tree instead of at the catalog root. In metadata-first mode
    (``create_folders=False``) no container is created: layers attach to
    pre-loaded dataset rows like sibling files."""
    display = _display_path(info.path, root)
    if not create_folders:
        try:
            _scan_geopackage_metadata_first(
                catalog,
                info,
                root=root,
                fs=fs,
                depth=depth,
                on_unmatched=on_unmatched,
                quiet=quiet,
                refresh=refresh,
                preview_rows=preview_rows,
                freq_threshold=freq_threshold,
                sample_size=sample_size,
                auto_enumerations=auto_enumerations,
            )
        except ConfigError:
            raise  # on_unmatched="error" propagates like any unmatched file
        except Exception as exc:
            log_error(display, exc, quiet)
            catalog._tally_scan(0, 0, 1)
        return
    # An explicit `database: sqlite:///….gpkg` entry may already catalog this
    # file (the pre-discovery pattern, often carrying curated metadata) — its
    # container folder wins, discovery steps aside instead of duplicating it.
    # The reverse order works too: an explicit entry running later takes over
    # a _discovered container (see _upsert_database_root_folder).
    db_data_path = f"sqlite://{Path(str(info.path)).stem}"
    existing = catalog.folder.get_by("data_path", db_data_path)
    if existing is not None and not existing._discovered:
        log_debug(f"{display}: already catalogued via a database entry, skipped", quiet)
        return
    if not _is_sqlite_file(info.path, fs):
        log_warn(f"{display}: not a SQLite/GeoPackage file, skipped", quiet)
        return
    folder_id, folder_name = build_dataset_id_name(info.path, root, prefix)
    parent_id = get_folder_id(info.path, root, prefix, subdir_ids)
    if fs is not None and not fs.is_local:
        # canonical_url_for_path yields a user-free URL for netloc-style schemes
        # (s3://bucket/…); hostless ones (memory:// …) rebuild via fsspec.
        connection = fs.canonical_url_for_path(info.path) or fs.fs.unstrip_protocol(
            str(info.path)
        )
    else:
        connection = f"sqlite:///{info.path}"

    from .add_database import add_database

    try:
        add_database(
            catalog,
            connection,
            metadata=EntityMetadata(
                id=folder_id, name=folder_name, parent_id=parent_id
            ),
            depth=depth,
            quiet=quiet,
            refresh=refresh,
            storage_options=storage_options,
        )
    except Exception as exc:
        log_error(display, exc, quiet)
        catalog._tally_scan(0, 0, 1)
        return
    # Mark the container so a later explicit database entry can take it over.
    if catalog.folder.get(folder_id) is not None:
        catalog.folder.update(folder_id, _discovered=True)


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


def _resolve_folder_root(
    path: str | Path, storage_options: dict[str, Any] | None, quiet: bool
) -> tuple[PurePath, str, FileSystem | None]:
    """Resolve the scan root (local path or remote filesystem) and reject
    paths that are themselves a dataset."""
    fs: FileSystem | None = None
    if is_remote_url(path) or storage_options:
        fs = FileSystem(path, storage_options)
        try:
            is_dir = fs_info_is_dir(fs, fs.root)
        except FileNotFoundError:
            _raise_folder_config_error(f"Folder not found: {path}", quiet)
        if not is_dir:
            _raise_folder_config_error(f"Not a directory: {path}", quiet)
        # Use PurePosixPath to preserve forward slashes on Windows
        root: PurePath = PurePosixPath(fs.root)
        root_name = fs.root.rstrip("/").rsplit("/", 1)[-1]
    else:
        root = Path(path).resolve()
        root_name = root.name
        try:
            root_stat = root.stat()
        except FileNotFoundError:
            _raise_folder_config_error(f"Folder not found: {root}", quiet)
        if not stat.S_ISDIR(root_stat.st_mode):
            _raise_folder_config_error(f"Not a directory: {root}", quiet)

    # Reject if path is a dataset (Delta/Hive/Iceberg) - use add_dataset instead
    if (
        is_delta_table(root, fs=fs)
        or is_hive_partitioned(root, fs=fs)
        or is_iceberg_table(root, fs=fs)
    ):
        raise ConfigError(
            f"Path is a dataset, not a folder: {root}. Use add_dataset() instead."
        )
    return root, root_name, fs


def _create_folder_tree(
    catalog: Catalog,
    metadata: EntityMetadata | None,
    *,
    root: PurePath,
    root_name: str,
    fs: FileSystem | None,
    discovery: DiscoveryResult,
) -> tuple[str, dict[PurePath, str]]:
    """Create the root folder and one folder per discovered sub-directory.

    Returns (root folder id, subdir path -> folder id)."""
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
            non_temporal_parts = get_series_folder_parts(info.series_normalized_path)
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
    return prefix, subdir_ids


def _structure_only_scan(
    catalog: Catalog,
    plan: ScanPlan,
    *,
    root: PurePath,
    fs: FileSystem | None,
    prefix: str,
    subdir_ids: dict[PurePath, str],
    create_folders: bool,
    on_unmatched: OnUnmatched,
    quiet: bool,
) -> int:
    """Depth "dataset": create or update dataset entries without scanning
    content. Returns the number of datasets actually touched — an unmatched
    file (create_folders=False) is skipped, not scanned, so it must not
    inflate the tally."""
    # Skip unchanged datasets (single batch upsert instead of per-dataset update)
    skipped: list[Dataset] = []
    for info in plan.to_skip:
        existing = plan.existing_by_path.get(str(info.path))
        assert existing is not None
        existing._seen = True
        existing.preview_rows = 0
        existing._match_path = str(info.path)
        skipped.append(existing)
        log_skip(_display_dataset_label(info, root), quiet)
    if skipped:
        catalog.dataset.upsert_all(skipped)

    scanned = 0
    for info in plan.to_scan:
        display_label = _display_dataset_label(info, root)
        t0 = log_start(display_label, quiet)
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
            log_done(display_label, quiet, t0)
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
            _handle_unmatched(display_label, on_unmatched, quiet)
            continue

        # A discovered .zip is classified by content even at dataset depth — like
        # the dataset: path, since the delivery_format cannot be known any other way.
        delivery_format = info.format
        if delivery_format == "zip":
            resolved_zip = _resolve_zip_format(info.path, fs, display_label, quiet)
            if resolved_zip is None:
                continue  # warning already logged
            delivery_format = resolved_zip

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
            fallback_id, dataset_name = build_dataset_id_name(info.path, root, prefix)
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
            delivery_format=delivery_format,
            nb_resources=nb_resources,
            preview_rows=0,
            data_size=(
                get_dir_data_size(info.path, fs=fs)
                if delivery_format in _DIR_FORMATS
                else get_data_size(info.path, fs=fs)
            ),
            start_date=start_date,
            end_date=end_date,
            _seen=True,
            _match_path=data_path_str,
        )
        catalog.dataset.add(dataset)
        log_done(display_label, quiet, t0)
        scanned += 1
    return scanned


@dataclass
class _FolderScan:
    """Shared context and accumulators for one add_folder content scan."""

    catalog: Catalog
    root: PurePath
    fs: FileSystem | None
    prefix: str
    subdir_ids: dict[PurePath, str]
    existing_by_path: dict[str, Any]
    quiet: bool
    create_folders: bool
    on_unmatched: OnUnmatched
    schema_only: bool
    freq_threshold: int | None
    csv_encoding: str | None
    sample_size: int | None
    auto_enumerations: bool
    preview_rows: int
    csv_skip_copy: bool
    # Count only datasets actually created/updated. An unmatched file
    # (create_folders=False) is skipped before any scan, so it is neither an
    # error nor a scan and must not inflate the tally.
    scanned: int = 0
    scan_errors: int = 0


def _scan_single_file(run: _FolderScan, info: DatasetInfo, display_path: str) -> None:
    """Scan one regular file and upsert its dataset and variables."""
    data_path_str = str(info.path)

    t0 = log_start(display_path, run.quiet)

    # Metadata-first peek: reuse pre-loaded id/folder_id if available.
    peek = find_loaded_dataset_by_match_paths(
        run.catalog, _match_path_candidates(info.path, run.fs)
    )
    if peek is None and not run.create_folders:
        _handle_unmatched(display_path, run.on_unmatched, run.quiet)
        return

    fallback_id, dataset_name = build_dataset_id_name(info.path, run.root, run.prefix)
    fallback_folder_id = get_folder_id(info.path, run.root, run.prefix, run.subdir_ids)
    dataset_id, folder_id = _resolve_ids_from_peek(
        peek, fallback_id, fallback_folder_id, run.create_folders
    )

    # Scan dataset
    errors_before = error_count()
    delivery_format = info.format
    try:
        if delivery_format == "zip":
            resolved = _resolve_zip_format(info.path, run.fs, display_path, run.quiet)
            if resolved is None:
                return  # warning already logged; unsupported, not a scan error
            delivery_format = resolved
        result = scan_file(
            info.path,
            delivery_format,
            dataset_id=dataset_id,
            schema_only=run.schema_only,
            freq_threshold=run.freq_threshold,
            csv_encoding=run.csv_encoding,
            sample_size=run.sample_size,
            preview_rows=run.preview_rows,
            csv_skip_copy=run.csv_skip_copy,
            fs=run.fs,
            quiet=run.quiet,
            path_label=display_path,
        )
    except Exception as exc:
        log_error(display_path, exc, run.quiet)
        run.scan_errors += min(1, error_count() - errors_before)
        return
    # One mechanism for both paths — a ✗ logged during the scan, swallowed
    # by the scanner or caught just above, marks the dataset failed (at
    # most once, however many ✗ the scan logged).
    run.scan_errors += min(1, error_count() - errors_before)

    # Remove old dataset only after successful scan
    existing = run.existing_by_path.get(data_path_str)
    if existing:
        remove_dataset_cascade(run.catalog, existing)

    # Create dataset
    dataset = Dataset(
        id=dataset_id,
        name=result.name or dataset_name,
        folder_id=folder_id,
        data_path=_public_data_path(info.path, run.root, run.fs),
        last_update_date=timestamp_to_iso(info.mtime),
        delivery_format=delivery_format,
        description=result.description,
        nb_row=result.nb_row,
        sample_size=result.sample_size,
        preview_rows=run.preview_rows,
        data_size=(
            result.data_size
            if delivery_format in _DIR_FORMATS
            else get_data_size(info.path, fs=run.fs)
        ),
        crs=result.crs,
        geometry_type=result.geometry_type,
        bbox=result.bbox,
        spatial_resolution=result.spatial_resolution,
        _seen=True,
        _match_path=data_path_str,
    )
    run.catalog.dataset.add(dataset)
    run.scanned += 1
    remember_preview(
        run.catalog,
        dataset.id,
        result.preview,
        label=display_path,
        variables=result.variables,
    )

    var_id_mapping = build_variable_ids(result.variables, dataset.id)
    if result.freq_table is not None:
        run.catalog.enumeration_manager.assign_from_freq(
            result.variables,
            result.freq_table,
            var_id_mapping,
            auto_enumerations=run.auto_enumerations,
        )
    run.catalog.variable.add_all(result.variables)

    # Log result
    if run.schema_only:
        log_done(f"{display_path} ({len(result.variables)} vars)", run.quiet, t0)
    elif result.nb_row is None:
        # Scanner already emitted a warning or error explaining why the
        # file could not be scanned (untreatable, malformed, etc.).
        pass
    elif result.nb_row > 0:
        log_done(
            f"{display_path} ({result.nb_row:,} rows, {len(result.variables)} vars)",
            run.quiet,
            t0,
        )
    else:
        log_warn(_no_rows_message(display_path, dataset.data_size), run.quiet)


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

    root, root_name, fs = _resolve_folder_root(path, storage_options, q)

    start_time = log_section("add_folder", str(root), q)
    vars_before = catalog.variable.count

    # Discover all datasets (parquet + other formats)
    discovery = discover_datasets(
        root, include, exclude, recursive, time_series=time_series, fs=fs
    )

    if create_folders:
        prefix, subdir_ids = _create_folder_tree(
            catalog,
            metadata,
            root=root,
            root_name=root_name,
            fs=fs,
            discovery=discovery,
        )
    else:
        # Metadata-first mode: do not create any folder; rely on peek for ids.
        prefix = ""
        subdir_ids = {}

    # GeoPackages are containers scanned through the database machinery — one
    # dataset per layer/table — so they leave the one-file-one-dataset plan and
    # are delegated after this scan's own summary (their tallies are their own).
    geopackages = [i for i in discovery.datasets if i.format == "geopackage"]
    regular = [i for i in discovery.datasets if i.format != "geopackage"]

    resolved_sample_size: int | None = None
    if resolved_depth == "value":
        resolved_sample_size = (
            sample_size if sample_size is not _UNSET else catalog.sample_size
        )

    def _delegate_geopackages() -> None:
        for info in geopackages:
            _delegate_geopackage(
                catalog,
                info,
                root=root,
                fs=fs,
                prefix=prefix,
                subdir_ids=subdir_ids,
                create_folders=create_folders,
                depth=resolved_depth,
                on_unmatched=on_unmatched,
                quiet=q,
                refresh=do_refresh,
                storage_options=storage_options,
                preview_rows=preview_limit,
                freq_threshold=(
                    catalog.freq_threshold if resolved_depth == "value" else None
                ),
                sample_size=resolved_sample_size,
                auto_enumerations=resolved_auto_enumerations,
            )

    # Compute scan plan (what to scan vs skip)
    plan = compute_scan_plan(regular, catalog, do_refresh, root=root)
    resource_count = sum(info.resource_count for info in regular)

    # Structure-only mode: create/update datasets without scanning
    if resolved_depth == "dataset":
        scanned = _structure_only_scan(
            catalog,
            plan,
            root=root,
            fs=fs,
            prefix=prefix,
            subdir_ids=subdir_ids,
            create_folders=create_folders,
            on_unmatched=on_unmatched,
            quiet=q,
        )
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
        _delegate_geopackages()
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
    run = _FolderScan(
        catalog=catalog,
        root=root,
        fs=fs,
        prefix=prefix,
        subdir_ids=subdir_ids,
        existing_by_path=plan.existing_by_path,
        quiet=q,
        create_folders=create_folders,
        on_unmatched=on_unmatched,
        schema_only=resolved_depth == "variable",
        freq_threshold=catalog.freq_threshold if resolved_depth == "value" else None,
        csv_encoding=csv_encoding if csv_encoding is not None else catalog.csv_encoding,
        sample_size=resolved_sample_size,
        auto_enumerations=resolved_auto_enumerations,
        preview_rows=preview_limit,
        csv_skip_copy=(
            csv_skip_copy if csv_skip_copy is not None else catalog.csv_skip_copy
        ),
    )

    for info in plan.to_scan:
        display_path = _display_dataset_path(info, root)
        # Time series: special handling
        if info.series_files is not None:
            errors_before = error_count()
            try:
                if _scan_time_series(
                    catalog=catalog,
                    info=info,
                    root=root,
                    prefix=prefix,
                    existing_by_path=plan.existing_by_path,
                    schema_only=run.schema_only,
                    freq_threshold=run.freq_threshold,
                    csv_encoding=run.csv_encoding,
                    sample_size=run.sample_size,
                    auto_enumerations=run.auto_enumerations,
                    preview_rows=run.preview_rows,
                    csv_skip_copy=run.csv_skip_copy,
                    quiet=q,
                    fs=fs,
                    create_folders=create_folders,
                    on_unmatched=on_unmatched,
                ):
                    run.scanned += 1
            except Exception as exc:
                log_error(display_path, exc, q)
            # One mechanism for both paths — a ✗ logged during the scan,
            # swallowed by the scanner or caught just above, marks the dataset
            # failed. Clamped: a series re-scans files (schema pass + full
            # pass), so one bad file may log several ✗ for one dataset.
            run.scan_errors += min(1, error_count() - errors_before)
            continue

        # Single file: standard handling
        _scan_single_file(run, info, display_path)

    vars_added = catalog.variable.count - vars_before
    unchanged = len(plan.to_skip)
    catalog._tally_scan(run.scanned, unchanged, run.scan_errors)
    log_summary(
        run.scanned,
        vars_added,
        q,
        start_time,
        run.scan_errors,
        resource_count=resource_count,
        resource_label="files",
        unchanged=unchanged,
    )
    _delegate_geopackages()


def _scan_time_series(
    catalog: Catalog,
    info: DatasetInfo,
    root: PurePath,
    prefix: str,
    existing_by_path: dict[str, Any],
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

    # Remove old dataset if exists. compute_scan_plan already resolved the
    # multi-key match (path, normalized series pattern) and cached the result
    # under str(info.path), so reuse it instead of re-reading catalog.dataset in
    # the loop — a live read would flush the insert buffer every iteration.
    existing = existing_by_path.get(str(info.path))
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
        _match_path=str(last_path),
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
    elif result.nb_row is None:
        # Scanner already emitted a warning or error explaining the failure.
        pass
    elif result.nb_row > 0:
        log_done(
            f"{display_path} ({result.nb_row:,} rows, {len(result.variables)} vars, {len(series_files)} files)",
            quiet,
            t0,
        )
    else:
        log_warn(_no_rows_message(display_path, dataset.data_size), quiet)

    return True
