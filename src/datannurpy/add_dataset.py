"""Add dataset to catalog."""

from __future__ import annotations

import hashlib
import stat
from collections.abc import Sequence
from pathlib import Path, PurePath, PurePosixPath
from typing import TYPE_CHECKING, Any, cast

from .utils import (
    build_variable_ids,
    error_count,
    log_done,
    log_section,
    log_skip,
    make_id,
    iso_to_timestamp,
    sanitize_id,
)
from .utils.params import _UNSET, validate_params
from .compression import strip_compression_suffix
from .dataset_scan import finalize_scanned_dataset, skip_unchanged
from .errors import ConfigError
from .finalize import remove_dataset_cascade
from .preview import (
    PreviewRows,
    effective_preview_rows,
    remember_preview,
    resolve_preview_rows,
)
from .schema import Dataset, EntityMetadata
from .scanner.filesystem import (
    FileSystem,
    is_remote_url,
    remote_access_error_reason,
)
from .scanner.format_detect import resolve_delivery_format
from .scanner.utils import (
    FsPath,
    fs_info_is_dir,
    get_content_signature,
    get_data_size,
    get_dir_data_size,
    get_mtime_iso,
    get_mtime_timestamp,
)
from .scanner.parquet import (
    DatasetType,
    ParquetDatasetInfo,
    scan_parquet_dataset,
)
from .scanner.parquet.discovery import (
    is_delta_table,
    is_hive_partitioned,
    is_iceberg_table,
)
from .scanner.scan import scan_file

if TYPE_CHECKING:
    from .catalog import Catalog, Depth


def _create_dataset(
    dataset_id: str,
    default_name: str,
    folder_id: str | None,
    data_path: str,
    dataset_path: FsPath,
    current_mtime: int,
    delivery_format: str,
    metadata: EntityMetadata | None,
    nb_row: int | None = None,
    sample_size: int | None = None,
    preview_rows: int | None = None,
    data_size: int | None = None,
    scanned_description: str | None = None,
    crs: str | None = None,
    geometry_type: str | None = None,
    bbox: list[float] | None = None,
    spatial_resolution: float | None = None,
    fs: FileSystem | None = None,
    match_path: str | None = None,
    schema_signature: str | None = None,
) -> Dataset:
    """Create Dataset with common fields."""
    resolved_metadata = metadata or EntityMetadata()
    return Dataset(
        id=dataset_id,
        name=resolved_metadata.name or default_name,
        folder_id=folder_id,
        license=resolved_metadata.license,
        data_path=data_path,
        last_update_date=get_mtime_iso(dataset_path, fs=fs),
        delivery_format=delivery_format,
        nb_row=nb_row,
        sample_size=sample_size,
        preview_rows=preview_rows,
        data_size=data_size,
        description=resolved_metadata.description
        if resolved_metadata.description is not None
        else scanned_description,
        type=resolved_metadata.type,
        link=resolved_metadata.link,
        localisation=resolved_metadata.localisation,
        crs=crs,
        geometry_type=geometry_type,
        bbox=bbox,
        spatial_resolution=spatial_resolution,
        manager_organization_id=resolved_metadata.manager_organization_id,
        owner_organization_id=resolved_metadata.owner_organization_id,
        tag_ids=resolved_metadata.tag_ids or [],
        doc_ids=resolved_metadata.doc_ids or [],
        start_date=resolved_metadata.start_date,
        end_date=resolved_metadata.end_date,
        updating_each=resolved_metadata.updating_each,
        no_more_update=resolved_metadata.no_more_update,
        schema_signature=schema_signature,
        _seen=True,
        _match_path=match_path or data_path,
    )


def _public_data_path(
    dataset_path: FsPath, path_name: str, fs: FileSystem | None
) -> str:
    """Return the exported data_path while keeping local scan paths portable."""
    if fs is not None and not fs.is_local:
        return str(dataset_path)
    return PurePosixPath(path_name).as_posix()


@validate_params
def add_dataset(
    catalog: Catalog,
    path: str | Path | Sequence[str | Path],
    *,
    metadata: EntityMetadata | None = None,
    depth: Depth | None = None,
    format: str | None = None,
    csv_encoding: str | None = None,
    sample_size: int | None = _UNSET,
    auto_enumerations: bool | None = None,
    preview_rows: PreviewRows = None,
    csv_skip_copy: bool | None = None,
    quiet: bool | None = None,
    refresh: bool | None = None,
    storage_options: dict[str, Any] | None = None,
) -> None:
    """Add a single dataset file or partitioned directory to the catalog."""
    if isinstance(path, list):
        kwargs = {k: v for k, v in locals().items() if k not in ("catalog", "path")}
        for p in path:
            add_dataset(catalog, p, **kwargs)
        return
    assert not isinstance(path, Sequence) or isinstance(path, (str, Path))

    catalog._has_scanned = True
    if (depth if depth is not None else catalog.depth) == "value":
        from .scanner.autotag import ensure_auto_tags

        ensure_auto_tags(catalog)
    q = quiet if quiet is not None else catalog.quiet
    do_refresh = refresh if refresh is not None else catalog.refresh
    resolved_depth: Depth = depth if depth is not None else cast("Depth", catalog.depth)
    resolved_auto_enumerations = (
        auto_enumerations
        if auto_enumerations is not None
        else catalog.auto_enumerations
    )

    # Handle remote URLs vs local paths
    is_remote = is_remote_url(path)
    fs: FileSystem | None = None

    if is_remote or storage_options:
        fs = FileSystem(path, storage_options)
        try:
            is_dir = fs_info_is_dir(fs, fs.root)
        except FileNotFoundError as exc:
            reason = remote_access_error_reason(exc)
            if reason is not None:
                raise ConfigError(f"Cannot access {path}: {reason}")
            raise ConfigError(f"Path not found: {path}")
        # URL-rooted backends (http/https) keep the raw root: PurePosixPath would
        # collapse the '//' after the scheme and corrupt the URL. Everything downstream
        # only str()s a remote path, so a plain string is the faithful carrier.
        dataset_path: FsPath = fs.root if "://" in fs.root else PurePosixPath(fs.root)
        path_name = fs.root.rstrip("/").rsplit("/", 1)[-1]
    else:
        dataset_path = Path(path).resolve()
        path_name = dataset_path.name
        try:
            dataset_stat = dataset_path.stat()
        except FileNotFoundError:
            raise ConfigError(f"Path not found: {dataset_path}")
        is_dir = stat.S_ISDIR(dataset_stat.st_mode)

    start_time = log_section("add_dataset", path_name, q)

    resolved_folder_id = metadata.parent_id if metadata is not None else None

    resolved_sample_size = (
        sample_size if sample_size is not _UNSET else catalog.sample_size
    )
    preview_limit = effective_preview_rows(
        resolve_preview_rows(preview_rows, catalog.preview_rows), resolved_depth
    )

    if is_dir:
        if isinstance(dataset_path, Path) and dataset_path.suffix.lower() == ".gdb":
            raise ConfigError(
                f"File Geodatabase is a multi-layer container, not a single dataset: "
                f"{path}. Use add_geodatabase() instead."
            )
        _add_parquet_directory(
            catalog,
            dataset_path,
            resolved_folder_id,
            metadata,
            depth=resolved_depth,
            sample_size=resolved_sample_size,
            auto_enumerations=resolved_auto_enumerations,
            preview_rows=preview_limit,
            quiet=q,
            refresh=do_refresh,
            start_time=start_time,
            fs=fs,
        )
        return

    # It's a file — resolve the delivery format. Locally the extension is trusted;
    # for remote sources an explicit format: wins, else a detection cascade runs
    # (content sniffing only when the depth already reads content).
    delivery_format = resolve_delivery_format(
        path_name,
        explicit_format=format,
        fs=fs,
        remote_path=dataset_path,
        allow_content_sniff=resolved_depth != "dataset",
        quiet=q,
    )

    # Get current freshness signals: mtime, and an ETag content signature that lets
    # incremental runs skip endpoints without a usable Last-Modified header.
    current_mtime = get_mtime_timestamp(dataset_path, fs=fs)
    current_signature = get_content_signature(dataset_path, fs=fs)
    match_path = str(dataset_path)
    data_path_str = _public_data_path(dataset_path, path_name, fs)

    # Check for existing dataset (incremental scan)
    if skip_unchanged(
        catalog,
        match_path,
        data_path_str,
        current_mtime,
        refresh=do_refresh,
        preview_rows=preview_limit,
        quiet=q,
        label=path_name,
        current_signature=current_signature,
    ):
        catalog._tally_scan(0, 1)
        return

    # Build dataset ID and default name from a clean segment (URL query string
    # stripped). Remote endpoints that differ only by query string (…/CSV?type=a vs
    # …/CSV?type=b) would otherwise collide on id, so a short URL hash keeps the
    # default id unique and stable across runs. Explicit metadata.id/name still win.
    clean_segment = strip_compression_suffix(
        path_name.split("?", 1)[0].split("#", 1)[0]
    )
    path_stem = PurePosixPath(clean_segment).stem or clean_segment
    base_name = sanitize_id(path_stem)
    if is_remote and "?" in path_name:
        url_hash = hashlib.sha256(str(dataset_path).encode()).hexdigest()[:8]
        base_name = f"{base_name}_{url_hash}"
    dataset_id = (
        metadata.id
        if metadata is not None and metadata.id is not None
        else (
            make_id(resolved_folder_id, base_name) if resolved_folder_id else base_name
        )
    )

    # Resolve csv_encoding
    resolved_encoding = (
        csv_encoding if csv_encoding is not None else catalog.csv_encoding
    )
    resolved_csv_skip_copy = (
        csv_skip_copy if csv_skip_copy is not None else catalog.csv_skip_copy
    )
    # Structure mode: create dataset without scanning
    if resolved_depth == "dataset":
        dataset = _create_dataset(
            dataset_id,
            path_stem,
            resolved_folder_id,
            data_path_str,
            dataset_path,
            current_mtime,
            delivery_format,
            metadata,
            preview_rows=0,
            data_size=get_data_size(dataset_path, fs=fs),
            fs=fs,
            match_path=match_path,
            schema_signature=current_signature,
        )
        catalog.dataset.add(dataset)
        catalog._tally_scan(1, 0)
        log_done(path_name, q, start_time)
        return

    # Schema/Stat/Value mode: scan file
    schema_only = resolved_depth == "variable"
    freq_threshold = catalog.freq_threshold if resolved_depth == "value" else None
    sample_size_for_scan: int | None = (
        resolved_sample_size if resolved_depth == "value" else None
    )
    errors_before = error_count()
    result = scan_file(
        dataset_path,
        delivery_format,
        dataset_id=dataset_id,
        schema_only=schema_only,
        freq_threshold=freq_threshold,
        csv_encoding=resolved_encoding,
        sample_size=sample_size_for_scan,
        preview_rows=preview_limit,
        csv_skip_copy=resolved_csv_skip_copy,
        fs=fs,
        quiet=q,
        path_label=path_name,
    )

    dataset = _create_dataset(
        dataset_id,
        path_stem,
        resolved_folder_id,
        data_path_str,
        dataset_path,
        current_mtime,
        delivery_format,
        metadata,
        nb_row=result.nb_row,
        sample_size=result.sample_size,
        preview_rows=preview_limit,
        data_size=get_data_size(dataset_path, fs=fs),
        scanned_description=result.description,
        crs=result.crs,
        geometry_type=result.geometry_type,
        bbox=result.bbox,
        spatial_resolution=result.spatial_resolution,
        fs=fs,
        match_path=match_path,
        schema_signature=current_signature,
    )
    finalize_scanned_dataset(
        catalog,
        dataset,
        variables=result.variables,
        freq_table=result.freq_table,
        preview=result.preview,
        label=path_name,
        auto_enumerations=resolved_auto_enumerations,
    )
    # Errors the scanner handled internally (logged the ✗ itself and returned
    # an empty result) still count for the run tally — at most one per dataset.
    catalog._tally_scan(1, 0, min(1, error_count() - errors_before))

    # Log result
    var_count = len(result.variables)
    if schema_only:
        log_done(f"{path_name} ({var_count} vars)", q, start_time)
    elif dataset.nb_row is None:
        # Scanner already emitted a warning explaining the failure.
        pass
    else:
        log_done(
            f"{path_name} ({dataset.nb_row:,} rows, {var_count} vars)",
            q,
            start_time,
        )


def _add_parquet_directory(
    catalog: Catalog,
    dir_path: PurePath,
    folder_id: str | None,
    metadata: EntityMetadata | None,
    *,
    depth: Depth,
    sample_size: int | None,
    auto_enumerations: bool,
    preview_rows: int = 0,
    quiet: bool,
    refresh: bool,
    start_time: float,
    fs: FileSystem | None = None,
) -> None:
    """Add a partitioned Parquet directory (Delta, Hive, or Iceberg) to catalog."""
    current_mtime = get_mtime_timestamp(dir_path, fs=fs)
    match_path = str(dir_path)
    # Normalize native separators first: a local Windows dir_path is backslashed,
    # so a bare rsplit("/") would return the whole path instead of the folder name.
    dir_name = str(dir_path).replace("\\", "/").rstrip("/").rsplit("/", 1)[-1]
    data_path_str = _public_data_path(dir_path, dir_name, fs)

    # Check for existing dataset (incremental scan)
    existing = catalog.dataset.get_by("_match_path", match_path)
    if existing is None:
        existing = catalog.dataset.get_by("_match_path", data_path_str)
    if existing is not None:
        if not refresh and iso_to_timestamp(existing.last_update_date) == current_mtime:
            catalog.dataset.update(
                existing.id,
                _seen=True,
                _match_path=match_path,
                preview_rows=preview_rows,
            )
            catalog.enumeration_manager.mark_dataset_seen(existing.id)
            catalog._tally_scan(0, 1)
            log_skip(dir_name, quiet)
            return
        remove_dataset_cascade(catalog, existing)

    # Detect dataset type
    if is_delta_table(dir_path, fs=fs):
        dataset_type, delivery_format = DatasetType.DELTA, "delta"
    elif is_iceberg_table(dir_path, fs=fs):
        dataset_type, delivery_format = DatasetType.ICEBERG, "iceberg"
    elif is_hive_partitioned(dir_path, fs=fs):
        dataset_type, delivery_format = DatasetType.HIVE, "parquet"
    else:
        raise ConfigError(
            f"Directory is not a recognized Parquet format "
            f"(Delta, Hive, or Iceberg): {dir_path}"
        )

    # Build dataset ID
    base_name = sanitize_id(dir_name)
    dataset_id = (
        metadata.id
        if metadata is not None and metadata.id is not None
        else (make_id(folder_id, base_name) if folder_id else base_name)
    )

    # Structure mode: create dataset without scanning
    if depth == "dataset":
        dataset = _create_dataset(
            dataset_id,
            dir_name,
            folder_id,
            data_path_str,
            dir_path,
            current_mtime,
            delivery_format,
            metadata,
            preview_rows=0,
            data_size=get_dir_data_size(dir_path, fs=fs),
            fs=fs,
            match_path=match_path,
        )
        catalog.dataset.add(dataset)
        catalog._tally_scan(1, 0)
        log_done(dir_name, quiet, start_time)
        return

    # Variable/Stat/Value mode: scan the dataset
    schema_only = depth == "variable"
    parquet_info = ParquetDatasetInfo(path=dir_path, type=dataset_type)
    variables, nb_row, freq_table, pq_meta, preview = scan_parquet_dataset(
        parquet_info,
        dataset_id=dataset_id,
        infer_stats=not schema_only,
        freq_threshold=(catalog.freq_threshold if depth == "value" else None),
        sample_size=(sample_size if depth == "value" else None),
        preview_rows=preview_rows,
        return_preview=True,
        quiet=quiet,
    )

    # Override default_name with parquet metadata if the caller did not set one.
    default_name = (
        (metadata.name if metadata is not None else None) or pq_meta.name or dir_name
    )
    scanned_desc = pq_meta.description

    dataset = _create_dataset(
        dataset_id,
        default_name,
        folder_id,
        data_path_str,
        dir_path,
        current_mtime,
        delivery_format,
        metadata,
        nb_row=nb_row,
        sample_size=pq_meta.sample_size,
        preview_rows=preview_rows,
        data_size=pq_meta.data_size,
        scanned_description=scanned_desc,
        fs=fs,
        match_path=match_path,
    )
    # Force the name (since _create_dataset uses meta.name or default_name)
    dataset.name = default_name
    catalog.dataset.add(dataset)
    remember_preview(catalog, dataset.id, preview, label=dir_name, variables=variables)

    var_id_mapping = build_variable_ids(variables, dataset.id)
    if freq_table is not None:
        catalog.enumeration_manager.assign_from_freq(
            variables,
            freq_table,
            var_id_mapping,
            auto_enumerations=auto_enumerations,
        )
    catalog.variable.add_all(variables)
    catalog._tally_scan(1, 0)

    # Log result
    var_count = len(variables)
    if schema_only:
        log_done(f"{dir_name} ({var_count} vars)", quiet, start_time)
    else:
        log_done(f"{dir_name} ({nb_row:,} rows, {var_count} vars)", quiet, start_time)
