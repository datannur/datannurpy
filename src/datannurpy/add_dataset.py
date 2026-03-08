"""Add dataset to catalog."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from .utils import (
    build_variable_ids,
    log_done,
    log_section,
    log_skip,
    make_id,
    sanitize_id,
    upsert_folder,
)
from .schema import Dataset, Folder
from .scanner.utils import SUPPORTED_FORMATS, get_mtime_iso, get_mtime_timestamp
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
    from .catalog import Catalog


@dataclass
class DatasetMeta:
    """User-provided dataset metadata."""

    name: str | None = None
    description: str | None = None
    type: str | None = None
    link: str | None = None
    localisation: str | None = None
    manager_id: str | None = None
    owner_id: str | None = None
    tag_ids: list[str] | None = None
    doc_ids: list[str] | None = None
    start_date: str | None = None
    end_date: str | None = None
    updating_each: str | None = None
    no_more_update: str | None = None


def _create_dataset(
    dataset_id: str,
    default_name: str,
    folder_id: str | None,
    data_path: str,
    dataset_path: Path,
    current_mtime: int,
    delivery_format: str,
    meta: DatasetMeta,
    nb_row: int | None = None,
    scanned_description: str | None = None,
) -> Dataset:
    """Create Dataset with common fields."""
    return Dataset(
        id=dataset_id,
        name=meta.name or default_name,
        folder_id=folder_id,
        data_path=data_path,
        last_update_date=get_mtime_iso(dataset_path),
        last_update_timestamp=current_mtime,
        delivery_format=delivery_format,
        nb_row=nb_row,
        description=meta.description
        if meta.description is not None
        else scanned_description,
        type=meta.type,
        link=meta.link,
        localisation=meta.localisation,
        manager_id=meta.manager_id,
        owner_id=meta.owner_id,
        tag_ids=meta.tag_ids or [],
        doc_ids=meta.doc_ids or [],
        start_date=meta.start_date,
        end_date=meta.end_date,
        updating_each=meta.updating_each,
        no_more_update=meta.no_more_update,
        _seen=True,
    )


def add_dataset(
    catalog: Catalog,
    path: str | Path,
    folder: Folder | None = None,
    *,
    depth: Literal["structure", "schema", "full"] | None = None,
    folder_id: str | None = None,
    infer_stats: bool = True,
    csv_encoding: str | None = None,
    quiet: bool | None = None,
    refresh: bool | None = None,
    # Dataset metadata overrides
    name: str | None = None,
    description: str | None = None,
    type: str | None = None,
    link: str | None = None,
    localisation: str | None = None,
    manager_id: str | None = None,
    owner_id: str | None = None,
    tag_ids: list[str] | None = None,
    doc_ids: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    updating_each: str | None = None,
    no_more_update: str | None = None,
) -> None:
    """Add a single dataset file or partitioned directory to the catalog."""
    catalog._has_scanned = True
    q = quiet if quiet is not None else catalog.quiet
    do_refresh = refresh if refresh is not None else catalog.refresh
    resolved_depth: Literal["structure", "schema", "full"] = (
        depth
        if depth is not None
        else cast(Literal["structure", "schema", "full"], catalog.depth)
    )
    dataset_path = Path(path).resolve()

    if not dataset_path.exists():
        raise FileNotFoundError(f"Path not found: {dataset_path}")

    start_time = log_section("add_dataset", dataset_path.name, q)

    # Handle folder
    resolved_folder_id: str | None = None
    if folder is not None:
        if folder_id is not None:
            raise ValueError("Cannot specify both folder and folder_id")
        upsert_folder(catalog, folder)
        resolved_folder_id = folder.id
    elif folder_id is not None:
        resolved_folder_id = folder_id

    # Build metadata container
    meta = DatasetMeta(
        name=name,
        description=description,
        type=type,
        link=link,
        localisation=localisation,
        manager_id=manager_id,
        owner_id=owner_id,
        tag_ids=tag_ids,
        doc_ids=doc_ids,
        start_date=start_date,
        end_date=end_date,
        updating_each=updating_each,
        no_more_update=no_more_update,
    )

    # Check if it's a partitioned Parquet directory
    if dataset_path.is_dir():
        _add_parquet_directory(
            catalog,
            dataset_path,
            resolved_folder_id,
            meta,
            depth=resolved_depth,
            infer_stats=infer_stats,
            quiet=q,
            refresh=do_refresh,
            start_time=start_time,
        )
        return

    # It's a file
    suffix = dataset_path.suffix.lower()
    delivery_format = SUPPORTED_FORMATS.get(suffix)
    if delivery_format is None:
        raise ValueError(
            f"Unsupported format: {suffix}. "
            f"Supported: {', '.join(SUPPORTED_FORMATS.keys())}"
        )

    # Get current mtime
    current_mtime = get_mtime_timestamp(dataset_path)
    data_path_str = str(dataset_path)

    # Check for existing dataset (incremental scan)
    existing = catalog._get_dataset_by_path(data_path_str)
    if existing is not None:
        if not do_refresh and existing.last_update_timestamp == current_mtime:
            # Unchanged - skip and mark as seen
            catalog.dataset.update(existing.id, _seen=True)
            catalog._mark_dataset_modalities_seen(existing)
            log_skip(dataset_path.name, q)
            return
        else:
            # Modified or refresh forced - remove old dataset cascade before rescan
            catalog._remove_dataset_cascade(existing)

    # Build dataset ID
    base_name = sanitize_id(dataset_path.stem)
    dataset_id = (
        make_id(resolved_folder_id, base_name) if resolved_folder_id else base_name
    )

    # Resolve csv_encoding
    resolved_encoding = (
        csv_encoding if csv_encoding is not None else catalog.csv_encoding
    )

    # Structure mode: create dataset without scanning
    if resolved_depth == "structure":
        dataset = _create_dataset(
            dataset_id,
            dataset_path.stem,
            resolved_folder_id,
            data_path_str,
            dataset_path,
            current_mtime,
            delivery_format,
            meta,
        )
        catalog.dataset.add(dataset)
        log_done(dataset_path.name, q, start_time)
        return

    # Schema/Full mode: scan file
    schema_only = resolved_depth == "schema"
    result = scan_file(
        dataset_path,
        delivery_format,
        dataset_id=dataset_id,
        schema_only=schema_only,
        infer_stats=infer_stats and not schema_only,
        freq_threshold=catalog.freq_threshold or None,
        csv_encoding=resolved_encoding,
    )

    dataset = _create_dataset(
        dataset_id,
        dataset_path.stem,
        resolved_folder_id,
        data_path_str,
        dataset_path,
        current_mtime,
        delivery_format,
        meta,
        nb_row=result.nb_row,
        scanned_description=result.description,
    )
    catalog.dataset.add(dataset)

    var_id_mapping = build_variable_ids(result.variables, dataset.id)
    if not schema_only:
        catalog.modality_manager.assign_from_freq(
            result.variables, result.freq_table, var_id_mapping
        )
    catalog._add_variables(result.variables, dataset.id)

    # Log result
    var_count = catalog._get_variable_count(dataset.id)
    if schema_only:
        log_done(f"{dataset_path.name} ({var_count} vars)", q, start_time)
    else:
        log_done(
            f"{dataset_path.name} ({dataset.nb_row:,} rows, {var_count} vars)",
            q,
            start_time,
        )


def _add_parquet_directory(
    catalog: Catalog,
    dir_path: Path,
    folder_id: str | None,
    meta: DatasetMeta,
    *,
    depth: Literal["structure", "schema", "full"],
    infer_stats: bool,
    quiet: bool,
    refresh: bool,
    start_time: float,
) -> None:
    """Add a partitioned Parquet directory (Delta, Hive, or Iceberg) to catalog."""
    current_mtime = get_mtime_timestamp(dir_path)
    data_path_str = str(dir_path)

    # Check for existing dataset (incremental scan)
    existing = catalog._get_dataset_by_path(data_path_str)
    if existing is not None:
        if not refresh and existing.last_update_timestamp == current_mtime:
            catalog.dataset.update(existing.id, _seen=True)
            catalog._mark_dataset_modalities_seen(existing)
            log_skip(dir_path.name, quiet)
            return
        catalog._remove_dataset_cascade(existing)

    # Detect dataset type
    if is_delta_table(dir_path):
        dataset_type, delivery_format = DatasetType.DELTA, "delta"
    elif is_iceberg_table(dir_path):
        dataset_type, delivery_format = DatasetType.ICEBERG, "iceberg"
    elif is_hive_partitioned(dir_path):
        dataset_type, delivery_format = DatasetType.HIVE, "parquet"
    else:
        raise ValueError(
            f"Directory is not a recognized Parquet format "
            f"(Delta, Hive, or Iceberg): {dir_path}"
        )

    # Build dataset ID
    base_name = sanitize_id(dir_path.name)
    dataset_id = make_id(folder_id, base_name) if folder_id else base_name

    # Structure mode: create dataset without scanning
    if depth == "structure":
        dataset = _create_dataset(
            dataset_id,
            dir_path.name,
            folder_id,
            data_path_str,
            dir_path,
            current_mtime,
            delivery_format,
            meta,
        )
        catalog.dataset.add(dataset)
        log_done(dir_path.name, quiet, start_time)
        return

    # Schema/Full mode: scan the dataset
    schema_only = depth == "schema"
    parquet_info = ParquetDatasetInfo(path=dir_path, type=dataset_type)
    variables, nb_row, freq_table, pq_meta = scan_parquet_dataset(
        parquet_info,
        dataset_id=dataset_id,
        infer_stats=infer_stats and not schema_only,
        freq_threshold=catalog.freq_threshold or None,
    )

    # Override meta.name with parquet metadata if not user-provided
    default_name = meta.name or pq_meta.name or dir_path.name
    scanned_desc = pq_meta.description

    dataset = _create_dataset(
        dataset_id,
        default_name,
        folder_id,
        data_path_str,
        dir_path,
        current_mtime,
        delivery_format,
        meta,
        nb_row=nb_row,
        scanned_description=scanned_desc,
    )
    # Force the name (since _create_dataset uses meta.name or default_name)
    dataset.name = default_name
    catalog.dataset.add(dataset)

    var_id_mapping = build_variable_ids(variables, dataset.id)
    if not schema_only:
        catalog.modality_manager.assign_from_freq(variables, freq_table, var_id_mapping)
    catalog._add_variables(variables, dataset.id)

    # Log result
    var_count = catalog._get_variable_count(dataset.id)
    if schema_only:
        log_done(f"{dir_path.name} ({var_count} vars)", quiet, start_time)
    else:
        log_done(
            f"{dir_path.name} ({nb_row:,} rows, {var_count} vars)", quiet, start_time
        )
