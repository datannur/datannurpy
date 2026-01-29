"""Add dataset to catalog."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from .utils import build_variable_ids, log_done, log_section, make_id, sanitize_id
from .entities import Dataset, Folder
from .scanner.utils import SUPPORTED_FORMATS, get_mtime_iso
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


def add_dataset(
    catalog: Catalog,
    path: str | Path,
    folder: Folder | None = None,
    *,
    folder_id: str | None = None,
    infer_stats: bool = True,
    csv_encoding: str | None = None,
    quiet: bool | None = None,
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
    q = quiet if quiet is not None else catalog.quiet
    dataset_path = Path(path).resolve()

    if not dataset_path.exists():
        raise FileNotFoundError(f"Path not found: {dataset_path}")

    start_time = log_section("add_dataset", dataset_path.name, q)

    # Handle folder
    resolved_folder_id: str | None = None
    if folder is not None:
        if folder_id is not None:
            raise ValueError("Cannot specify both folder and folder_id")
        # Add folder if not already present
        if not any(f.id == folder.id for f in catalog.folders):
            catalog.folders.append(folder)
        resolved_folder_id = folder.id
    elif folder_id is not None:
        resolved_folder_id = folder_id

    # Check if it's a partitioned Parquet directory
    if dataset_path.is_dir():
        add_parquet_directory(
            catalog,
            dataset_path,
            resolved_folder_id,
            infer_stats=infer_stats,
            quiet=q,
            start_time=start_time,
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
        return

    # It's a file
    suffix = dataset_path.suffix.lower()
    delivery_format = SUPPORTED_FORMATS.get(suffix)
    if delivery_format is None:
        raise ValueError(
            f"Unsupported format: {suffix}. "
            f"Supported: {', '.join(SUPPORTED_FORMATS.keys())}"
        )

    # Build dataset ID and name
    base_name = sanitize_id(dataset_path.stem)
    if resolved_folder_id:
        dataset_id = make_id(resolved_folder_id, base_name)
    else:
        dataset_id = base_name
    dataset_name = name or dataset_path.stem

    # Create dataset
    dataset = Dataset(
        id=dataset_id,
        name=dataset_name,
        folder_id=resolved_folder_id,
        data_path=str(dataset_path),
        last_update_date=get_mtime_iso(dataset_path),
        delivery_format=delivery_format,
        description=description,
        type=type,
        link=link,
        localisation=localisation,
        manager_id=manager_id,
        owner_id=owner_id,
        tag_ids=tag_ids or [],
        doc_ids=doc_ids or [],
        start_date=start_date,
        end_date=end_date,
        updating_each=updating_each,
        no_more_update=no_more_update,
    )
    catalog.datasets.append(dataset)

    # Resolve csv_encoding: parameter > catalog default
    resolved_encoding = (
        csv_encoding if csv_encoding is not None else catalog.csv_encoding
    )

    # Scan file
    result = scan_file(
        dataset_path,
        delivery_format,
        dataset_id=dataset_id,
        infer_stats=infer_stats,
        freq_threshold=catalog.freq_threshold if catalog.freq_threshold else None,
        csv_encoding=resolved_encoding,
    )
    dataset.nb_row = result.nb_row
    if result.description and not dataset.description:
        dataset.description = result.description
    var_id_mapping = build_variable_ids(result.variables, dataset.id)
    catalog.modality_manager.assign_from_freq(
        result.variables, result.freq_table, var_id_mapping
    )
    catalog.variables.extend(result.variables)

    # Log result
    var_count = sum(1 for v in catalog.variables if v.dataset_id == dataset.id)
    log_done(
        f"{dataset_path.name} ({dataset.nb_row:,} rows, {var_count} vars)",
        q,
        start_time,
    )


def add_parquet_directory(
    catalog: Catalog,
    dir_path: Path,
    folder_id: str | None,
    *,
    infer_stats: bool,
    quiet: bool,
    start_time: float,
    name: str | None,
    description: str | None,
    type: str | None,
    link: str | None,
    localisation: str | None,
    manager_id: str | None,
    owner_id: str | None,
    tag_ids: list[str] | None,
    doc_ids: list[str] | None,
    start_date: str | None,
    end_date: str | None,
    updating_each: str | None,
    no_more_update: str | None,
) -> None:
    """Add a partitioned Parquet directory (Delta, Hive, or Iceberg) to catalog."""
    # Detect dataset type
    if is_delta_table(dir_path):
        dataset_type = DatasetType.DELTA
        delivery_format = "delta"
    elif is_iceberg_table(dir_path):
        dataset_type = DatasetType.ICEBERG
        delivery_format = "iceberg"
    elif is_hive_partitioned(dir_path):
        dataset_type = DatasetType.HIVE
        delivery_format = "parquet"
    else:
        raise ValueError(
            f"Directory is not a recognized Parquet format "
            f"(Delta, Hive, or Iceberg): {dir_path}"
        )

    # Create ParquetDatasetInfo for scanning
    parquet_info = ParquetDatasetInfo(
        path=dir_path,
        type=dataset_type,
    )

    # Build dataset ID
    base_name = sanitize_id(dir_path.name)
    if folder_id:
        dataset_id = make_id(folder_id, base_name)
    else:
        dataset_id = base_name

    # Scan the dataset
    freq_threshold = catalog.freq_threshold if catalog.freq_threshold else None
    variables, nb_row, freq_table, metadata = scan_parquet_dataset(
        parquet_info,
        dataset_id=dataset_id,
        infer_stats=infer_stats,
        freq_threshold=freq_threshold,
    )

    # Build dataset name: user override > metadata > directory name
    dataset_name = name or metadata.name or dir_path.name

    # Use provided metadata or fall back to extracted metadata
    final_description = description if description is not None else metadata.description

    # Create dataset
    dataset = Dataset(
        id=dataset_id,
        name=dataset_name,
        folder_id=folder_id,
        data_path=str(dir_path),
        last_update_date=get_mtime_iso(dir_path),
        delivery_format=delivery_format,
        nb_row=nb_row,
        description=final_description,
        type=type,
        link=link,
        localisation=localisation,
        manager_id=manager_id,
        owner_id=owner_id,
        tag_ids=tag_ids or [],
        doc_ids=doc_ids or [],
        start_date=start_date,
        end_date=end_date,
        updating_each=updating_each,
        no_more_update=no_more_update,
    )
    catalog.datasets.append(dataset)

    # Finalize variables and modalities
    var_id_mapping = build_variable_ids(variables, dataset.id)
    catalog.modality_manager.assign_from_freq(variables, freq_table, var_id_mapping)
    catalog.variables.extend(variables)

    if not quiet:
        log_done(dataset_id, quiet=False, start_time=start_time)
