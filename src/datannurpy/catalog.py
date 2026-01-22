"""Catalog for managing datasets and variables."""

from __future__ import annotations

import shutil
import webbrowser
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import ibis
import pyarrow as pa

from ._ids import (
    MODALITIES_FOLDER_ID,
    build_modality_name,
    compute_modality_hash,
    make_id,
    sanitize_id,
)
from ._prefix import get_prefix_folders, get_table_prefix
from .entities import Dataset, Folder, Modality, Value, Variable
from .readers._utils import (
    SUPPORTED_FORMATS,
    find_files,
    find_subdirs,
    get_mtime_iso,
)
from .readers.csv import scan_csv
from .readers.excel import scan_excel
from .readers.sas import scan_sas
from .readers.parquet import (
    DatasetType,
    ParquetDatasetInfo,
    discover_parquet_datasets,
    scan_parquet,
    scan_parquet_dataset,
)
from .readers.parquet._discovery import (
    _is_delta_table,
    _is_hive_partitioned,
    _is_iceberg_table,
)
from .readers.database import (
    connect,
    get_database_name,
    get_database_path,
    get_schemas_to_scan,
    list_tables,
    scan_table,
)
from .writers.app import copy_app
from .writers.json import write_catalog


@dataclass
class Catalog:
    """A catalog containing folders, datasets and variables."""

    folders: list[Folder] = field(default_factory=list)
    datasets: list[Dataset] = field(default_factory=list)
    variables: list[Variable] = field(default_factory=list)
    modalities: list[Modality] = field(default_factory=list)
    values: list[Value] = field(default_factory=list)
    freq_threshold: int = 100  # 0 = disabled
    _freq_tables: list[pa.Table] = field(default_factory=list, repr=False)
    _modality_index: dict[frozenset[str], str] = field(
        default_factory=dict, repr=False
    )  # signature → modality_id

    def _ensure_modalities_folder(self) -> None:
        """Create the _modalities folder if not already present."""
        if not any(f.id == MODALITIES_FOLDER_ID for f in self.folders):
            self.folders.append(Folder(id=MODALITIES_FOLDER_ID, name="Modalities"))

    def _get_or_create_modality(self, values: set[str]) -> str:
        """Get existing modality or create new one for the given values."""
        signature = frozenset(values)

        if signature in self._modality_index:
            return self._modality_index[signature]

        # Create new modality
        self._ensure_modalities_folder()

        hash_10 = compute_modality_hash(values)
        modality_id = make_id(MODALITIES_FOLDER_ID, f"mod_{hash_10}")

        modality = Modality(
            id=modality_id,
            folder_id=MODALITIES_FOLDER_ID,
            name=build_modality_name(values),
        )
        self.modalities.append(modality)

        # Create values
        for val in sorted(values):
            self.values.append(Value(modality_id=modality_id, value=val))

        self._modality_index[signature] = modality_id
        return modality_id

    def _finalize_variables(
        self,
        variables: list[Variable],
        dataset: Dataset,
        freq_table: ibis.Table | None,
    ) -> None:
        """Finalize variable IDs and add to catalog."""
        # Build final variable IDs
        var_id_mapping: dict[str, str] = {}
        for var in variables:
            old_col_name = var.id
            var.dataset_id = dataset.id
            var.id = make_id(dataset.id, sanitize_id(var.name or old_col_name))
            var_id_mapping[old_col_name] = var.id

        # Build modalities from freq table (before updating variable_id column)
        freq_by_var: dict[str, set[str]] = {}
        if freq_table is not None:
            freq_data = freq_table.to_pyarrow().to_pylist()
            for row in freq_data:
                col_name = row["variable_id"]
                val = row["value"]
                if col_name not in freq_by_var:
                    freq_by_var[col_name] = set()
                if val is not None:
                    freq_by_var[col_name].add(val)

        # Assign modalities to variables
        for var in variables:
            old_col_name = next(k for k, v in var_id_mapping.items() if v == var.id)
            if old_col_name in freq_by_var and freq_by_var[old_col_name]:
                modality_id = self._get_or_create_modality(freq_by_var[old_col_name])
                var.modality_ids = [modality_id]

        # Update freq table with final variable IDs and materialize to PyArrow
        if freq_table is not None:
            cases_list = [
                (freq_table["variable_id"] == old_id, new_id)
                for old_id, new_id in var_id_mapping.items()
            ]
            case_expr = ibis.cases(*cases_list, else_=freq_table["variable_id"])
            freq_table = freq_table.mutate(variable_id=case_expr)
            self._freq_tables.append(freq_table.to_pyarrow())

        self.variables.extend(variables)

    def _process_file(
        self,
        file_path: Path,
        dataset: Dataset,
        *,
        infer_stats: bool,
        freq_threshold: int | None,
    ) -> None:
        """Scan file, update dataset with row count, add variables."""
        if dataset.delivery_format == "parquet":
            file_vars, nb_row, freq_table, metadata = scan_parquet(
                file_path, infer_stats=infer_stats, freq_threshold=freq_threshold
            )
            # Apply Parquet metadata to dataset (if not already set)
            if metadata and metadata.description and not dataset.description:
                dataset.description = metadata.description
        elif dataset.delivery_format == "sas":
            file_vars, nb_row, freq_table, metadata = scan_sas(
                file_path, infer_stats=infer_stats, freq_threshold=freq_threshold
            )
            # Apply SAS metadata to dataset (if not already set)
            if metadata and metadata.description and not dataset.description:
                dataset.description = metadata.description
        else:
            assert dataset.delivery_format is not None
            scanners = {"csv": scan_csv, "excel": scan_excel}
            scanner = scanners[dataset.delivery_format]
            file_vars, nb_row, freq_table = scanner(
                file_path, infer_stats=infer_stats, freq_threshold=freq_threshold
            )

        dataset.nb_row = nb_row
        self._finalize_variables(file_vars, dataset, freq_table)

    def _get_folder_id(
        self,
        path: Path,
        root: Path,
        prefix: str,
        subdir_ids: dict[Path, str],
    ) -> str:
        """Determine folder_id for a file or directory."""
        parent_dir = path.parent
        if parent_dir == root:
            return prefix
        return subdir_ids.get(parent_dir, prefix)

    def _build_dataset_id_name(
        self,
        path: Path,
        root: Path,
        prefix: str,
    ) -> tuple[str, str]:
        """Build dataset ID and name from path."""
        rel_path = path.relative_to(root)
        if path.is_file():
            path_parts = [sanitize_id(p) for p in rel_path.parts]
            return make_id(prefix, *path_parts), path.stem
        # Directory (Delta/Hive)
        if rel_path == Path("."):
            return prefix, path.name
        path_parts = [sanitize_id(p) for p in rel_path.parts]
        return make_id(prefix, *path_parts), path.name

    def add_folder(
        self,
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
        self.folders.append(folder)
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
            self.folders.append(sub_folder)
            subdir_ids[subdir] = folder_id

        freq_threshold = self.freq_threshold if self.freq_threshold else None

        # Process Parquet datasets (simple files, Delta tables, Hive partitioned)
        for info in parquet_result.datasets:
            dataset_id, dataset_name = self._build_dataset_id_name(
                info.path, root, prefix
            )
            folder_id = self._get_folder_id(info.path, root, prefix, subdir_ids)

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
            self.datasets.append(dataset)

            dataset.nb_row = nb_row
            self._finalize_variables(variables, dataset, freq_table)

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

            dataset_id, dataset_name = self._build_dataset_id_name(
                file_path, root, prefix
            )
            folder_id = self._get_folder_id(file_path, root, prefix, subdir_ids)

            # Create dataset
            dataset = Dataset(
                id=dataset_id,
                name=dataset_name,
                folder_id=folder_id,
                data_path=str(file_path),
                last_update_date=get_mtime_iso(file_path),
                delivery_format=delivery_format,
            )
            self.datasets.append(dataset)

            self._process_file(
                file_path,
                dataset,
                infer_stats=infer_stats,
                freq_threshold=freq_threshold,
            )

    def add_database(
        self,
        connection: str | ibis.BaseBackend,
        folder: Folder | None = None,
        *,
        schema: str | None = None,
        include: Sequence[str] | None = None,
        exclude: Sequence[str] | None = None,
        infer_stats: bool = True,
        sample_size: int | None = None,
        group_by_prefix: bool | str = True,
        prefix_min_tables: int = 2,
    ) -> None:
        """Scan a database and add its tables to the catalog."""
        # Connect to database
        con, backend_name = connect(connection)

        # Determine database name for folder
        db_name = get_database_name(connection, con, backend_name)

        # Get timestamp for folder/dataset
        now_iso = datetime.now(tz=timezone.utc).strftime("%Y/%m/%d")

        # Determine schemas to scan
        schemas_to_scan = get_schemas_to_scan(con, schema, backend_name)

        # Create root folder for database
        if folder is None:
            root_folder_id = sanitize_id(db_name)
            folder = Folder(id=root_folder_id, name=db_name)
        else:
            root_folder_id = folder.id

        folder.last_update_date = now_iso
        folder.data_path = get_database_path(connection, backend_name)
        folder.type = backend_name
        self.folders.append(folder)

        freq_threshold = self.freq_threshold if self.freq_threshold else None

        # Process each schema
        for schema_name in schemas_to_scan:
            # Determine folder for this schema
            if schema_name is not None and len(schemas_to_scan) > 1:
                # Multiple schemas: create sub-folder for each
                schema_folder_id = make_id(root_folder_id, sanitize_id(schema_name))
                schema_folder = Folder(
                    id=schema_folder_id,
                    name=schema_name,
                    parent_id=root_folder_id,
                    type="schema",
                    last_update_date=now_iso,
                )
                self.folders.append(schema_folder)
                current_folder_id = schema_folder_id
            else:
                current_folder_id = root_folder_id

            # Get tables
            tables = list_tables(con, schema_name, include, exclude, backend_name)

            # Group tables by prefix if enabled
            prefix_folder_ids: dict[str, str] = {}  # prefix → folder_id
            valid_prefixes: set[str] = set()
            prefix_sep = "_" if group_by_prefix is True else group_by_prefix or "_"

            if group_by_prefix:
                prefix_folders = get_prefix_folders(
                    tables, sep=prefix_sep, min_count=prefix_min_tables
                )
                valid_prefixes = {pf.prefix for pf in prefix_folders}

                # Create prefix folders
                for pf in prefix_folders:
                    if pf.parent_prefix is not None:
                        parent_id = prefix_folder_ids[pf.parent_prefix]
                    else:
                        parent_id = current_folder_id

                    folder_id = make_id(parent_id, sanitize_id(pf.prefix))
                    prefix_folder_ids[pf.prefix] = folder_id

                    prefix_folder = Folder(
                        id=folder_id,
                        name=pf.prefix,
                        parent_id=parent_id,
                        type="table_prefix",
                        last_update_date=now_iso,
                    )
                    self.folders.append(prefix_folder)

            for table_name in tables:
                # Determine folder for this table
                table_prefix: str | None = None
                if valid_prefixes:
                    table_prefix = get_table_prefix(
                        table_name, valid_prefixes, sep=prefix_sep
                    )

                if table_prefix:
                    table_folder_id = prefix_folder_ids[table_prefix]
                else:
                    table_folder_id = current_folder_id

                # Build dataset ID
                dataset_id = make_id(table_folder_id, sanitize_id(table_name))

                # Create dataset
                dataset = Dataset(
                    id=dataset_id,
                    name=table_name,
                    folder_id=table_folder_id,
                    delivery_format=backend_name,
                    last_update_date=now_iso,
                )

                # Scan table
                table_vars, nb_row, freq_table = scan_table(
                    con,
                    table_name,
                    schema=schema_name,
                    infer_stats=infer_stats,
                    freq_threshold=freq_threshold,
                    sample_size=sample_size,
                )

                dataset.nb_row = nb_row
                self.datasets.append(dataset)
                self._finalize_variables(table_vars, dataset, freq_table)

    def add_dataset(
        self,
        path: str | Path,
        folder: Folder | None = None,
        *,
        folder_id: str | None = None,
        infer_stats: bool = True,
        # Dataset metadata overrides
        id: str | None = None,
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
        dataset_path = Path(path).resolve()

        if not dataset_path.exists():
            raise FileNotFoundError(f"Path not found: {dataset_path}")

        # Handle folder
        resolved_folder_id: str | None = None
        if folder is not None:
            if folder_id is not None:
                raise ValueError("Cannot specify both folder and folder_id")
            # Add folder if not already present
            if not any(f.id == folder.id for f in self.folders):
                self.folders.append(folder)
            resolved_folder_id = folder.id
        elif folder_id is not None:
            resolved_folder_id = folder_id

        # Check if it's a partitioned Parquet directory
        if dataset_path.is_dir():
            self._add_parquet_directory(
                dataset_path,
                resolved_folder_id,
                infer_stats=infer_stats,
                id=id,
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
        if id is not None:
            dataset_id = id
            dataset_name = name or dataset_path.stem
        else:
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
        self.datasets.append(dataset)

        self._process_file(
            dataset_path,
            dataset,
            infer_stats=infer_stats,
            freq_threshold=self.freq_threshold if self.freq_threshold else None,
        )

    def _add_parquet_directory(
        self,
        dir_path: Path,
        folder_id: str | None,
        *,
        infer_stats: bool,
        id: str | None,
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
        if _is_delta_table(dir_path):
            dataset_type = DatasetType.DELTA
            delivery_format = "delta"
        elif _is_iceberg_table(dir_path):
            dataset_type = DatasetType.ICEBERG
            delivery_format = "iceberg"
        elif _is_hive_partitioned(dir_path):
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
        if id is not None:
            dataset_id = id
        else:
            base_name = sanitize_id(dir_path.name)
            if folder_id:
                dataset_id = make_id(folder_id, base_name)
            else:
                dataset_id = base_name

        # Scan the dataset
        freq_threshold = self.freq_threshold if self.freq_threshold else None
        variables, nb_row, freq_table, metadata = scan_parquet_dataset(
            parquet_info,
            dataset_id=dataset_id,
            infer_stats=infer_stats,
            freq_threshold=freq_threshold,
        )

        # Build dataset name: user override > metadata > directory name
        dataset_name = name or metadata.name or dir_path.name

        # Use provided metadata or fall back to extracted metadata
        final_description = (
            description if description is not None else metadata.description
        )

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
        self.datasets.append(dataset)

        # Finalize variables and modalities
        self._finalize_variables(variables, dataset, freq_table)

    def write(
        self,
        output_dir: str | Path,
        *,
        write_js: bool = True,
    ) -> None:
        """Export catalog to JSON files."""
        write_catalog(
            output_dir,
            folders=self.folders,
            datasets=self.datasets,
            variables=self.variables,
            modalities=self.modalities,
            values=self.values,
            freq_tables=self._freq_tables,
            write_js=write_js,
        )

    def export_app(
        self,
        output_dir: str | Path,
        *,
        open_browser: bool = False,
    ) -> None:
        """Export a standalone datannur visualization app with catalog data."""
        output_dir = Path(output_dir)

        # Copy app files
        copy_app(output_dir)

        # Clear and write to data/db/
        db_dir = output_dir / "data" / "db"
        if db_dir.exists():
            shutil.rmtree(db_dir)

        self.write(db_dir)

        if open_browser:
            index_path = output_dir / "index.html"
            webbrowser.open(index_path.as_uri())

    def __len__(self) -> int:
        """Return number of datasets."""
        return len(self.datasets)

    def __repr__(self) -> str:
        return (
            f"Catalog(folders={len(self.folders)}, "
            f"datasets={len(self.datasets)}, "
            f"variables={len(self.variables)}, "
            f"modalities={len(self.modalities)})"
        )
