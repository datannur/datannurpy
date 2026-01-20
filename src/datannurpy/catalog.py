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

from ._ids import make_id, sanitize_id
from .entities import Dataset, Folder, Variable
from .readers._utils import (
    SUPPORTED_FORMATS,
    find_files,
    find_subdirs,
    get_mtime_iso,
)
from .readers.database import (
    BACKEND_FORMATS,
    connect,
    list_schemas,
    list_tables,
    scan_table,
)
from .writers.app import copy_app
from .writers.json import write_freq_json, write_json, write_table_registry


@dataclass
class Catalog:
    """A catalog containing folders, datasets and variables."""

    folders: list[Folder] = field(default_factory=list)
    datasets: list[Dataset] = field(default_factory=list)
    variables: list[Variable] = field(default_factory=list)
    freq_threshold: int = 100  # 0 = disabled
    _freq_tables: list[pa.Table] = field(default_factory=list, repr=False)

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
        from .readers.csv import scan_csv
        from .readers.excel import scan_excel

        scanner = scan_csv if dataset.delivery_format == "csv" else scan_excel
        file_vars, nb_row, freq_table = scanner(
            file_path, infer_stats=infer_stats, freq_threshold=freq_threshold
        )

        dataset.nb_row = nb_row
        self._finalize_variables(file_vars, dataset, freq_table)

    def _get_freq_table(self) -> pa.Table | None:
        """Get combined frequency table."""
        if not self._freq_tables:
            return None
        return pa.concat_tables(self._freq_tables)

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

        # Add root folder
        self.folders.append(folder)
        prefix = folder.id

        # Find files and subdirectories
        files = find_files(root, include, exclude, recursive)
        subdirs = find_subdirs(root, files)

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
                data_path=str(subdir),
                last_update_date=get_mtime_iso(subdir),
            )
            self.folders.append(sub_folder)
            subdir_ids[subdir] = folder_id

        # Process files
        freq_threshold = self.freq_threshold if self.freq_threshold else None

        for file_path in sorted(files):
            # Determine folder_id for this file
            parent_dir = file_path.parent
            if parent_dir == root:
                folder_id = prefix
            else:
                folder_id = subdir_ids.get(parent_dir, prefix)

            # Build dataset ID
            rel_path = file_path.relative_to(root)
            path_parts = [sanitize_id(p) for p in rel_path.parts]
            dataset_id = make_id(prefix, *path_parts)

            # Get format info
            suffix = file_path.suffix.lower()
            delivery_format = SUPPORTED_FORMATS.get(suffix)
            if delivery_format is None:
                continue

            # Create dataset
            dataset = Dataset(
                id=dataset_id,
                name=file_path.stem,
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
    ) -> None:
        """Scan a database and add its tables to the catalog."""
        # Connect to database
        con, backend_name = connect(connection)
        delivery_format = BACKEND_FORMATS.get(backend_name, backend_name)

        # Determine database name for folder
        db_name = self._get_database_name(connection, con, backend_name)

        # Get timestamp for folder/dataset
        now_iso = datetime.now(tz=timezone.utc).strftime("%Y/%m/%d")

        # Determine schemas to scan
        schemas_to_scan: list[str | None]
        if schema is not None:
            schemas_to_scan = [schema]
        elif backend_name in ("postgres", "mysql"):
            # For postgres/mysql, scan all schemas (or just public)
            available_schemas = list_schemas(con)
            # Filter out system schemas
            system_schemas = {
                "information_schema",
                "pg_catalog",
                "pg_toast",
                "mysql",
                "performance_schema",
                "sys",
            }
            schemas_to_scan = [
                s for s in available_schemas if s not in system_schemas
            ] or [None]
        else:
            # SQLite doesn't have schemas
            schemas_to_scan = [None]

        # Create root folder for database
        if folder is None:
            root_folder_id = sanitize_id(db_name)
            folder = Folder(id=root_folder_id, name=db_name)
        else:
            root_folder_id = folder.id

        folder.last_update_date = now_iso
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
                    last_update_date=now_iso,
                )
                self.folders.append(schema_folder)
                current_folder_id = schema_folder_id
            else:
                current_folder_id = root_folder_id

            # Get tables
            tables = list_tables(con, schema_name, include, exclude, backend_name)

            for table_name in tables:
                # Build dataset ID
                dataset_id = make_id(current_folder_id, sanitize_id(table_name))

                # Create dataset
                dataset = Dataset(
                    id=dataset_id,
                    name=table_name,
                    folder_id=current_folder_id,
                    delivery_format=delivery_format,
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

    def _get_database_name(
        self,
        connection: str | ibis.BaseBackend,
        con: ibis.BaseBackend,
        backend_name: str,
    ) -> str:
        """Extract database name from connection."""
        if isinstance(connection, str):
            from urllib.parse import urlparse

            parsed = urlparse(connection)
            if backend_name == "sqlite":
                # Use filename without extension
                path = parsed.netloc + parsed.path if parsed.netloc else parsed.path
                return Path(path).stem or "sqlite"
            else:
                # Use database name from path
                return parsed.path.lstrip("/") or backend_name
        else:
            # Try to get name from connection object
            db_attr = getattr(con, "database", None)
            if db_attr:
                return str(db_attr)
            return backend_name

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
        """Add a single dataset file to the catalog."""
        file_path = Path(path).resolve()

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        if not file_path.is_file():
            raise ValueError(f"Not a file: {file_path}")

        suffix = file_path.suffix.lower()
        delivery_format = SUPPORTED_FORMATS.get(suffix)
        if delivery_format is None:
            raise ValueError(
                f"Unsupported format: {suffix}. "
                f"Supported: {', '.join(SUPPORTED_FORMATS.keys())}"
            )

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

        # Build dataset ID
        if id is not None:
            dataset_id = id
        elif resolved_folder_id:
            dataset_id = make_id(resolved_folder_id, sanitize_id(file_path.stem))
        else:
            dataset_id = sanitize_id(file_path.stem)

        # Create dataset
        dataset = Dataset(
            id=dataset_id,
            name=name or file_path.stem,
            folder_id=resolved_folder_id,
            data_path=str(file_path),
            last_update_date=get_mtime_iso(file_path),
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
            file_path,
            dataset,
            infer_stats=infer_stats,
            freq_threshold=self.freq_threshold if self.freq_threshold else None,
        )

    def write(
        self,
        output_dir: str | Path,
        *,
        write_js: bool = True,
    ) -> None:
        """Export catalog to JSON files."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        tables: list[str] = []

        if self.folders:
            write_json(self.folders, "folder", output_dir, write_js=write_js)
            tables.append("folder")

        if self.datasets:
            write_json(self.datasets, "dataset", output_dir, write_js=write_js)
            tables.append("dataset")

        if self.variables:
            write_json(self.variables, "variable", output_dir, write_js=write_js)
            tables.append("variable")

        freq_table = self._get_freq_table()
        if freq_table is not None and len(freq_table) > 0:
            write_freq_json(freq_table, output_dir, write_js=write_js)
            tables.append("freq")

        if tables:
            write_table_registry(output_dir, tables, write_js=write_js)

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
            f"variables={len(self.variables)})"
        )
