"""Catalog for managing datasets and variables."""

from __future__ import annotations

import shutil
import webbrowser
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from ._ids import make_id, sanitize_id
from .entities import Dataset, Folder, Variable
from .readers._utils import (
    SUPPORTED_FORMATS,
    find_files,
    find_subdirs,
    get_mtime_iso,
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
    _freq_df: pl.DataFrame | None = field(default=None, repr=False)

    def _process_file(
        self,
        file_path: Path,
        dataset: Dataset,
        *,
        infer_stats: bool,
        freq_threshold: int | None,
    ) -> pl.DataFrame | None:
        """Scan file, update dataset with row count, add variables. Returns freq_df."""
        from .readers.csv import scan_csv
        from .readers.excel import scan_excel

        scanner = scan_csv if dataset.delivery_format == "csv" else scan_excel
        file_vars, nb_row, freq_df = scanner(
            file_path, infer_stats=infer_stats, freq_threshold=freq_threshold
        )

        dataset.nb_row = nb_row

        # Build final variable IDs
        var_id_mapping: dict[str, str] = {}
        for var in file_vars:
            old_col_name = var.id
            var.dataset_id = dataset.id
            var.id = make_id(dataset.id, sanitize_id(var.name or old_col_name))
            var_id_mapping[old_col_name] = var.id

        # Update freq DataFrame with final variable IDs
        if freq_df is not None:
            freq_df = freq_df.with_columns(
                pl.col("variable_id").replace(var_id_mapping)
            )

        self.variables.extend(file_vars)
        return freq_df

    def _accumulate_freq(self, freq_df: pl.DataFrame | None) -> None:
        """Accumulate frequency DataFrame."""
        if freq_df is None:
            return
        if self._freq_df is None:
            self._freq_df = freq_df
        else:
            self._freq_df = pl.concat([self._freq_df, freq_df])

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
        freq_threshold = self.freq_threshold or None

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

            freq_df = self._process_file(
                file_path,
                dataset,
                infer_stats=infer_stats,
                freq_threshold=freq_threshold,
            )
            self._accumulate_freq(freq_df)

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

        freq_df = self._process_file(
            file_path,
            dataset,
            infer_stats=infer_stats,
            freq_threshold=self.freq_threshold or None,
        )
        self._accumulate_freq(freq_df)

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

        if self._freq_df is not None and len(self._freq_df) > 0:
            write_freq_json(self._freq_df, output_dir, write_js=write_js)
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
