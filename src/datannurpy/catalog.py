"""Catalog for managing datasets and variables."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Literal

import polars as pl

from .add_database import add_database
from .add_dataset import add_dataset
from .add_folder import add_folder
from .add_metadata import add_metadata
from .exporter.app import export_app
from .finalize import finalize, mark_dataset_modalities_seen, remove_dataset_cascade
from .schema import DatannurDB, Dataset, Table, Variable
from .utils import ModalityManager


class Catalog(DatannurDB):
    """A catalog containing folders, datasets and variables."""

    add_folder = add_folder
    add_dataset = add_dataset
    add_database = add_database
    add_metadata = add_metadata
    export_app = export_app
    finalize = finalize
    _remove_dataset_cascade = remove_dataset_cascade
    _mark_dataset_modalities_seen = mark_dataset_modalities_seen

    def _add_variables(self, variables: list[Variable], dataset_id: str) -> None:
        """Add variables to the catalog."""
        self.variable.add_all(variables)

    def _get_variable_count(self, dataset_id: str) -> int:
        """Get the number of variables for a dataset."""
        return len(self.variable.having.dataset(dataset_id))

    def _get_dataset_by_path(self, data_path: str) -> Dataset | None:
        """Get dataset by data_path (for incremental scan)."""
        results = self.dataset.where("data_path", "==", data_path)
        return results[0] if results else None

    @staticmethod
    def _compute_runtime_id(table: Table, cols: list[str]) -> None:
        """Compute runtime id column by concatenating cols with '---'."""
        if table.is_empty or "id" in table.df.columns:
            return
        expr = pl.col(cols[0])
        for col in cols[1:]:
            expr = expr + "---" + pl.col(col).fill_null("_null_")
        table._df = table._df.with_columns(expr.alias("id"))

    def __init__(
        self,
        *,
        app_path: str | Path | None = None,
        depth: Literal["structure", "schema", "full"] = "full",
        refresh: bool = False,
        freq_threshold: int = 100,
        csv_encoding: str | None = None,
        quiet: bool = False,
        _now: int | None = None,
    ) -> None:
        # Derive db_path from app_path (db is stored in app_path/data/db/)
        self.app_path = Path(app_path) if app_path is not None else None
        self.db_path = self.app_path / "data" / "db" if self.app_path else None

        # Only pass path to DatannurDB if it exists (otherwise create empty)
        load_path: str | None = None
        if self.db_path is not None and self.db_path.exists():
            table_index = self.db_path / "__table__.json"
            if table_index.exists():
                load_path = str(self.db_path)

        # Initialize DatannurDB (loads existing data if path provided and exists)
        super().__init__(load_path)
        self.depth = depth
        self.refresh = refresh
        self._now = _now if _now is not None else int(time.time())
        self.freq_threshold = freq_threshold
        self.csv_encoding = csv_encoding
        self.quiet = quiet
        self.modality_manager = ModalityManager(self)

        # Flag to track if finalize() has been called (idempotent)
        self._finalized: bool = False

        # Track whether data was loaded from existing db (for finalize cleanup)
        self._loaded_from_db: bool = load_path is not None

        # Track whether a scan (add_folder/add_dataset/add_database) was performed
        # Only run finalize cleanup if scanning was done
        self._has_scanned: bool = False

        # Structure mode: clear variable/modality/value/freq tables (not needed)
        if depth == "structure" and self._loaded_from_db:
            self.variable._df = self.variable._df.clear()
            self.modality._df = self.modality._df.clear()
            self.value._df = self.value._df.clear()
            self.freq._df = self.freq._df.clear()

        # Add _seen column to tables that have it as runtime field (defaults to False)
        # Only add if the table has data (not empty)
        if self._loaded_from_db:
            for table in [
                self.folder,
                self.dataset,
                self.modality,
                self.institution,
                self.tag,
                self.doc,
            ]:
                if (
                    "_seen" in table.runtime_fields
                    and not table.is_empty
                    and "_seen" not in table.df.columns
                ):
                    table._df = table._df.with_columns(pl.lit(False).alias("_seen"))
            # Rebuild modality index from loaded values
            self.modality_manager.rebuild_index()

            # Compute runtime id columns (not persisted)
            self._compute_runtime_id(self.value, ["modality_id", "value"])
            self._compute_runtime_id(self.freq, ["variable_id", "value"])

    def export_db(
        self,
        output_dir: str | Path | None = None,
        *,
        track_evolution: bool = True,
        quiet: bool | None = None,
    ) -> None:
        """Write all catalog entities to JSON files."""
        # Only finalize (cleanup unseen entities) if a scan was performed
        if self._has_scanned:
            self.finalize()

        path = output_dir or self.db_path
        if path is None:
            msg = "output_dir is required when app_path was not set at init"
            raise ValueError(msg)

        # Parent relations for cascade suppression in evolution tracking
        parent_relations = {
            "dataset": "folder",
            "variable": "dataset",
            "freq": "variable",
            "value": "modality",
        }
        self.save(
            path,
            track_evolution=track_evolution,
            timestamp=self._now,
            parent_relations=parent_relations,
        )

    def __repr__(self) -> str:
        return (
            f"Catalog(\n"
            f"  folders={self.folder.count},\n"
            f"  datasets={self.dataset.count},\n"
            f"  variables={self.variable.count},\n"
            f"  modalities={self.modality.count},\n"
            f"  values={self.value.count},\n"
            f"  institutions={self.institution.count},\n"
            f"  tags={self.tag.count},\n"
            f"  docs={self.doc.count}\n"
            f")"
        )
