"""Catalog for managing datasets and variables."""

from __future__ import annotations

import time
from pathlib import Path

import pyarrow as pa

from .add_database import add_database
from .add_dataset import add_dataset
from .add_folder import add_folder
from .add_metadata import add_metadata
from .entities import Dataset, Doc, Folder, Institution, Modality, Tag, Value, Variable
from .exporter.app import export_app
from .exporter.db import export_db
from .finalize import finalize, mark_dataset_modalities_seen, remove_dataset_cascade
from .importer.db import load_db
from .utils import ModalityManager


class Catalog:
    """A catalog containing folders, datasets and variables."""

    add_folder = add_folder
    add_dataset = add_dataset
    add_database = add_database
    add_metadata = add_metadata
    export_app = export_app
    export_db = export_db
    finalize = finalize
    _remove_dataset_cascade = remove_dataset_cascade
    _mark_dataset_modalities_seen = mark_dataset_modalities_seen

    def _add_variables(self, variables: list[Variable], dataset_id: str) -> None:
        """Add variables and update the index."""
        self.variables.extend(variables)
        self._variables_by_dataset[dataset_id] = variables

    def _get_variable_count(self, dataset_id: str) -> int:
        """Get the number of variables for a dataset (O(1))."""
        return len(self._variables_by_dataset.get(dataset_id, []))

    def __init__(
        self,
        *,
        db_path: str | Path | None = None,
        refresh: bool = False,
        freq_threshold: int = 100,
        csv_encoding: str | None = None,
        quiet: bool = False,
        _now: int | None = None,
    ) -> None:
        self.db_path = Path(db_path) if db_path is not None else None
        self.refresh = refresh
        self._now = _now if _now is not None else int(time.time())
        self.folders: list[Folder] = []
        self.datasets: list[Dataset] = []
        self.variables: list[Variable] = []
        self.modalities: list[Modality] = []
        self.values: list[Value] = []
        self.institutions: list[Institution] = []
        self.tags: list[Tag] = []
        self.docs: list[Doc] = []
        self.freq_threshold = freq_threshold
        self.csv_encoding = csv_encoding
        self.quiet = quiet
        self._freq_tables: list[pa.Table] = []
        self.modality_manager = ModalityManager(self)

        # Index for incremental scan: data_path -> Dataset
        self._dataset_index: dict[str, Dataset] = {}

        # Index for quick variable lookup by dataset_id
        self._variables_by_dataset: dict[str, list[Variable]] = {}

        # Index for quick folder lookup by id
        self._folder_index: dict[str, Folder] = {}

        # Index for quick modality lookup by id
        self._modality_index: dict[str, Modality] = {}

        # Flag to track if finalize() has been called (idempotent)
        self._finalized: bool = False

        # Load existing catalog if db_path provided
        if self.db_path is not None:
            load_db(self.db_path, self)
            self._dataset_index = {
                ds.data_path: ds for ds in self.datasets if ds.data_path
            }
            self._variables_by_dataset = _build_variables_index(self.variables)
            self._folder_index = {f.id: f for f in self.folders}
            self._modality_index = {m.id: m for m in self.modalities}
            # Rebuild modality index from loaded values
            self.modality_manager.rebuild_index()

    def __repr__(self) -> str:
        return (
            f"Catalog(\n"
            f"  folders={len(self.folders)},\n"
            f"  datasets={len(self.datasets)},\n"
            f"  variables={len(self.variables)},\n"
            f"  modalities={len(self.modalities)},\n"
            f"  values={len(self.values)},\n"
            f"  institutions={len(self.institutions)},\n"
            f"  tags={len(self.tags)},\n"
            f"  docs={len(self.docs)}\n"
            f")"
        )


def _build_variables_index(
    variables: list[Variable],
) -> dict[str, list[Variable]]:
    """Build index of variables by dataset_id."""
    index: dict[str, list[Variable]] = {}
    for var in variables:
        if var.dataset_id not in index:
            index[var.dataset_id] = []
        index[var.dataset_id].append(var)
    return index
