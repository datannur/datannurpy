"""Catalog for managing datasets and variables."""

from __future__ import annotations

import pyarrow as pa

from .add_database import add_database
from .add_dataset import add_dataset
from .add_folder import add_folder
from .entities import Dataset, Folder, Modality, Value, Variable
from .exporter.app import export_app
from .exporter.db import export_db
from .utils import ModalityManager


class Catalog:
    """A catalog containing folders, datasets and variables."""

    add_folder = add_folder
    add_dataset = add_dataset
    add_database = add_database
    export_app = export_app
    export_db = export_db

    def __init__(
        self,
        *,
        freq_threshold: int = 100,
        csv_encoding: str | None = None,
        quiet: bool = False,
    ) -> None:
        self.folders: list[Folder] = []
        self.datasets: list[Dataset] = []
        self.variables: list[Variable] = []
        self.modalities: list[Modality] = []
        self.values: list[Value] = []
        self.freq_threshold = freq_threshold
        self.csv_encoding = csv_encoding
        self.quiet = quiet
        self._freq_tables: list[pa.Table] = []
        self.modality_manager = ModalityManager(self)

    def __repr__(self) -> str:
        return (
            f"Catalog(\n"
            f"  folders={len(self.folders)},\n"
            f"  datasets={len(self.datasets)},\n"
            f"  variables={len(self.variables)},\n"
            f"  modalities={len(self.modalities)},\n"
            f"  values={len(self.values)}\n"
            f")"
        )
