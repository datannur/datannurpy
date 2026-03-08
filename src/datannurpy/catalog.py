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
from .exporter import export_app, export_db
from .finalize import finalize
from .schema import DatannurDB
from .utils import ModalityManager
from .utils.ids import compute_runtime_ids


class Catalog(DatannurDB):
    """A catalog containing folders, datasets and variables."""

    add_folder = add_folder
    add_dataset = add_dataset
    add_database = add_database
    add_metadata = add_metadata
    export_app = export_app
    export_db = export_db
    finalize = finalize

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
        # Paths
        self.app_path = Path(app_path) if app_path is not None else None
        self.db_path = self.app_path / "data" / "db" if self.app_path else None

        # Load existing db if present
        load_path: str | None = None
        if self.db_path and self.db_path.exists():
            if (self.db_path / "__table__.json").exists():
                load_path = str(self.db_path)

        super().__init__(load_path)

        # Config
        self.depth = depth
        self.refresh = refresh
        self.freq_threshold = freq_threshold
        self.csv_encoding = csv_encoding
        self.quiet = quiet
        self._now = _now if _now is not None else int(time.time())

        # State
        self._loaded_from_db = load_path is not None
        self._has_scanned = False
        self._finalized = False

        self.modality_manager = ModalityManager(self)

        if not self._loaded_from_db:
            return

        # Structure mode: clear schema-related tables
        if depth == "structure":
            for t in [self.variable, self.modality, self.value, self.freq]:
                t._df = t._df.clear()

        # Add _seen runtime column to trackable tables
        for table in [
            self.folder,
            self.dataset,
            self.modality,
            self.institution,
            self.tag,
            self.doc,
        ]:
            if "_seen" in table.runtime_fields and not table.is_empty:
                table._df = table._df.with_columns(pl.lit(False).alias("_seen"))

        self.modality_manager.rebuild_index()

        # Compute runtime id columns
        self.value._df = compute_runtime_ids(self.value._df, ["modality_id", "value"])
        self.freq._df = compute_runtime_ids(self.freq._df, ["variable_id", "value"])

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
