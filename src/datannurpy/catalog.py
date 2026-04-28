"""Catalog for managing datasets and variables."""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import polars as pl

from .add_database import add_database
from .add_dataset import add_dataset
from .add_folder import add_folder
from .exporter import export_app, export_db
from .finalize import finalize
from .schema import Config, DatannurDB
from .utils import EnumerationManager, configure_logging
from .utils.ids import compute_runtime_ids
from .utils.params import validate_params

if TYPE_CHECKING:
    from .add_metadata import LoadedDatasetRef

Depth = Literal["dataset", "variable", "stat", "value"]


class Catalog(DatannurDB):
    """A catalog containing folders, datasets and variables."""

    add_folder = add_folder
    add_dataset = add_dataset
    add_database = add_database
    export_app = export_app
    export_db = export_db
    finalize = finalize

    @validate_params
    def __init__(
        self,
        *,
        app_path: str | Path | None = None,
        metadata_path: str | Path | list[str | Path] | None = None,
        depth: Depth = "value",
        refresh: bool = False,
        freq_threshold: int = 100,
        csv_encoding: str | None = None,
        sample_size: int | None = 100_000,
        csv_skip_copy: bool = False,
        app_config: dict[str, str] | None = None,
        quiet: bool = False,
        verbose: bool = False,
        log_file: str | Path | None = None,
        _now: int | None = None,
    ) -> None:
        # Paths
        self.app_path = Path(app_path) if app_path is not None else None
        self.db_path = self.app_path / "data" / "db" if self.app_path else None

        # Load existing db if present (skip when refresh=True: full rescan)
        load_path: str | None = None
        if not refresh and self.db_path and self.db_path.exists():
            if (self.db_path / "__table__.json").exists():
                load_path = str(self.db_path)

        try:
            super().__init__(load_path)
        except Exception:
            if load_path is not None:
                import warnings

                warnings.warn(
                    f"Could not load existing database at {load_path}. "
                    "The schema may have changed after a datannurpy upgrade. "
                    "Starting fresh (use refresh=True to avoid this warning).",
                    stacklevel=2,
                )
                load_path = None
                super().__init__(None)
            else:
                raise

        # Config
        self.depth: Depth = depth
        self.refresh = refresh
        self.freq_threshold = freq_threshold
        self.csv_encoding = csv_encoding
        self.sample_size = sample_size
        self.csv_skip_copy = csv_skip_copy
        self.quiet = quiet
        self.verbose = verbose
        self.log_file = log_file
        configure_logging(verbose=verbose, log_file=log_file)
        self._now = _now if _now is not None else int(time.time())

        # Populate config table
        self.config._df = self.config._df.clear()
        if app_config is not None:
            for key, val in app_config.items():
                self.config.add(Config(id=key, value=val))

        # Metadata
        self.metadata_path: str | Path | list[str | Path] | None = metadata_path
        self._metadata_applied = False
        self._loaded_metadata: list[dict[str, Any]] | None = None
        self._dataset_match_index: dict[str, LoadedDatasetRef] | None = None
        self._freq_hidden_ids: set[str] = set()
        if metadata_path is not None:
            from .add_metadata import load_metadata

            load_metadata(self, metadata_path)

        # State
        self._loaded_from_db = load_path is not None
        self._has_scanned = False
        self._finalized = False

        self.enumeration_manager = EnumerationManager(self)

        if not self._loaded_from_db:
            return

        # Dataset-only mode: clear variable-level tables
        if depth == "dataset":
            for t in [self.variable, self.enumeration, self.value, self.frequency]:
                t._df = t._df.clear()

        # Add _seen runtime column to trackable tables
        for table in [
            self.folder,
            self.dataset,
            self.enumeration,
            self.organization,
            self.tag,
            self.doc,
            self.concept,
        ]:
            if "_seen" in table.runtime_fields and not table.is_empty:
                table._df = table._df.with_columns(pl.lit(False).alias("_seen"))

        # Restore _match_path (runtime field, not persisted).
        # data_path is the default match key, then metadata-loaded dataset.csv
        # rows override it with their resolved absolute scan path before any
        # incremental discovery runs.
        if not self.dataset.is_empty:
            if "data_path" in self.dataset._df.columns:
                self.dataset._df = self.dataset._df.with_columns(
                    pl.col("data_path").alias("_match_path")
                )
            else:
                self.dataset._df = self.dataset._df.with_columns(
                    pl.lit(None, dtype=pl.String).alias("_match_path")
                )

            if metadata_path is not None and "id" in self.dataset._df.columns:
                from .add_metadata import _build_dataset_match_paths_by_id

                metadata_match_paths = _build_dataset_match_paths_by_id(
                    self._loaded_metadata
                )
                if metadata_match_paths:
                    self.dataset._df = self.dataset._df.with_columns(
                        pl.col("id")
                        .map_elements(
                            lambda dataset_id: metadata_match_paths.get(
                                str(dataset_id)
                            ),
                            return_dtype=pl.String,
                        )
                        .fill_null(pl.col("_match_path"))
                        .alias("_match_path")
                    )

        self.enumeration_manager.rebuild_index()

        # Compute runtime id columns
        self.value._df = compute_runtime_ids(
            self.value._df, ["enumeration_id", "value"]
        )
        self.frequency._df = compute_runtime_ids(
            self.frequency._df, ["variable_id", "value"]
        )

    def __repr__(self) -> str:
        return (
            f"Catalog(\n"
            f"  folders={self.folder.count},\n"
            f"  datasets={self.dataset.count},\n"
            f"  variables={self.variable.count},\n"
            f"  enumerations={self.enumeration.count},\n"
            f"  values={self.value.count},\n"
            f"  organizations={self.organization.count},\n"
            f"  tags={self.tag.count},\n"
            f"  docs={self.doc.count},\n"
            f"  concepts={self.concept.count}\n"
            f")"
        )
