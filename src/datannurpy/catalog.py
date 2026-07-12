"""Catalog for managing datasets and variables."""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, get_args

import polars as pl

from .add_database import add_database
from .add_dataset import add_dataset
from .add_folder import add_folder
from .add_geodatabase import add_geodatabase
from .errors import ConfigError
from .exporter import export_app, export_db
from .finalize import finalize
from .preview import PreviewRows, effective_preview_rows, validate_preview_rows
from .scan_cache import scan_cache_load_path
from .schema import Config, DatannurDB
from .utils import EnumerationManager, configure_logging
from .utils.ids import build_frequency_id, build_value_id, compute_runtime_ids
from .utils.params import validate_params
from .utils.schema_columns import ensure_schema_columns

if TYPE_CHECKING:
    from .add_metadata import LoadedDatasetRef

Depth = Literal["dataset", "variable", "stat", "value"]
OnScanError = Literal["warn", "fail"]
OnMetadataError = Literal["warn", "fail"]


def _normalize_metadata_paths(
    metadata_path: str | Path | list[str | Path] | None,
) -> list[str | Path]:
    """Return configured metadata paths as a list."""
    if metadata_path is None:
        return []
    if isinstance(metadata_path, list):
        return list(metadata_path)
    return [metadata_path]


def _effective_metadata_path(
    metadata_path: str | Path | list[str | Path] | None,
    app_path: Path | None,
) -> str | Path | list[str | Path] | None:
    """Append app_path/data/db-ui after configured metadata sources when present."""
    paths = _normalize_metadata_paths(metadata_path)
    if app_path is not None:
        db_ui_path = app_path / "data" / "db-ui"
        if db_ui_path.is_dir():
            resolved_db_ui = db_ui_path.resolve()
            local_paths = [
                Path(path).resolve() for path in paths if "://" not in str(path)
            ]
            if resolved_db_ui not in local_paths:
                paths.append(db_ui_path)
    if not paths:
        return None
    if len(paths) == 1:
        return paths[0]
    return paths


class Catalog(DatannurDB):
    """A catalog containing folders, datasets and variables."""

    add_folder = add_folder
    add_dataset = add_dataset
    add_database = add_database
    add_geodatabase = add_geodatabase
    export_app = export_app
    export_db = export_db
    finalize = finalize

    @validate_params
    def __init__(  # noqa: C901 — ratchet: refactor pending
        self,
        *,
        app_path: str | Path | None = None,
        output_dir: str | Path | None = None,
        metadata_path: str | Path | list[str | Path] | None = None,
        depth: Depth = "value",
        refresh: bool = False,
        on_scan_error: OnScanError = "warn",
        on_metadata_error: OnMetadataError = "warn",
        freq_threshold: int = 100,
        auto_enumerations: bool = True,
        csv_encoding: str | None = None,
        sample_size: int | None = 100_000,
        preview_rows: PreviewRows = 100,
        csv_skip_copy: bool = False,
        app_config: dict[str, str] | None = None,
        quiet: bool = False,
        verbose: bool = False,
        log_file: str | Path | None = None,
        _now: int | None = None,
    ) -> None:
        # Paths. The persistent db lives under app_path/data/db for app exports,
        # or directly at output_dir for db-only exports — either way db_path is
        # both the previous-state source (incremental scans) and export target.
        self.app_path = Path(app_path) if app_path is not None else None
        if self.app_path is not None:
            self.db_path: Path | None = self.app_path / "data" / "db"
        elif output_dir is not None:
            self.db_path = Path(output_dir)
        else:
            self.db_path = None

        # Load the scan-derived base if present (skip when refresh=True: full
        # rescan). The incremental base is the pristine _scan cache, never the
        # previously-exported final DB — that DB already carries metadata overlays
        # and reusing it as a base is what left stale values behind.
        load_path: str | None = None
        if not refresh:
            load_path = scan_cache_load_path(self.db_path)

        try:
            super().__init__(load_path)
        except Exception:
            if load_path is not None:
                import warnings

                warnings.warn(
                    f"Could not load the scan cache at {load_path}. "
                    "The schema may have changed after a datannurpy upgrade. "
                    "Starting fresh (use refresh=True to avoid this warning).",
                    stacklevel=2,
                )
                load_path = None
                super().__init__(None)
            else:
                raise

        # Ensure all schema columns exist on loaded tables
        if load_path is not None:
            for table in self._tables.values():
                table.df = ensure_schema_columns(
                    table.df, table._entity_type, skip=table.runtime_fields
                )

        # Config
        self.depth: Depth = depth
        self.refresh = refresh
        if on_scan_error not in get_args(OnScanError):
            raise ConfigError(
                f"on_scan_error must be 'warn' or 'fail', got {on_scan_error!r}"
            )
        self.on_scan_error: OnScanError = on_scan_error
        if on_metadata_error not in get_args(OnMetadataError):
            raise ConfigError(
                f"on_metadata_error must be 'warn' or 'fail', got {on_metadata_error!r}"
            )
        self.on_metadata_error: OnMetadataError = on_metadata_error
        self.freq_threshold = freq_threshold
        self.auto_enumerations = auto_enumerations
        self.csv_encoding = csv_encoding
        self.sample_size = sample_size
        self.preview_rows = validate_preview_rows(preview_rows, allow_none=False) or 0
        self.csv_skip_copy = csv_skip_copy
        self.quiet = quiet
        self.verbose = verbose
        self.log_file = log_file
        configure_logging(verbose=verbose, log_file=log_file)
        self._now = _now if _now is not None else int(time.time())

        # Populate config table
        self.config.df = self.config.df.clear()
        if app_config is not None:
            for key, val in app_config.items():
                self.config.add(Config(id=key, value=val))

        # Metadata
        self.metadata_path = _effective_metadata_path(metadata_path, self.app_path)
        self._metadata_applied = False
        self._loaded_metadata: list[dict[str, Any]] | None = None
        self._dataset_match_index: dict[str, LoadedDatasetRef] | None = None
        self._freq_hidden_ids: set[str] = set()
        self._metadata_tombstones: dict[str, set[str]] = {}
        self._dataset_previews: dict[str, pl.DataFrame] = {}
        self._dataset_preview_labels: dict[str, str] = {}
        if self.metadata_path is not None:
            from .add_metadata import load_metadata

            load_metadata(self, self.metadata_path)

        # State
        self._loaded_from_db = load_path is not None
        self._has_scanned = False
        self._finalized = False

        # Run-level scan tallies, aggregated across every add_* call so the
        # export can print one whole-run bilan (see log_run_summary).
        self._run_scanned = 0
        self._run_unchanged = 0
        self._run_errors = 0
        # Metadata loading is continue-on-error too: invalid tables are skipped
        # while valid ones still apply. This tallies skipped tables so the CLI
        # can fail the run under on_metadata_error="fail".
        self._metadata_errors = 0

        self.enumeration_manager = EnumerationManager(self)

        if not self._loaded_from_db:
            return

        # Dataset-only mode: clear variable-level tables
        if depth == "dataset":
            for t in [self.variable, self.enumeration, self.value, self.frequency]:
                t.df = t.df.clear()

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
                table.df = table.df.with_columns(pl.lit(False).alias("_seen"))

        # Restore _match_path (runtime field, not persisted).
        # data_path is the default match key, then metadata-loaded dataset.csv
        # rows override it with their resolved absolute scan path before any
        # incremental discovery runs.
        if not self.dataset.is_empty:
            self.dataset.df = self.dataset.df.with_columns(
                pl.col("data_path").alias("_match_path")
            )
            self.dataset.df = self.dataset.df.with_columns(
                pl.lit(effective_preview_rows(self.preview_rows, self.depth)).alias(
                    "preview_rows"
                )
            )

            if self.metadata_path is not None and "id" in self.dataset.df.columns:
                from .add_metadata import _build_dataset_match_paths_by_id

                metadata_match_paths = _build_dataset_match_paths_by_id(
                    self._loaded_metadata
                )
                if metadata_match_paths:
                    self.dataset.df = self.dataset.df.with_columns(
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
        self.value.df = compute_runtime_ids(
            self.value.df, ["enumeration_id", "value"], build_value_id
        )
        self.frequency.df = compute_runtime_ids(
            self.frequency.df, ["variable_id", "value"], build_frequency_id
        )

    def _tally_scan(self, scanned: int, unchanged: int, errors: int = 0) -> None:
        """Fold one ``add_*`` call's outcome into the run-level bilan totals."""
        self._run_scanned += scanned
        self._run_unchanged += unchanged
        self._run_errors += errors

    @property
    def run_errors(self) -> int:
        """Number of items (files/tables) that failed to scan this run.

        Scanning is continue-on-error, so a failed item is logged and skipped
        rather than aborting the run; this counter lets a caller (e.g. the CLI)
        surface partial failures — see ``on_scan_error``.
        """
        return self._run_errors

    @property
    def metadata_errors(self) -> int:
        """Number of metadata tables that failed validation this run.

        Metadata loading is continue-on-error: an invalid table is logged and
        skipped while valid tables are still applied, rather than discarding all
        curation. This counter lets a caller (e.g. the CLI) surface those
        failures — see ``on_metadata_error``.
        """
        return self._metadata_errors

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
