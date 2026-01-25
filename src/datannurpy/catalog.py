"""Catalog for managing datasets and variables."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import ibis
import pyarrow as pa

from ._add_database import add_database as _add_database
from ._add_dataset import add_dataset as _add_dataset
from ._add_folder import add_folder as _add_folder
from ._ids import make_id, sanitize_id
from ._modality import ModalityManager
from .entities import Dataset, Folder, Modality, Value, Variable
from .readers.csv import scan_csv
from .readers.excel import scan_excel
from .readers.parquet import scan_parquet
from .readers.statistical import scan_statistical
from .writers.app import export_app as _export_app
from .writers.json import write_catalog as _write_catalog


@dataclass
class Catalog:
    """A catalog containing folders, datasets and variables."""

    folders: list[Folder] = field(default_factory=list)
    datasets: list[Dataset] = field(default_factory=list)
    variables: list[Variable] = field(default_factory=list)
    modalities: list[Modality] = field(default_factory=list)
    values: list[Value] = field(default_factory=list)
    freq_threshold: int = 100  # 0 = disabled
    csv_encoding: str | None = None  # Priority encoding for CSV (e.g., 'CP1252')
    quiet: bool = False  # Suppress progress output
    _freq_tables: list[pa.Table] = field(default_factory=list, repr=False)
    _modality_manager: ModalityManager | None = field(default=None, repr=False)

    add_folder = _add_folder
    add_dataset = _add_dataset
    add_database = _add_database
    write = _write_catalog
    export_app = _export_app

    def __post_init__(self) -> None:
        """Initialize the modality manager."""
        self._modality_manager = ModalityManager(self)

    def _finalize_variables(
        self,
        variables: list[Variable],
        dataset: Dataset,
        freq_table: ibis.Table | None,
    ) -> None:
        """Finalize variable IDs, assign modalities, and add to catalog."""
        assert self._modality_manager is not None

        # Build final variable IDs
        var_id_mapping: dict[str, str] = {}
        for var in variables:
            var.dataset_id = dataset.id
            var.id = make_id(dataset.id, sanitize_id(var.name))
            var_id_mapping[var.name] = var.id

        # Assign modalities from freq table
        self._modality_manager.assign_from_freq(variables, freq_table, var_id_mapping)

        self.variables.extend(variables)

    def _process_file(
        self,
        file_path: Path,
        dataset: Dataset,
        *,
        infer_stats: bool,
        freq_threshold: int | None,
        csv_encoding: str | None = None,
    ) -> None:
        """Scan file, update dataset with row count, add variables."""
        if dataset.delivery_format == "parquet":
            file_vars, nb_row, freq_table, metadata = scan_parquet(
                file_path, infer_stats=infer_stats, freq_threshold=freq_threshold
            )
            # Apply Parquet metadata to dataset (if not already set)
            if metadata and metadata.description and not dataset.description:
                dataset.description = metadata.description
        elif dataset.delivery_format in ("sas", "spss", "stata"):
            file_vars, nb_row, freq_table, metadata = scan_statistical(
                file_path, infer_stats=infer_stats, freq_threshold=freq_threshold
            )
            # Apply statistical file metadata to dataset (if not already set)
            if metadata and metadata.description and not dataset.description:
                dataset.description = metadata.description
        else:
            assert dataset.delivery_format is not None
            if dataset.delivery_format == "csv":
                file_vars, nb_row, freq_table = scan_csv(
                    file_path,
                    infer_stats=infer_stats,
                    freq_threshold=freq_threshold,
                    csv_encoding=csv_encoding,
                )
            else:
                file_vars, nb_row, freq_table = scan_excel(
                    file_path, infer_stats=infer_stats, freq_threshold=freq_threshold
                )

        dataset.nb_row = nb_row
        self._finalize_variables(file_vars, dataset, freq_table)

    def __len__(self) -> int:
        """Return number of datasets."""
        return len(self.datasets)

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
