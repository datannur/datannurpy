"""Modality management for catalog."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl
import pyarrow as pa

from .ids import (
    MODALITIES_FOLDER_ID,
    build_freq_id,
    build_modality_name,
    build_value_id,
    compute_modality_hash,
    make_id,
)
from ..schema import Folder, Freq, Modality, Value, Variable

if TYPE_CHECKING:
    from ..catalog import Catalog


class ModalityManager:
    """Manages modalities, values, and frequency tables."""

    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog
        self._modality_index: dict[frozenset[str], str] = {}

    def rebuild_index(self) -> None:
        """Rebuild modality index from existing values (after loading from db)."""
        df = self._catalog.value.df
        if df.is_empty():
            return

        # Group values by modality_id using Polars
        grouped = df.group_by("modality_id").agg(pl.col("value"))
        for row in grouped.iter_rows(named=True):
            modality_id = row["modality_id"]
            values = {v for v in row["value"] if v is not None}
            if values:
                self._modality_index[frozenset(values)] = modality_id

    def ensure_modalities_folder(self) -> None:
        """Create the _modalities folder if not already present."""
        if self._catalog.folder.exists(MODALITIES_FOLDER_ID):
            self._catalog.folder.update(MODALITIES_FOLDER_ID, _seen=True)
            return
        folder = Folder(id=MODALITIES_FOLDER_ID, name="Modalities", _seen=True)
        self._catalog.folder.add(folder)

    def mark_dataset_seen(self, dataset_id: str) -> None:
        """Mark all modalities referenced by a dataset's variables as seen."""
        dataset_vars = self._catalog.variable.having.dataset(dataset_id)
        referenced_modality_ids: set[str] = set()
        for var in dataset_vars:
            referenced_modality_ids.update(var.modality_ids)

        if not referenced_modality_ids:
            return

        self._catalog.modality.update_many(list(referenced_modality_ids), _seen=True)
        self.ensure_modalities_folder()

    def get_or_create(self, values: set[str]) -> str:
        """Get existing modality or create new one for the given values."""
        signature = frozenset(values)

        if signature in self._modality_index:
            modality_id = self._modality_index[signature]
            # Mark existing modality as seen for incremental scan
            modality = self._catalog.modality.get(modality_id)
            if modality is not None:
                self._catalog.modality.update(modality_id, _seen=True)
            # Also mark the _modalities folder as seen
            self.ensure_modalities_folder()
            return modality_id

        # Create new modality
        self.ensure_modalities_folder()

        hash_10 = compute_modality_hash(values)
        modality_id = make_id(MODALITIES_FOLDER_ID, f"mod_{hash_10}")

        modality = Modality(
            id=modality_id,
            folder_id=MODALITIES_FOLDER_ID,
            name=build_modality_name(values),
            _seen=True,
        )
        self._catalog.modality.add(modality)

        # Create values
        for val in sorted(values):
            value_id = build_value_id(modality_id, val)
            self._catalog.value.add(
                Value(
                    id=value_id,
                    modality_id=modality_id,
                    value=val,
                )
            )

        self._modality_index[signature] = modality_id
        return modality_id

    def assign_from_freq(
        self,
        variables: list[Variable],
        freq_table: pa.Table,
        var_id_mapping: dict[str, str],
    ) -> None:
        """Assign modalities to variables from freq table and store it."""
        # Parse freq table to extract values by variable
        freq_by_var: dict[str, set[str]] = {}
        for row in freq_table.to_pylist():
            col_name = row["variable_id"]
            val: str = row["value"]
            if col_name not in freq_by_var:
                freq_by_var[col_name] = set()
            freq_by_var[col_name].add(val)

        # Assign modalities to variables
        for var in variables:
            old_col_name = next(k for k, v in var_id_mapping.items() if v == var.id)
            if old_col_name in freq_by_var and freq_by_var[old_col_name]:
                modality_id = self.get_or_create(freq_by_var[old_col_name])
                var.modality_ids = [modality_id]

        # Store freq table with updated IDs
        self.store_freq_table(freq_table, var_id_mapping)

    def store_freq_table(
        self,
        freq_table: pa.Table,
        var_id_mapping: dict[str, str],
    ) -> None:
        """Convert freq table to Freq objects and add to catalog."""
        freqs: list[Freq] = []
        for row in freq_table.to_pylist():
            old_var_id: str = row["variable_id"]
            new_var_id = var_id_mapping.get(old_var_id, old_var_id)
            value: str | None = row["value"]
            freq_id = build_freq_id(new_var_id, value)
            freq = Freq(
                id=freq_id,
                variable_id=new_var_id,
                value=value,
                freq=int(row["freq"]),
            )
            freqs.append(freq)
        if freqs:
            self._catalog.freq.add_all(freqs)
