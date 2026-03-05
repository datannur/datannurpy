"""Modality management for catalog."""

from __future__ import annotations

from typing import TYPE_CHECKING

import ibis
import pyarrow as pa

from .ids import (
    MODALITIES_FOLDER_ID,
    build_modality_name,
    build_value_id,
    compute_modality_hash,
    make_id,
)
from ..schema import Folder, Modality, Value, Variable

if TYPE_CHECKING:
    from ..catalog import Catalog


class ModalityManager:
    """Manages modalities, values, and frequency tables."""

    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog
        self._modality_index: dict[frozenset[str], str] = {}

    def rebuild_index(self) -> None:
        """Rebuild modality index from existing values (after loading from db)."""
        # Group values by modality_id
        values_by_modality: dict[str, set[str]] = {}
        for v in self._catalog.value.all():
            if v.modality_id not in values_by_modality:
                values_by_modality[v.modality_id] = set()
            if v.value is not None:
                values_by_modality[v.modality_id].add(v.value)

        # Build index: frozenset(values) -> modality_id
        for modality_id, vals in values_by_modality.items():
            self._modality_index[frozenset(vals)] = modality_id

    def ensure_modalities_folder(self) -> None:
        """Create the _modalities folder if not already present."""
        if self._catalog.folder.exists(MODALITIES_FOLDER_ID):
            self._catalog.folder.update(MODALITIES_FOLDER_ID, _seen=True)
            return
        folder = Folder(id=MODALITIES_FOLDER_ID, name="Modalities", _seen=True)
        self._catalog.folder.add(folder)

    def get_or_create(self, values: set[str]) -> str:
        """Get existing modality or create new one for the given values."""
        signature = frozenset(values)

        if signature in self._modality_index:
            modality_id = self._modality_index[signature]
            # Mark existing modality as seen for incremental scan
            modality = self._catalog.modality.get(modality_id)
            if modality is not None:
                self._catalog.modality.update(modality_id, _seen=True)
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
            self._catalog.value.add(
                Value(
                    id=build_value_id(modality_id, val),
                    modality_id=modality_id,
                    value=val,
                )
            )

        self._modality_index[signature] = modality_id
        return modality_id

    def assign_from_freq(
        self,
        variables: list[Variable],
        freq_table: pa.Table | None,
        var_id_mapping: dict[str, str],
    ) -> None:
        """Assign modalities to variables from freq table and store it."""
        if freq_table is None:
            return

        # Parse freq table to extract values by variable
        freq_by_var: dict[str, set[str]] = {}
        for row in freq_table.to_pylist():
            col_name = row["variable_id"]
            val = row["value"]
            if col_name not in freq_by_var:
                freq_by_var[col_name] = set()
            if val is not None:
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
        """Update freq table with final variable IDs and store it."""
        # Convert to Ibis for transformation, then back to PyArrow
        ibis_table = ibis.memtable(freq_table)
        cases_list = [
            (ibis_table["variable_id"] == old_id, new_id)
            for old_id, new_id in var_id_mapping.items()
        ]
        case_expr = ibis.cases(*cases_list, else_=ibis_table["variable_id"])
        ibis_table = ibis_table.mutate(variable_id=case_expr)
        self._catalog._freq_tables.append(ibis_table.to_pyarrow())
