"""Enumeration management for catalog."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl
import pyarrow as pa

from .ids import (
    ENUMERATIONS_FOLDER_ID,
    build_frequency_id,
    build_enumeration_name,
    build_value_id,
    compute_enumeration_hash,
    make_id,
)
from ..schema import Enumeration, Folder, Frequency, Value, Variable

if TYPE_CHECKING:
    from ..catalog import Catalog


class EnumerationManager:
    """Manages enumerations, values, and frequency tables."""

    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog
        self._enumeration_index: dict[frozenset[str], str] = {}

    def rebuild_index(self) -> None:
        """Rebuild enumeration index from existing values (after loading from db)."""
        df = self._catalog.value.df
        if df.is_empty():
            return

        # Group values by enumeration_id using Polars
        grouped = df.group_by("enumeration_id").agg(pl.col("value"))
        for row in grouped.iter_rows(named=True):
            enumeration_id = row["enumeration_id"]
            values = {v for v in row["value"] if v is not None}
            if values:
                self._enumeration_index[frozenset(values)] = enumeration_id

    def ensure_enumerations_folder(self) -> None:
        """Create the _enumerations folder if not already present."""
        if self._catalog.folder.exists(ENUMERATIONS_FOLDER_ID):
            self._catalog.folder.update(ENUMERATIONS_FOLDER_ID, _seen=True)
            return
        folder = Folder(id=ENUMERATIONS_FOLDER_ID, name="Enumerations", _seen=True)
        self._catalog.folder.add(folder)

    def mark_dataset_seen(self, dataset_id: str) -> None:
        """Mark all enumerations referenced by a dataset's variables as seen."""
        dataset_vars = self._catalog.variable.having.dataset(dataset_id)
        referenced_enumeration_ids: set[str] = set()
        for var in dataset_vars:
            referenced_enumeration_ids.update(var.enumeration_ids)

        if not referenced_enumeration_ids:
            return

        self._catalog.enumeration.update_many(
            list(referenced_enumeration_ids), _seen=True
        )
        self.ensure_enumerations_folder()

    def get_or_create(self, values: set[str]) -> str:
        """Get existing enumeration or create new one for the given values."""
        signature = frozenset(values)

        if signature in self._enumeration_index:
            enumeration_id = self._enumeration_index[signature]
            enumeration = self._catalog.enumeration.get(enumeration_id)
            if enumeration is not None:
                self._catalog.enumeration.update(enumeration_id, _seen=True)
            self.ensure_enumerations_folder()
            return enumeration_id

        self.ensure_enumerations_folder()

        hash_10 = compute_enumeration_hash(values)
        enumeration_id = make_id(ENUMERATIONS_FOLDER_ID, f"enum_{hash_10}")

        enumeration = Enumeration(
            id=enumeration_id,
            folder_id=ENUMERATIONS_FOLDER_ID,
            name=build_enumeration_name(values),
            _seen=True,
        )
        self._catalog.enumeration.add(enumeration)

        # Create values
        for val in sorted(values):
            value_id = build_value_id(enumeration_id, val)
            self._catalog.value.add(
                Value(
                    id=value_id,
                    enumeration_id=enumeration_id,
                    value=val,
                )
            )

        self._enumeration_index[signature] = enumeration_id
        return enumeration_id

    def assign_from_freq(
        self,
        variables: list[Variable],
        freq_table: pa.Table,
        var_id_mapping: dict[str, str],
    ) -> None:
        """Assign enumerations to variables from frequency table and store it."""
        # Determine columns to exclude (policy---frequency-hidden)
        hidden_ids = self._catalog._freq_hidden_ids
        hidden_cols = {
            col for col, var_id in var_id_mapping.items() if var_id in hidden_ids
        }

        # Parse frequency table to extract values by variable
        freq_by_var: dict[str, set[str]] = {}
        for row in freq_table.to_pylist():
            col_name = row["variable_id"]
            if col_name in hidden_cols:
                continue
            val: str = row["value"]
            if col_name not in freq_by_var:
                freq_by_var[col_name] = set()
            freq_by_var[col_name].add(val)

        # Resolve enumerations: batch new ones, collect existing to mark seen
        new_enumerations: list[Enumeration] = []
        new_values: list[Value] = []
        existing_seen_ids: set[str] = set()

        for var in variables:
            if var.is_pattern:
                continue
            old_col_name = next(k for k, v in var_id_mapping.items() if v == var.id)
            values = freq_by_var.get(old_col_name)
            if not values:
                continue

            signature = frozenset(values)

            if signature in self._enumeration_index:
                enumeration_id = self._enumeration_index[signature]
                existing_seen_ids.add(enumeration_id)
            else:
                hash_10 = compute_enumeration_hash(values)
                enumeration_id = make_id(ENUMERATIONS_FOLDER_ID, f"enum_{hash_10}")
                new_enumerations.append(
                    Enumeration(
                        id=enumeration_id,
                        folder_id=ENUMERATIONS_FOLDER_ID,
                        name=build_enumeration_name(values),
                        _seen=True,
                    )
                )
                for val in sorted(values):
                    new_values.append(
                        Value(
                            id=build_value_id(enumeration_id, val),
                            enumeration_id=enumeration_id,
                            value=val,
                        )
                    )
                self._enumeration_index[signature] = enumeration_id

            var.enumeration_ids = [enumeration_id]

        # Batch apply: one concat each instead of thousands
        if new_enumerations or existing_seen_ids:
            self.ensure_enumerations_folder()
        if existing_seen_ids:
            self._catalog.enumeration.update_many(list(existing_seen_ids), _seen=True)
        if new_enumerations:
            self._catalog.enumeration.add_all(new_enumerations)
        if new_values:
            self._catalog.value.add_all(new_values)

        # Store frequency table with updated IDs (excluding hidden vars)
        self.store_freq_table(freq_table, var_id_mapping, hidden_cols)

    def store_freq_table(
        self,
        freq_table: pa.Table,
        var_id_mapping: dict[str, str],
        exclude_cols: set[str] | None = None,
    ) -> None:
        """Convert frequency table to Frequency objects and add to catalog."""
        frequencies: list[Frequency] = []
        for row in freq_table.to_pylist():
            old_var_id: str = row["variable_id"]
            if exclude_cols and old_var_id in exclude_cols:
                continue
            new_var_id = var_id_mapping.get(old_var_id, old_var_id)
            value: str | None = row["value"]
            freq_id = build_frequency_id(new_var_id, value)
            frequency_value = int(row.get("frequency", row.get("freq", 0)))
            frequency = Frequency(
                id=freq_id,
                variable_id=new_var_id,
                value=value,
                frequency=frequency_value,
            )
            frequencies.append(frequency)
        if frequencies:
            self._catalog.frequency.add_all(frequencies)
