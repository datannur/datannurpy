"""Finalize catalog by removing unseen entities."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .utils.ids import MODALITIES_FOLDER_ID

if TYPE_CHECKING:
    from .catalog import Catalog
    from .schema import Dataset


def finalize(catalog: Catalog) -> None:
    """Remove entities with _seen=False (incremental cleanup)."""
    # Check if already finalized (idempotent)
    if catalog._finalized:
        return

    # Only cleanup if catalog has a db_path (persistent catalog)
    if catalog.db_path is None:
        catalog._finalized = True
        return

    # 1. Remove unseen folders
    removed_folder_ids = catalog.folder.ids_where("_seen", "==", False)
    if removed_folder_ids:
        catalog.folder.remove_all(removed_folder_ids)

    # 2. Remove unseen datasets (cascade: variables, frequencies)
    unseen_datasets = catalog.dataset.where("_seen", "==", False)
    for dataset in unseen_datasets:
        catalog._remove_dataset_cascade(dataset)

    # 3. Remove unseen modalities
    removed_modality_ids = catalog.modality.ids_where("_seen", "==", False)
    if removed_modality_ids:
        catalog.modality.remove_all(removed_modality_ids)

    # 4. Remove unseen institutions
    catalog.institution.remove_where("_seen", "==", False)

    # 5. Remove unseen tags
    catalog.tag.remove_where("_seen", "==", False)

    # 6. Remove unseen docs
    catalog.doc.remove_where("_seen", "==", False)

    # 7. Remove values of removed modalities
    if removed_modality_ids:
        catalog.value.remove_where("modality_id", "in", removed_modality_ids)

    # Mark as finalized
    catalog._finalized = True


def mark_dataset_modalities_seen(catalog: Catalog, dataset: Dataset) -> None:
    """Mark all modalities referenced by a dataset's variables as seen."""
    # Find all modality_ids referenced by this dataset's variables
    dataset_vars = catalog.variable.having.dataset(dataset.id)
    referenced_modality_ids: set[str] = set()
    for var in dataset_vars:
        referenced_modality_ids.update(var.modality_ids)

    if not referenced_modality_ids:
        return

    # Mark those modalities as seen (batch update)
    catalog.modality.update_many(list(referenced_modality_ids), _seen=True)

    # Also mark the _modalities folder as seen
    if catalog.folder.exists(MODALITIES_FOLDER_ID):
        catalog.folder.update(MODALITIES_FOLDER_ID, _seen=True)


def remove_dataset_cascade(self: Catalog, dataset: Dataset) -> None:
    """Remove a dataset and its associated variables and frequencies."""
    # Remove variables for this dataset
    vars_to_remove = self.variable.having.dataset(dataset.id)
    if vars_to_remove:
        self.variable.remove_all([v.id for v in vars_to_remove])

    # Remove frequencies for this dataset's variables
    if self._freq_tables:
        new_freq_tables = []
        var_id_prefix = f"{dataset.id}---"
        for table in self._freq_tables:
            var_ids = table["variable_id"].to_pylist()
            keep_indices = [
                i for i, vid in enumerate(var_ids) if not vid.startswith(var_id_prefix)
            ]
            if keep_indices:
                filtered = table.take(keep_indices)
                new_freq_tables.append(filtered)
        self._freq_tables = new_freq_tables

    # Remove dataset
    self.dataset.remove(dataset.id)
