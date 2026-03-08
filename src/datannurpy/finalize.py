"""Finalize catalog by removing unseen entities."""

from __future__ import annotations

from typing import TYPE_CHECKING

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
        remove_dataset_cascade(catalog, dataset)

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


def remove_dataset_cascade(self: Catalog, dataset: Dataset) -> None:
    """Remove a dataset and its associated variables and frequencies."""
    # Remove variables for this dataset
    var_ids = self.variable.ids_having.dataset(dataset.id)
    if var_ids:
        self.variable.remove_all(var_ids)
        # Remove frequencies for these variables
        self.freq.remove_where("variable_id", "in", var_ids)

    # Remove dataset
    self.dataset.remove(dataset.id)
