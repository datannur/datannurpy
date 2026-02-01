"""Finalize catalog by removing unseen entities."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .utils.ids import MODALITIES_FOLDER_ID

if TYPE_CHECKING:
    from .catalog import Catalog
    from .entities import Dataset


def finalize(catalog: Catalog) -> None:
    """Remove entities with _seen=False (incremental cleanup)."""
    # Check if already finalized (idempotent)
    if catalog._finalized:
        return

    # Only cleanup if a catalog was loaded from db_path
    # For fresh catalogs (no db_path), skip cleanup to preserve manually added entities
    if catalog.db_path is None:
        catalog._finalized = True
        return

    # 1. Remove unseen folders and update index
    removed_folder_ids = {f.id for f in catalog.folders if not f._seen}
    catalog.folders = [f for f in catalog.folders if f._seen]
    for fid in removed_folder_ids:
        catalog._folder_index.pop(fid, None)

    # 2. Remove unseen datasets (cascade: variables, frequencies)
    unseen_datasets = [ds for ds in catalog.datasets if not ds._seen]
    for dataset in unseen_datasets:
        catalog._remove_dataset_cascade(dataset)

    # 3. Remove unseen modalities and update index
    removed_modality_ids = {m.id for m in catalog.modalities if not m._seen}
    catalog.modalities = [m for m in catalog.modalities if m._seen]
    for mid in removed_modality_ids:
        catalog._modality_index.pop(mid, None)

    # 4. Remove unseen institutions
    catalog.institutions = [i for i in catalog.institutions if i._seen]

    # 5. Remove unseen tags
    catalog.tags = [t for t in catalog.tags if t._seen]

    # 6. Remove unseen docs
    catalog.docs = [d for d in catalog.docs if d._seen]

    # 7. Remove values of removed modalities
    if removed_modality_ids:
        catalog.values = [
            v for v in catalog.values if v.modality_id not in removed_modality_ids
        ]

    # Mark as finalized
    catalog._finalized = True


def mark_dataset_modalities_seen(catalog: Catalog, dataset: Dataset) -> None:
    """Mark all modalities referenced by a dataset's variables as seen."""
    # Find all modality_ids referenced by this dataset's variables (O(1) lookup)
    dataset_vars = catalog._variables_by_dataset.get(dataset.id, [])
    referenced_modality_ids: set[str] = set()
    for var in dataset_vars:
        referenced_modality_ids.update(var.modality_ids)

    if not referenced_modality_ids:
        return

    # Mark those modalities as seen (O(1) per modality)
    for modality_id in referenced_modality_ids:
        modality = catalog._modality_index.get(modality_id)
        if modality is not None:
            modality._seen = True

    # Also mark the _modalities folder as seen (O(1))
    folder = catalog._folder_index.get(MODALITIES_FOLDER_ID)
    if folder is not None:
        folder._seen = True


def remove_dataset_cascade(self: Catalog, dataset: Dataset) -> None:
    """Remove a dataset and its associated variables and frequencies."""
    # Remove variables for this dataset
    self.variables = [v for v in self.variables if v.dataset_id != dataset.id]

    # Remove from variables index
    if dataset.id in self._variables_by_dataset:
        del self._variables_by_dataset[dataset.id]

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

    # Remove from index
    if dataset.data_path and dataset.data_path in self._dataset_index:
        del self._dataset_index[dataset.data_path]

    # Remove dataset
    self.datasets = [ds for ds in self.datasets if ds.id != dataset.id]
