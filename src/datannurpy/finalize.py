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
    unseen_folders = [f for f in catalog.folder.all() if not f._seen]
    removed_folder_ids = [f.id for f in unseen_folders]
    if removed_folder_ids:
        catalog.folder.remove_all(removed_folder_ids)

    # 2. Remove unseen datasets (cascade: variables, frequencies)
    unseen_datasets = [ds for ds in catalog.dataset.all() if not ds._seen]
    for dataset in unseen_datasets:
        catalog._remove_dataset_cascade(dataset)

    # 3. Remove unseen modalities
    unseen_modalities = [m for m in catalog.modality.all() if not m._seen]
    removed_modality_ids = [m.id for m in unseen_modalities]
    if removed_modality_ids:
        catalog.modality.remove_all(removed_modality_ids)

    # 4. Remove unseen institutions
    unseen_institutions = [
        i for i in catalog.institution.all() if not getattr(i, "_seen", True)
    ]
    if unseen_institutions:
        catalog.institution.remove_all([i.id for i in unseen_institutions])

    # 5. Remove unseen tags
    unseen_tags = [t for t in catalog.tag.all() if not getattr(t, "_seen", True)]
    if unseen_tags:
        catalog.tag.remove_all([t.id for t in unseen_tags])

    # 6. Remove unseen docs
    unseen_docs = [d for d in catalog.doc.all() if not getattr(d, "_seen", True)]
    if unseen_docs:
        catalog.doc.remove_all([d.id for d in unseen_docs])

    # 7. Remove values of removed modalities
    if removed_modality_ids:
        # Value doesn't have id, filter using DataFrame directly
        import polars as pl

        if not catalog.value._df.is_empty():
            catalog.value._df = catalog.value._df.filter(
                ~pl.col("modality_id").is_in(removed_modality_ids)
            )

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

    # Mark those modalities as seen
    for modality_id in referenced_modality_ids:
        modality = catalog.modality.get(modality_id)
        if modality is not None:
            catalog.modality.update(modality_id, _seen=True)

    # Also mark the _modalities folder as seen
    folder = catalog.folder.get(MODALITIES_FOLDER_ID)
    if folder is not None:
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
