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

    # 6. Remove unreferenced scan tags (auto---, db---)
    _remove_orphan_scan_tags(catalog)

    # 7. Remove unseen docs
    catalog.doc.remove_where("_seen", "==", False)

    # 7b. Remove unseen concepts
    catalog.concept.remove_where("_seen", "==", False)

    # 8. Remove values of removed modalities
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


def _collect_referenced_tag_ids(catalog: Catalog) -> set[str]:
    """Collect all tag IDs referenced by any entity."""
    referenced: set[str] = set()
    for entity in catalog.variable.all():
        referenced.update(entity.tag_ids)
    for entity in catalog.dataset.all():
        referenced.update(entity.tag_ids)
    for entity in catalog.folder.all():
        referenced.update(entity.tag_ids)
    for entity in catalog.institution.all():
        referenced.update(entity.tag_ids)
    return referenced


def _remove_orphan_scan_tags(catalog: Catalog) -> None:
    """Remove scan tags (auto---, db---) not referenced by any entity."""
    from .scanner.autotag import SCAN_TAG_ID

    scan_tag = catalog.tag.get(SCAN_TAG_ID)
    if scan_tag is None:
        return

    referenced = _collect_referenced_tag_ids(catalog)

    # Collect all descendants of scan (DFS)
    all_tags = catalog.tag.all()
    children: dict[str | None, list[str]] = {}
    parent_of: dict[str, str | None] = {}
    for tag in all_tags:
        children.setdefault(tag.parent_id, []).append(tag.id)
        parent_of[tag.id] = tag.parent_id
    scan_tag_ids: set[str] = set()
    stack = [SCAN_TAG_ID]
    while stack:
        tid = stack.pop()
        scan_tag_ids.add(tid)
        stack.extend(children.get(tid, []))

    # Start by marking all unreferenced scan tags for removal
    to_remove = {t for t in scan_tag_ids if t not in referenced}

    # Keep parent tags that still have at least one child remaining
    keep: set[str] = set()
    for tag_id in scan_tag_ids - to_remove:
        pid = parent_of.get(tag_id)
        while pid is not None:
            keep.add(pid)
            pid = parent_of.get(pid)

    to_remove -= keep

    if to_remove:
        catalog.tag.remove_all(list(to_remove))
