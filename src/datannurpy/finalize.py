"""Finalize catalog by removing unseen entities."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .catalog import Catalog
    from .schema import Dataset


def _as_id_list(ids: str | list[str] | set[str] | tuple[str, ...]) -> list[str]:
    """Return a stable non-empty list of IDs."""
    if isinstance(ids, str):
        return [ids]
    return list(ids)


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
    unseen_dataset_ids = catalog.dataset.ids_where("_seen", "==", False)
    if unseen_dataset_ids:
        remove_datasets_cascade(catalog, unseen_dataset_ids)

    # 3. Remove unseen enumerations
    removed_enumeration_ids = catalog.enumeration.ids_where("_seen", "==", False)
    if removed_enumeration_ids:
        remove_enumerations_cascade(catalog, removed_enumeration_ids)

    # 4. Remove unseen organizations
    unseen_organization_ids = catalog.organization.ids_where("_seen", "==", False)
    if unseen_organization_ids:
        remove_organizations_cascade(catalog, unseen_organization_ids)

    # 5. Remove unseen tags
    unseen_tag_ids = catalog.tag.ids_where("_seen", "==", False)
    if unseen_tag_ids:
        remove_tags_cascade(catalog, unseen_tag_ids)

    # 6. Remove unreferenced scan tags (auto---, db---)
    _remove_orphan_scan_tags(catalog)

    # 7. Remove unseen docs
    unseen_doc_ids = catalog.doc.ids_where("_seen", "==", False)
    if unseen_doc_ids:
        remove_docs_cascade(catalog, unseen_doc_ids)

    # 7b. Remove unseen concepts
    unseen_concept_ids = catalog.concept.ids_where("_seen", "==", False)
    if unseen_concept_ids:
        remove_concepts_cascade(catalog, unseen_concept_ids)

    # Mark as finalized
    catalog._finalized = True


def remove_dataset_cascade(self: Catalog, dataset: Dataset) -> None:
    """Remove a dataset and its associated variables and frequencies."""
    remove_datasets_cascade(self, [dataset.id])


def remove_datasets_cascade(
    catalog: Catalog, dataset_ids: str | list[str] | set[str]
) -> None:
    """Remove datasets and their variables/frequencies in bulk."""
    ids = _as_id_list(dataset_ids)
    if not ids:
        return
    variable_ids = catalog.variable.ids_where("dataset_id", "in", ids)
    remove_variables_cascade(catalog, variable_ids)
    catalog.dataset.remove_all(ids)


def remove_folders_cascade(
    catalog: Catalog, folder_ids: str | list[str] | set[str]
) -> None:
    """Remove folders, descendant folders, and contained datasets/enumerations."""
    ids = _as_id_list(folder_ids)
    if not ids:
        return

    removed = _collect_descendant_folder_ids(catalog, set(ids))
    dataset_ids = catalog.dataset.ids_where("folder_id", "in", removed)
    remove_datasets_cascade(catalog, dataset_ids)
    enumeration_ids = catalog.enumeration.ids_where("folder_id", "in", removed)
    remove_enumerations_cascade(catalog, enumeration_ids)
    catalog.folder.remove_all(list(removed))


def _collect_descendant_folder_ids(catalog: Catalog, folder_ids: set[str]) -> set[str]:
    """Return folder IDs plus all descendants by parent_id."""
    children: dict[str, list[str]] = {}
    for folder in catalog.folder.all():
        if folder.parent_id is not None:
            children.setdefault(folder.parent_id, []).append(folder.id)

    collected: set[str] = set()
    stack = list(folder_ids)
    while stack:
        folder_id = stack.pop()
        if folder_id in collected:
            continue
        collected.add(folder_id)
        stack.extend(children.get(folder_id, []))
    return collected


def remove_variables_cascade(
    catalog: Catalog, variable_ids: str | list[str] | set[str]
) -> None:
    """Remove variables and their frequencies in bulk."""
    ids = _as_id_list(variable_ids)
    if not ids:
        return
    catalog.frequency.remove_where("variable_id", "in", ids)
    catalog.variable.remove_all(ids)


def remove_enumerations_cascade(
    catalog: Catalog, enumeration_ids: str | list[str] | set[str]
) -> None:
    """Remove enumerations, values, and variable references in bulk."""
    ids = _as_id_list(enumeration_ids)
    if not ids:
        return
    _remove_ids_from_list_field(catalog.variable, "enumeration_ids", set(ids))
    catalog.value.remove_where("enumeration_id", "in", ids)
    catalog.enumeration.remove_all(ids)


def remove_tags_cascade(catalog: Catalog, tag_ids: str | list[str] | set[str]) -> None:
    """Remove tags and tag_ids references in bulk."""
    ids = _as_id_list(tag_ids)
    if not ids:
        return
    removed = set(ids)
    for table in (
        catalog.folder,
        catalog.dataset,
        catalog.variable,
        catalog.organization,
        catalog.concept,
    ):
        _remove_ids_from_list_field(table, "tag_ids", removed)
    catalog.tag.remove_all(ids)


def remove_docs_cascade(catalog: Catalog, doc_ids: str | list[str] | set[str]) -> None:
    """Remove docs and doc_ids references in bulk."""
    ids = _as_id_list(doc_ids)
    if not ids:
        return
    removed = set(ids)
    for table in (
        catalog.folder,
        catalog.dataset,
        catalog.organization,
        catalog.tag,
        catalog.concept,
    ):
        _remove_ids_from_list_field(table, "doc_ids", removed)
    catalog.doc.remove_all(ids)


def remove_concepts_cascade(
    catalog: Catalog, concept_ids: str | list[str] | set[str]
) -> None:
    """Remove concepts and variable concept_id references in bulk."""
    ids = _as_id_list(concept_ids)
    if not ids:
        return
    _clear_scalar_ids(catalog.variable, "concept_id", set(ids))
    catalog.concept.remove_all(ids)


def remove_organizations_cascade(
    catalog: Catalog, organization_ids: str | list[str] | set[str]
) -> None:
    """Remove organizations and owner/manager references in bulk."""
    ids = _as_id_list(organization_ids)
    if not ids:
        return
    removed = set(ids)
    for table in (catalog.folder, catalog.dataset):
        _clear_scalar_ids(table, "owner_organization_id", removed)
        _clear_scalar_ids(table, "manager_organization_id", removed)
    catalog.organization.remove_all(ids)


def _remove_ids_from_list_field(table: object, field: str, removed: set[str]) -> None:
    """Remove IDs from a list field and replace changed rows in one batch."""
    df = table.df  # type: ignore[attr-defined]
    if df.is_empty() or field not in df.columns:
        return
    changed = []
    for row in table.all():  # type: ignore[attr-defined]
        current = getattr(row, field, None)
        if not current:
            continue
        updated = [item for item in current if item not in removed]
        if len(updated) != len(current):
            changed.append(replace(row, **{field: updated}))
    if changed:
        table.remove_all([row.id for row in changed])  # type: ignore[attr-defined]
        table.add_all(changed)  # type: ignore[attr-defined]


def _clear_scalar_ids(table: object, field: str, removed: set[str]) -> None:
    """Clear scalar ID references and replace changed rows in one batch."""
    df = table.df  # type: ignore[attr-defined]
    if df.is_empty() or field not in df.columns:
        return
    changed = []
    for row in table.all():  # type: ignore[attr-defined]
        if getattr(row, field, None) in removed:
            changed.append(replace(row, **{field: None}))
    if changed:
        table.remove_all([row.id for row in changed])  # type: ignore[attr-defined]
        table.add_all(changed)  # type: ignore[attr-defined]


def _collect_referenced_tag_ids(catalog: Catalog) -> set[str]:
    """Collect all tag IDs referenced by any entity."""
    referenced: set[str] = set()
    for table in (
        catalog.variable,
        catalog.dataset,
        catalog.folder,
        catalog.organization,
    ):
        df = table.df
        if not df.is_empty() and "tag_ids" in df.columns:
            for tag_list in df["tag_ids"].drop_nulls().to_list():
                if tag_list:
                    referenced.update(tag_list)
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
