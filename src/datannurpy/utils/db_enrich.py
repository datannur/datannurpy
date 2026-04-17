"""Apply database introspection metadata to catalog entities."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..scanner.autotag import SCAN_TAG_ID, SCAN_TAG_DESCRIPTION
from ..scanner.db_introspect import ForeignKey, TableMetadata
from ..schema import Dataset, Tag, Variable
from .ids import make_id, sanitize_id

if TYPE_CHECKING:
    from ..catalog import Catalog

_DB_TAG_PARENT_ID = "db"
_DB_CONSTRAINT_TAGS: dict[str, str] = {
    "db---not-null": "Not Null",
    "db---unique": "Unique",
    "db---indexed": "Indexed",
    "db---auto-increment": "Auto-increment",
}


def _upsert_tag(
    catalog: Catalog,
    tag_id: str,
    name: str,
    parent_id: str | None = None,
    description: str | None = None,
) -> None:
    """Create tag if missing, or mark existing as seen (never overwriting name/description)."""
    if catalog.tag.get(tag_id) is None:
        catalog.tag.add(
            Tag(
                id=tag_id,
                name=name,
                description=description,
                parent_id=parent_id,
                _seen=True,
            )
        )
    else:
        catalog.tag.update(tag_id, _seen=True)


def ensure_db_tags(catalog: Catalog) -> None:
    """Create or mark DB constraint tags as seen."""
    _upsert_tag(catalog, SCAN_TAG_ID, "Scan", description=SCAN_TAG_DESCRIPTION)
    _upsert_tag(catalog, _DB_TAG_PARENT_ID, "Database", parent_id=SCAN_TAG_ID)
    for tag_id, name in _DB_CONSTRAINT_TAGS.items():
        _upsert_tag(catalog, tag_id, name, parent_id=_DB_TAG_PARENT_ID)


def _compute_var_db_tags(name: str, meta: TableMetadata) -> list[str]:
    """Compute DB constraint tag IDs for a variable."""
    tags: list[str] = []
    if name in meta.not_null:
        tags.append("db---not-null")
    if name in meta.unique:
        tags.append("db---unique")
    if name in meta.indexed:
        tags.append("db---indexed")
    if name in meta.auto_inc:
        tags.append("db---auto-increment")
    return tags


def apply_metadata_to_new_vars(
    table_vars: list[Variable],
    dataset: Dataset,
    meta: TableMetadata,
) -> None:
    """Apply introspection results to freshly-scanned variables and dataset (in-place)."""
    if meta.table_comment and not dataset.description:
        dataset.description = meta.table_comment

    for var in table_vars:
        name = var.name
        if name in meta.pk_map:
            var.key = meta.pk_map[name]
        if name in meta.col_comments and not var.description:
            var.description = meta.col_comments[name]

        db_tags = _compute_var_db_tags(name, meta)
        if db_tags:
            var.tag_ids = list(dict.fromkeys(var.tag_ids + db_tags))


def update_cached_metadata(
    catalog: Catalog,
    dataset_id: str,
    meta: TableMetadata,
) -> None:
    """Apply introspection results to cached (existing) variables and dataset."""
    if meta.table_comment:
        ds = catalog.dataset.get(dataset_id)
        if ds and not ds.description:
            catalog.dataset.update(dataset_id, description=meta.table_comment)

    var_ids = catalog.variable.ids_having.dataset(dataset_id)
    all_db_tag_ids = set(_DB_CONSTRAINT_TAGS.keys())
    for var_id in var_ids:
        var = catalog.variable.get(var_id)
        assert var is not None  # guaranteed by ids_having
        name = var.name
        updates: dict[str, object] = {}

        pk_pos = meta.pk_map.get(name)
        if pk_pos is not None and var.key != pk_pos:
            updates["key"] = pk_pos
        elif pk_pos is None and var.key is not None:
            updates["key"] = None

        if name in meta.col_comments and not var.description:
            updates["description"] = meta.col_comments[name]

        db_tags = _compute_var_db_tags(name, meta)
        existing_tags = list(var.tag_ids) if var.tag_ids else []
        non_db_tags = [t for t in existing_tags if t not in all_db_tag_ids]
        new_tags = list(dict.fromkeys(non_db_tags + db_tags))
        if new_tags != existing_tags:
            updates["tag_ids"] = new_tags

        if updates:
            catalog.variable.update(var_id, **updates)


def collect_fk_refs(
    fks: list[ForeignKey],
    dataset_id: str,
    raw_fk_refs: list[tuple[str, str | None, str, str]],
) -> None:
    """Append raw FK references for post-loop resolution."""
    for fk in fks:
        var_id = make_id(dataset_id, sanitize_id(fk.local_col))
        raw_fk_refs.append((var_id, fk.ref_schema, fk.ref_table, fk.ref_col))


def resolve_foreign_keys(
    catalog: Catalog,
    raw_fk_refs: list[tuple[str, str | None, str, str]],
    table_to_dataset_id: dict[tuple[str | None, str], str],
) -> None:
    """Resolve FK references to actual variable IDs."""
    for var_id, ref_schema, ref_table, ref_col in raw_fk_refs:
        target_dataset_id = table_to_dataset_id.get((ref_schema, ref_table))
        if target_dataset_id is None:
            target_dataset_id = table_to_dataset_id.get((None, ref_table))
        if target_dataset_id is None:
            continue
        fk_var_id = make_id(target_dataset_id, sanitize_id(ref_col))
        catalog.variable.update(var_id, fk_var_id=fk_var_id)
