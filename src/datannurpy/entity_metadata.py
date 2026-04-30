"""Public API input type for scanned folders, datasets, and databases."""

from __future__ import annotations

from dataclasses import dataclass

from .schema import Folder


@dataclass
class EntityMetadata:
    """Identity, parent linkage, and user metadata for a scanned entity."""

    id: str | None = None
    parent_id: str | None = None
    manager_id: str | None = None
    owner_id: str | None = None
    tag_ids: list[str] | None = None
    doc_ids: list[str] | None = None
    name: str | None = None
    description: str | None = None
    license: str | None = None
    type: str | None = None
    link: str | None = None
    localisation: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    updating_each: str | None = None
    no_more_update: str | None = None


def folder_from_metadata(
    metadata: EntityMetadata,
    *,
    default_id: str | None = None,
    default_name: str | None = None,
) -> Folder:
    """Build a Folder row from EntityMetadata with scan-derived defaults."""
    return Folder(
        id=metadata.id or default_id or "",
        parent_id=metadata.parent_id,
        manager_id=metadata.manager_id,
        owner_id=metadata.owner_id,
        tag_ids=list(metadata.tag_ids or []),
        doc_ids=list(metadata.doc_ids or []),
        name=metadata.name if metadata.name is not None else default_name,
        description=metadata.description,
        license=metadata.license,
        type=metadata.type,
        link=metadata.link,
        localisation=metadata.localisation,
        start_date=metadata.start_date,
        end_date=metadata.end_date,
        updating_each=metadata.updating_each,
        no_more_update=metadata.no_more_update,
    )
