"""Tag entity."""

from __future__ import annotations

from dataclasses import dataclass, field

from .base import Entity


@dataclass
class Tag(Entity):
    """A keyword/tag that can be attached to various entities."""

    # Foreign key (parent tag)
    parent_id: str | None = None

    # Many-to-many relations
    doc_ids: list[str] = field(default_factory=list)

    # Descriptive
    name: str | None = None
    description: str | None = None
