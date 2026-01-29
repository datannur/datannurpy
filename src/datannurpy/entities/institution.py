"""Institution entity."""

from __future__ import annotations

from dataclasses import dataclass, field

from .base import Entity


@dataclass
class Institution(Entity):
    """An institution that can be owner or manager of folders/datasets."""

    # Foreign key (parent institution)
    parent_id: str | None = None

    # Many-to-many relations
    tag_ids: list[str] = field(default_factory=list)
    doc_ids: list[str] = field(default_factory=list)

    # Descriptive
    name: str | None = None
    description: str | None = None
    email: str | None = None
    phone: str | None = None

    # Temporal
    start_date: str | None = None
    end_date: str | None = None
