"""Doc entity."""

from __future__ import annotations

from dataclasses import dataclass

from .base import Entity


@dataclass
class Doc(Entity):
    """A document (PDF, Markdown, etc.) attached to entities."""

    # Descriptive
    name: str | None = None
    description: str | None = None
    path: str | None = None
    type: str | None = None

    # Temporal
    last_update: str | None = None
