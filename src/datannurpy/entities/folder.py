"""Folder entity."""

from __future__ import annotations

from dataclasses import dataclass, field

from .base import Entity


@dataclass
class Folder(Entity):
    """A folder containing datasets and sub-folders.

    Folders are used to organize datasets hierarchically,
    typically representing directories or data sources.
    """

    # Foreign key (parent folder)
    parent_id: str | None = None

    # Many-to-many relations
    tag_ids: list[str] = field(default_factory=list)
    doc_ids: list[str] = field(default_factory=list)

    # Descriptive
    name: str | None = None
    description: str | None = None
    type: str | None = (
        None  # filesystem, sqlite, postgres, mysql, oracle, duckdb, schema, table_prefix
    )

    # Location
    data_path: str | None = None

    # Temporal
    last_update_date: str | None = None
