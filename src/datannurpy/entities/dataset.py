"""Dataset entity."""

from __future__ import annotations

from dataclasses import dataclass, field

from .base import Entity


@dataclass
class Dataset(Entity):
    """Catalog dataset entity.

    A dataset represents a tabular data collection (table, file, etc.)
    within the datannur catalog.
    """

    # Foreign keys (single reference)
    folder_id: str | None = None
    manager_id: str | None = None
    owner_id: str | None = None

    # Many-to-many relations (list internally, comma-separated in JSON)
    tag_ids: list[str] = field(default_factory=list)
    doc_ids: list[str] = field(default_factory=list)

    # Descriptive
    name: str | None = None
    description: str | None = None
    type: str | None = None

    # Location
    data_path: str | None = None
    link: str | None = None
    localisation: str | None = None
    delivery_format: str | None = None

    # Stats
    nb_row: int | None = None

    # Temporal
    start_date: str | None = None
    end_date: str | None = None
    last_update_date: str | None = None
    updating_each: str | None = None
    no_more_update: str | None = None
