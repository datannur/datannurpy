"""Variable entity."""

from __future__ import annotations

from dataclasses import dataclass, field

from .base import Entity


@dataclass
class Variable(Entity):
    """Variable contained in a dataset.

    A variable represents a column in a tabular dataset,
    with optional statistics and metadata.
    """

    # Foreign key
    dataset_id: str | None = None

    # Many-to-many relations
    modality_ids: list[str] = field(default_factory=list)
    tag_ids: list[str] = field(default_factory=list)
    source_var_ids: list[str] = field(default_factory=list)

    # Descriptive
    name: str | None = None
    original_name: str | None = None
    description: str | None = None
    type: str | None = None

    # Key info
    key: int | None = None

    # Stats
    nb_distinct: int | None = None
    nb_duplicate: int | None = None
    nb_missing: int | None = None

    # Temporal
    start_date: str | None = None
    end_date: str | None = None
