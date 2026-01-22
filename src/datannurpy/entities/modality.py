"""Modality and Value entities."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .base import Entity


@dataclass
class Modality(Entity):
    """A modality represents a reusable set of values (codification).

    Multiple variables can reference the same modality if they share
    the same set of values.
    """

    folder_id: str | None = None
    name: str | None = None
    description: str | None = None
    type: str | None = None


@dataclass
class Value:
    """A value within a modality.

    Not an Entity because it has no unique id field.
    """

    modality_id: str
    value: str | None = None
    description: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Export to datannur JSON format (None excluded)."""
        result: dict[str, Any] = {}
        for key, val in self.__dict__.items():
            if val is not None:
                result[key] = val
        return result
