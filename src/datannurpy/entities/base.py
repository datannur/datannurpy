"""Base entity class for datannur metadata."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class Entity:
    """Base class for all datannur entities."""

    id: str

    def to_dict(self) -> dict[str, Any]:
        """Export to datannur JSON format (None and empty lists excluded)."""
        result: dict[str, Any] = {}

        for key, value in self.__dict__.items():
            if value is None:
                continue
            if isinstance(value, list):
                if value:  # non-empty → comma-separated
                    result[key] = ",".join(str(v) for v in value)
            else:
                result[key] = value

        return result
