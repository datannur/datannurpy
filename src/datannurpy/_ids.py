"""ID generation and validation utilities."""

from __future__ import annotations

import re

# Separator for path components (folder---dataset---variable)
ID_SEPARATOR = "---"

# Valid ID pattern: a-zA-Z0-9_, - (and space)
_INVALID_ID_CHARS = re.compile(r"[^a-zA-Z0-9_,\- ]")


def sanitize_id(value: str) -> str:
    """Replace invalid characters with underscore."""
    return _INVALID_ID_CHARS.sub("_", value)


def make_id(*parts: str) -> str:
    """Join parts with ID_SEPARATOR."""
    return ID_SEPARATOR.join(parts)
