"""Time utilities."""

from __future__ import annotations

from datetime import datetime, timezone


def timestamp_to_iso(timestamp: int) -> str:
    """Convert Unix timestamp to ISO date string (YYYY/MM/DD)."""
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y/%m/%d")
