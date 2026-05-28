"""Time utilities."""

from __future__ import annotations

from datetime import datetime, timezone


def timestamp_to_iso(timestamp: int | float) -> str:
    """Convert Unix timestamp to UTC date-time string."""
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime(
        "%Y/%m/%dT%H:%M:%S"
    )


def iso_to_timestamp(value: str | None) -> int | None:
    """Parse a datannur date or date-time string as a Unix timestamp."""
    if not value:
        return None
    for fmt in ("%Y/%m/%dT%H:%M:%S", "%Y/%m/%d"):
        try:
            return int(
                datetime.strptime(value, fmt).replace(tzinfo=timezone.utc).timestamp()
            )
        except ValueError:
            continue
    return None
