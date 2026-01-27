"""Prefix-based grouping utilities for table names."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass


@dataclass
class PrefixFolder:
    """A folder derived from a common prefix."""

    prefix: str
    parent_prefix: str | None


def get_prefix_folders(
    table_names: list[str],
    sep: str = "_",
    min_count: int = 2,
) -> list[PrefixFolder]:
    """Find prefixes that group at least min_count tables."""
    if not table_names:
        return []

    prefix_counts: Counter[str] = Counter()

    for table in table_names:
        parts = table.split(sep)
        # Count all prefixes except the full name
        for i in range(1, len(parts)):
            prefix = sep.join(parts[:i])
            prefix_counts[prefix] += 1

    # Keep only prefixes with enough tables
    valid_prefixes = {p for p, count in prefix_counts.items() if count >= min_count}

    # Exclude prefix common to ALL tables (would be useless as root)
    total_tables = len(table_names)
    valid_prefixes = {p for p in valid_prefixes if prefix_counts[p] < total_tables}

    if not valid_prefixes:
        return []

    # Build hierarchy: find parent for each prefix
    result: list[PrefixFolder] = []
    for prefix in sorted(valid_prefixes):
        parent = _find_parent_prefix(prefix, valid_prefixes, sep)
        result.append(PrefixFolder(prefix=prefix, parent_prefix=parent))

    return result


def _find_parent_prefix(
    prefix: str,
    valid_prefixes: set[str],
    sep: str,
) -> str | None:
    """Find the closest valid parent prefix."""
    parts = prefix.split(sep)
    # Try progressively shorter prefixes
    for i in range(len(parts) - 1, 0, -1):
        candidate = sep.join(parts[:i])
        if candidate in valid_prefixes:
            return candidate
    return None


def get_table_prefix(
    table_name: str,
    valid_prefixes: set[str],
    sep: str = "_",
) -> str | None:
    """Find the most specific (longest) prefix folder for a table."""
    parts = table_name.split(sep)
    # Try longest prefix first
    for i in range(len(parts) - 1, 0, -1):
        candidate = sep.join(parts[:i])
        if candidate in valid_prefixes:
            return candidate
    return None
