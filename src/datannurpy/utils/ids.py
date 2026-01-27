"""ID generation and validation utilities."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

# Separator for path components (folder---dataset---variable)
ID_SEPARATOR = "---"

# Folder ID for auto-generated modalities
MODALITIES_FOLDER_ID = "_modalities"

# Valid ID pattern: a-zA-Z0-9_, - (and space)
_INVALID_ID_CHARS = re.compile(r"[^a-zA-Z0-9_,\- ]")

# Modality name settings
_MODALITY_NAME_MAX_VALUES = 3
_MODALITY_NAME_VALUE_MAX_LEN = 15


def sanitize_id(value: str) -> str:
    """Replace invalid characters with underscore."""
    return _INVALID_ID_CHARS.sub("_", value)


def make_id(*parts: str) -> str:
    """Join parts with ID_SEPARATOR."""
    return ID_SEPARATOR.join(parts)


def compute_modality_hash(values: set[str]) -> str:
    """Compute deterministic 10-char hash for a set of values."""
    signature = json.dumps(sorted(values), ensure_ascii=False)
    return hashlib.md5(signature.encode()).hexdigest()[:10]


def build_modality_name(values: set[str]) -> str:
    """Build human-readable name from values.

    Rules:
    - Sort values alphabetically (case-insensitive)
    - Take first 3 values, truncate each to 15 chars
    - Join with ", "
    - Add "... (+N)" if more values
    """
    sorted_vals = sorted(values, key=str.lower)
    display_vals: list[str] = []

    for val in sorted_vals[:_MODALITY_NAME_MAX_VALUES]:
        truncated = val[:_MODALITY_NAME_VALUE_MAX_LEN]
        if len(val) > _MODALITY_NAME_VALUE_MAX_LEN:
            truncated = truncated[:-3] + "..."
        display_vals.append(truncated)

    name = ", ".join(display_vals)

    remaining = len(sorted_vals) - _MODALITY_NAME_MAX_VALUES
    if remaining > 0:
        name += f"... (+{remaining})"

    return name


def get_folder_id(
    path: Path,
    root: Path,
    prefix: str,
    subdir_ids: dict[Path, str],
) -> str:
    """Determine folder_id for a file or directory."""
    parent_dir = path.parent
    if parent_dir == root:
        return prefix
    return subdir_ids.get(parent_dir, prefix)


def build_dataset_id_name(
    path: Path,
    root: Path,
    prefix: str,
) -> tuple[str, str]:
    """Build dataset ID and name from path."""
    rel_path = path.relative_to(root)
    if path.is_file():
        path_parts = [sanitize_id(p) for p in rel_path.parts]
        return make_id(prefix, *path_parts), path.stem
    # Directory (Delta/Hive)
    if rel_path == Path("."):
        return prefix, path.name
    path_parts = [sanitize_id(p) for p in rel_path.parts]
    return make_id(prefix, *path_parts), path.name


def build_variable_ids(
    variables: list,
    dataset_id: str,
) -> dict[str, str]:
    """Build final IDs for variables and return name→id mapping."""
    var_id_mapping: dict[str, str] = {}
    for var in variables:
        var.dataset_id = dataset_id
        var.id = make_id(dataset_id, sanitize_id(var.name))
        var_id_mapping[var.name] = var.id
    return var_id_mapping
