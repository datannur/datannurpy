"""ID generation and validation utilities."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import PurePath

import polars as pl

# Separator for path components (folder---dataset---variable)
ID_SEPARATOR = "---"

# Folder ID for auto-generated enumerations
ENUMERATIONS_FOLDER_ID = "_enumerations"

# Valid ID pattern: a-zA-Z0-9_, - (and space)
_INVALID_ID_CHARS = re.compile(r"[^a-zA-Z0-9_,\- ]")

# Enumeration name settings
_ENUMERATION_NAME_MAX_VALUES = 3
_ENUMERATION_NAME_VALUE_MAX_LEN = 15


def sanitize_id(value: str) -> str:
    """Replace invalid characters with underscore."""
    return _INVALID_ID_CHARS.sub("_", value)


def make_id(*parts: str) -> str:
    """Join parts with ID_SEPARATOR."""
    return ID_SEPARATOR.join(parts)


def build_dataset_id(folder_id: str, dataset_name: str) -> str:
    """Build dataset ID from folder and dataset name."""
    return make_id(folder_id, sanitize_id(dataset_name))


def build_variable_id(folder_id: str, dataset_name: str, variable_name: str) -> str:
    """Build variable ID from folder, dataset and variable name."""
    return make_id(folder_id, sanitize_id(dataset_name), sanitize_id(variable_name))


def compute_enumeration_hash(values: set[str]) -> str:
    """Compute deterministic 10-char hash for a set of values."""
    signature = json.dumps(sorted(values), ensure_ascii=False)
    return hashlib.md5(signature.encode()).hexdigest()[:10]


def build_enumeration_name(values: set[str]) -> str:
    """Build human-readable name from values.

    Rules:
    - Sort values alphabetically (case-insensitive)
    - Take first 3 values, truncate each to 15 chars
    - Join with ", "
    - Add "... (+N)" if more values
    """
    sorted_vals = sorted(values, key=str.lower)
    display_vals: list[str] = []

    for val in sorted_vals[:_ENUMERATION_NAME_MAX_VALUES]:
        truncated = val[:_ENUMERATION_NAME_VALUE_MAX_LEN]
        if len(val) > _ENUMERATION_NAME_VALUE_MAX_LEN:
            truncated = truncated[:-3] + "..."
        display_vals.append(truncated)

    name = ", ".join(display_vals)

    remaining = len(sorted_vals) - _ENUMERATION_NAME_MAX_VALUES
    if remaining > 0:
        name += f"... (+{remaining})"

    return name


def _hash_value(value: str | None) -> str:
    """Hash a raw value into a short deterministic ID-safe string."""
    if value is None:
        return "_null_"
    return hashlib.md5(value.encode()).hexdigest()[:16]


def build_value_id(enumeration_id: str, value: str | None) -> str:
    """Build value ID from enumeration and value."""
    return make_id(enumeration_id, _hash_value(value))


def build_frequency_id(variable_id: str, value: str | None) -> str:
    """Build frequency ID from variable and value."""
    return make_id(variable_id, _hash_value(value))


def compute_runtime_ids(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    """Compute id column by concatenating cols with ID_SEPARATOR."""
    if df.is_empty() or "id" in df.columns:
        return df
    expr = pl.col(cols[0])
    for col in cols[1:]:
        expr = expr + ID_SEPARATOR + pl.col(col).fill_null("_null_")
    return df.with_columns(expr.alias("id"))


def get_folder_id(
    path: PurePath,
    root: PurePath,
    prefix: str,
    subdir_ids: dict[PurePath, str],
) -> str:
    """Determine folder_id for a file or directory."""
    parent_dir = path.parent
    if parent_dir == root:
        return prefix
    return subdir_ids.get(parent_dir, prefix)


def build_dataset_id_name(
    path: PurePath,
    root: PurePath,
    prefix: str,
) -> tuple[str, str]:
    """Build dataset ID and name from path."""
    rel_path = path.relative_to(root)
    if path.suffix:  # File (has extension)
        path_parts = [sanitize_id(p) for p in rel_path.parts]
        return make_id(prefix, *path_parts), path.stem
    # Directory (Delta/Hive) in subdirectory
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
