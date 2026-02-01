"""JSON loader for datannur catalog format."""

from __future__ import annotations

import json
from dataclasses import fields
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

import pyarrow as pa

from ..entities import (
    Dataset,
    Doc,
    Folder,
    Institution,
    Modality,
    Tag,
    Value,
    Variable,
)
from ..entities.base import Entity

if TYPE_CHECKING:
    from ..catalog import Catalog

T = TypeVar("T", bound=Entity)


def _load_json(path: Path) -> list[dict]:
    """Load JSON file if it exists, return empty list otherwise."""
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _get_list_fields(entity_class: type[Entity]) -> set[str]:
    """Get field names that are list types for an entity class."""
    list_fields = set()
    for f in fields(entity_class):
        # Check if the field type annotation contains 'list'
        type_str = str(f.type)
        if "list[" in type_str.lower():
            list_fields.add(f.name)
    return list_fields


def _parse_entity(data: dict, entity_class: type[T]) -> T:
    """Parse a JSON dict into an entity instance."""
    # Get list fields for this entity class
    list_fields = _get_list_fields(entity_class)

    # Get valid field names for this entity
    valid_fields = {f.name for f in fields(entity_class)}

    kwargs = {}
    for key, value in data.items():
        if key not in valid_fields:
            continue  # Skip unknown fields

        if key in list_fields and isinstance(value, str):
            # Convert comma-separated string back to list
            kwargs[key] = [v.strip() for v in value.split(",") if v.strip()]
        else:
            kwargs[key] = value

    return entity_class(**kwargs)


def _parse_value(data: dict) -> Value:
    """Parse a JSON dict into a Value instance."""
    return Value(
        modality_id=data["modality_id"],
        value=data.get("value"),
        description=data.get("description"),
    )


def _load_freq_table(path: Path) -> pa.Table | None:
    """Load freq.json into a PyArrow Table."""
    if not path.exists():
        return None

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    if not data:
        return None

    # Build PyArrow table from list of dicts
    variable_ids = [row["variable_id"] for row in data]
    values = [row["value"] for row in data]
    freqs = [row["freq"] for row in data]

    return pa.table(
        {
            "variable_id": pa.array(variable_ids, type=pa.string()),
            "value": pa.array(values, type=pa.string()),
            "freq": pa.array(freqs, type=pa.int64()),
        }
    )


def load_db(db_path: str | Path, catalog: Catalog) -> None:
    """Load catalog entities from JSON files in db_path directory.

    Populates the catalog's entity lists from existing JSON files.
    Sets _seen = False on loaded entities for incremental scan tracking.
    Does not clear existing entities - use on a fresh Catalog.
    """
    db_path = Path(db_path)

    if not db_path.exists():
        return  # Empty catalog, nothing to load

    if not db_path.is_dir():
        msg = f"db_path must be a directory: {db_path}"
        raise ValueError(msg)

    # Load entities with _seen = False for incremental tracking
    for data in _load_json(db_path / "folder.json"):
        entity = _parse_entity(data, Folder)
        entity._seen = False
        catalog.folders.append(entity)

    for data in _load_json(db_path / "dataset.json"):
        entity = _parse_entity(data, Dataset)
        entity._seen = False
        catalog.datasets.append(entity)

    for data in _load_json(db_path / "variable.json"):
        catalog.variables.append(_parse_entity(data, Variable))

    for data in _load_json(db_path / "modality.json"):
        entity = _parse_entity(data, Modality)
        entity._seen = False
        catalog.modalities.append(entity)

    for data in _load_json(db_path / "value.json"):
        catalog.values.append(_parse_value(data))

    for data in _load_json(db_path / "institution.json"):
        entity = _parse_entity(data, Institution)
        entity._seen = False
        catalog.institutions.append(entity)

    for data in _load_json(db_path / "tag.json"):
        entity = _parse_entity(data, Tag)
        entity._seen = False
        catalog.tags.append(entity)

    for data in _load_json(db_path / "doc.json"):
        entity = _parse_entity(data, Doc)
        entity._seen = False
        catalog.docs.append(entity)

    # Load frequency table
    freq_table = _load_freq_table(db_path / "freq.json")
    if freq_table is not None:
        catalog._freq_tables.append(freq_table)
