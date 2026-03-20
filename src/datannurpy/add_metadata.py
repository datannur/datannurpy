"""Load manually curated metadata from files or database."""

from __future__ import annotations

import json
import sys
import time
from collections.abc import Hashable
from dataclasses import MISSING, fields
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlparse

import ibis
import pandas as pd

from .schema import (
    Dataset,
    Doc,
    Folder,
    Institution,
    Modality,
    Tag,
    Value,
    Variable,
)
from .scanner import read_csv, read_excel, read_statistical
from .utils import log_done, log_section, log_warn
from .utils.ids import build_value_id
from .utils.params import validate_params

if TYPE_CHECKING:
    from .catalog import Catalog

# Entity type to class mapping
ENTITY_CLASSES: dict[str, type] = {
    "folder": Folder,
    "dataset": Dataset,
    "variable": Variable,
    "modality": Modality,
    "value": Value,
    "institution": Institution,
    "tag": Tag,
    "doc": Doc,
}

# Entities without required id (use composite key)
ENTITIES_WITHOUT_ID = {"value"}

# List fields that should be merged (union)
LIST_FIELDS = {"tag_ids", "doc_ids", "modality_ids", "source_var_ids"}

# Supported file extensions for metadata
SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls", ".json", ".sas7bdat"}

# Entities allowed per depth level
DEPTH_ENTITIES: dict[str, set[str]] = {
    "structure": {"folder", "dataset", "institution", "tag", "doc"},
    "schema": {
        "folder",
        "dataset",
        "institution",
        "tag",
        "doc",
        "variable",
    },
    "full": {
        "folder",
        "dataset",
        "institution",
        "tag",
        "doc",
        "variable",
        "modality",
        "value",
        "freq",
    },
}


def _get_required_fields(entity_class: type) -> set[str]:
    """Get required field names for an entity class (fields without defaults)."""
    return {
        f.name
        for f in fields(entity_class)
        if f.default is MISSING and f.default_factory is MISSING
    }


def _validate_entity_table(
    catalog: Catalog,
    entity_name: str,
    df: pd.DataFrame,
    file_name: str,
) -> list[str]:
    """Validate an entity table before processing. Returns list of error messages."""
    errors: list[str] = []
    entity_class = ENTITY_CLASSES[entity_name]

    # Skip validation for entities without id (value uses composite key)
    if entity_name in ENTITIES_WITHOUT_ID:
        return errors

    catalog_table = _get_catalog_table(catalog, entity_name)
    assert catalog_table is not None  # entity_name is always valid
    required = _get_required_fields(entity_class)
    csv_columns = set(df.columns)

    # Check for missing required columns at file level
    # "name" can be inferred from "id" for variables
    check_required = required.copy()
    if entity_name == "variable" and "id" in csv_columns:
        check_required.discard("name")

    missing_columns = check_required - csv_columns
    if missing_columns:
        missing_str = ", ".join(sorted(missing_columns))
        errors.append(f"{file_name}: missing column(s) {missing_str}")
        return errors  # No point checking rows if columns are missing

    # Check for empty required values in each row (for new entities only)
    rows = df.to_dict(orient="records")
    for row_idx, row in enumerate(rows):
        row_data = _convert_row_to_dict(row, entity_class)

        entity_id = row_data.get("id")
        if entity_id is None:
            continue

        # Check if entity already exists (will be updated, not created)
        existing = catalog_table.get(str(entity_id))
        if existing:
            continue

        # For new entities, check required fields have values
        # "name" can be inferred from "id" for variables
        check_fields = check_required - {"id"}  # id already checked above
        missing_values = [f for f in check_fields if f not in row_data]
        if missing_values:
            missing_str = ", ".join(sorted(missing_values))
            errors.append(f"{file_name}, line {row_idx + 2}: empty {missing_str}")

    return errors


def _validate_all_tables(
    catalog: Catalog,
    tables: dict[str, tuple[pd.DataFrame, str]],
) -> list[str]:
    """Validate all tables and return all errors."""
    all_errors: list[str] = []
    for entity_name, (table, file_name) in tables.items():
        errors = _validate_entity_table(catalog, entity_name, table, file_name)
        all_errors.extend(errors)
    return all_errors


def _is_database_connection(path: str) -> bool:
    """Check if path is a database connection string."""
    parsed = urlparse(path)
    return parsed.scheme in {
        "sqlite",
        "postgresql",
        "postgres",
        "mysql",
        "oracle",
        "mssql",
    }


def _read_file(file_path: Path, *, quiet: bool = False) -> pd.DataFrame | None:
    """Read a file into a pandas DataFrame using existing scanners."""
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        return read_csv(file_path)
    elif suffix in {".xlsx", ".xls"}:
        return read_excel(file_path, quiet=quiet)
    elif suffix == ".json":
        return _read_json(file_path, quiet=quiet)
    elif suffix in {".sas7bdat", ".sav", ".dta"}:
        return read_statistical(file_path, quiet=quiet)
    return None


def _read_json(file_path: Path, *, quiet: bool = False) -> pd.DataFrame | None:
    """Read JSON file into pandas DataFrame."""
    try:
        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)

        # Handle both array and object with entity key
        if isinstance(data, dict):
            for key in data:
                if isinstance(data[key], list) and data[key]:
                    data = data[key]
                    break

        if not isinstance(data, list) or not data:
            return None

        return pd.DataFrame(data)
    except Exception as e:
        log_warn(f"Could not read JSON '{file_path.name}': {e}", quiet)
        return None


def _load_tables_from_folder(
    folder_path: Path,
    allowed_entities: set[str],
    *,
    quiet: bool = False,
) -> dict[str, tuple[pd.DataFrame, str]]:
    """Load entity files from a folder. Returns dict of (DataFrame, filename)."""
    tables: dict[str, tuple[pd.DataFrame, str]] = {}

    for entity_name in allowed_entities:
        for ext in SUPPORTED_EXTENSIONS:
            file_path = folder_path / f"{entity_name}{ext}"
            if file_path.exists():
                df = _read_file(file_path, quiet=quiet)
                if df is not None and not df.empty:
                    tables[entity_name] = (df, file_path.name)
                break

    return tables


def _load_tables_from_database(
    connection: str,
    allowed_entities: set[str],
    *,
    quiet: bool = False,
) -> dict[str, tuple[pd.DataFrame, str]]:
    """Load entity tables from a database. Returns dict of (DataFrame, table_name)."""
    tables: dict[str, tuple[pd.DataFrame, str]] = {}

    try:
        con = ibis.connect(connection)
    except Exception as e:
        log_warn(f"Could not connect to database: {e}", quiet)
        return tables

    try:
        available_tables = set(con.list_tables())

        for entity_name in allowed_entities:
            if entity_name in available_tables:
                try:
                    table = con.table(entity_name)
                    tables[entity_name] = (table.to_pandas(), f"table '{entity_name}'")
                except Exception as e:
                    log_warn(f"Could not read table '{entity_name}': {e}", quiet)
    finally:
        if hasattr(con, "disconnect"):
            con.disconnect()

    return tables


def _parse_list_field(value: Any) -> list[str]:
    """Parse a list field value (comma-separated string or list)."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value if v is not None]
    if isinstance(value, str):
        if not value.strip():
            return []
        return [v.strip() for v in value.split(",") if v.strip()]
    return []


def _convert_row_to_dict(
    row: dict[Hashable, Any], entity_class: type
) -> dict[str, Any]:
    """Convert a row dict to entity constructor kwargs."""
    # Get valid field names for this entity
    valid_fields = {f.name for f in fields(entity_class)}

    result: dict[str, Any] = {}
    for key, value in row.items():
        key_str = str(key)
        if key_str not in valid_fields:
            continue

        # Handle None/NaN values
        if value is None or (isinstance(value, float) and value != value):  # NaN check
            continue

        # Handle list fields
        if key_str in LIST_FIELDS:
            parsed = _parse_list_field(value)
            if parsed:
                result[key_str] = parsed
        else:
            result[key_str] = value

    return result


def _merge_entity(
    existing: Any,
    new_data: dict[str, Any],
) -> None:
    """Merge new data into existing entity (override + merge lists)."""
    # Mark entity as seen for incremental scan
    if hasattr(existing, "_seen"):
        existing._seen = True

    for key, value in new_data.items():
        if key == "id":
            continue  # Never override id

        if key in LIST_FIELDS:
            # Merge lists: new values first, then existing (deduplicated)
            existing_list = getattr(existing, key, []) or []
            new_list = value if isinstance(value, list) else []
            merged = list(
                dict.fromkeys(new_list + existing_list)
            )  # Preserve order, dedupe
            setattr(existing, key, merged)
        else:
            # Override with new value
            setattr(existing, key, value)


def _compute_merge_updates(
    existing: Any,
    new_data: dict[str, Any],
) -> dict[str, Any]:
    """Compute update dict for Table.update() (override + merge lists)."""
    updates: dict[str, Any] = {}

    for key, value in new_data.items():
        if key == "id":
            continue  # Never override id

        if key in LIST_FIELDS:
            # Merge lists: new values first, then existing (deduplicated)
            existing_list = getattr(existing, key, []) or []
            new_list = value if isinstance(value, list) else []
            merged = list(dict.fromkeys(new_list + existing_list))
            updates[key] = merged
        else:
            # Override with new value
            updates[key] = value

    # Mark entity as seen for incremental scan
    if hasattr(existing, "_seen"):
        updates["_seen"] = True

    return updates


def _find_entity_by_id(
    entities: list[Any],
    entity_id: str,
) -> Any | None:
    """Find entity by id in list."""
    for entity in entities:
        if entity.id == entity_id:
            return entity
    return None


def _find_value(
    values: list[Value],
    modality_id: str,
    value: str,
) -> Value | None:
    """Find Value by composite key."""
    for v in values:
        if v.modality_id == modality_id and v.value == value:
            return v
    return None


def _process_entity_table(
    catalog: Catalog,
    entity_name: str,
    df: pd.DataFrame,
) -> tuple[int, int]:
    """Process a single entity DataFrame and merge into catalog."""
    entity_class = ENTITY_CLASSES[entity_name]
    created = 0
    updated = 0

    # Get the appropriate catalog table
    catalog_table = _get_catalog_table(catalog, entity_name)
    assert catalog_table is not None  # entity_name is always valid

    # Convert DataFrame to list of dicts
    rows = df.to_dict(orient="records")

    for row in rows:
        row_data = _convert_row_to_dict(row, entity_class)

        if entity_name == "value":
            # Value uses composite key (modality_id, value)
            modality_id = row_data.get("modality_id")
            value_str = row_data.get("value")
            if modality_id is None or value_str is None:
                continue

            value_id = build_value_id(str(modality_id), str(value_str))
            existing = catalog.value.get(value_id)
            if existing:
                # Update using Table.update()
                updates = {}
                if row_data.get("description") is not None:
                    updates["description"] = row_data["description"]
                if updates:
                    catalog.value.update(value_id, **updates)
                updated += 1
            else:
                new_value = Value(
                    id=value_id,
                    modality_id=str(modality_id),
                    value=str(value_str),
                    description=row_data.get("description"),
                )
                catalog.value.add(new_value)
                created += 1
                # Mark parent modality as seen
                modality = catalog.modality.get(str(modality_id))
                if modality and hasattr(modality, "_seen"):
                    catalog.modality.update(str(modality_id), _seen=True)

        else:
            # Standard entity with id
            entity_id = row_data.get("id")
            if entity_id is None:
                continue

            entity_id = str(entity_id)
            row_data["id"] = entity_id

            # For Variable: infer name from id if not provided
            if entity_name == "variable" and "name" not in row_data:
                # id format: folder---dataset---variable_name
                parts = entity_id.split("---")
                row_data["name"] = parts[-1]

            existing = catalog_table.get(entity_id)
            if existing:
                # Compute updates and use Table.update()
                updates = _compute_merge_updates(existing, row_data)
                catalog_table.update(entity_id, **updates)
                updated += 1
            else:
                # Validation already done, just create the entity
                new_entity = entity_class(**row_data)
                # Mark new entity as seen for incremental scan
                if hasattr(new_entity, "_seen"):
                    new_entity._seen = True
                catalog_table.add(new_entity)
                created += 1

    return created, updated


def _get_catalog_table(catalog: Catalog, entity_name: str) -> Any | None:
    """Get the appropriate jsonjsdb table from catalog for an entity type."""
    mapping = {
        "folder": catalog.folder,
        "dataset": catalog.dataset,
        "variable": catalog.variable,
        "modality": catalog.modality,
        "value": catalog.value,
        "institution": catalog.institution,
        "tag": catalog.tag,
        "doc": catalog.doc,
    }
    return mapping.get(entity_name)


@validate_params
def add_metadata(
    self: Catalog,
    path: str | Path,
    *,
    depth: Literal["structure", "schema", "full"] | None = None,
    quiet: bool | None = None,
) -> None:
    """Load manually curated metadata from files or database.

    Args:
        path: Folder containing metadata files or database connection string.
        depth: Control which entities to load (structure/schema/full).
        quiet: Suppress progress logging.
    """
    if quiet is None:
        quiet = self.quiet
    resolved_depth = depth if depth is not None else self.depth
    allowed_entities = DEPTH_ENTITIES[resolved_depth]

    path_str = str(path)

    # Load tables from source
    if _is_database_connection(path_str):
        start_time = log_section("add_metadata", path_str, quiet)
        tables = _load_tables_from_database(path_str, allowed_entities, quiet=quiet)
    else:
        folder_path = Path(path)
        if not folder_path.exists():
            raise FileNotFoundError(f"Metadata folder not found: {folder_path}")
        if not folder_path.is_dir():
            raise ValueError(f"Path must be a directory: {folder_path}")

        start_time = log_section("add_metadata", str(folder_path), quiet)
        tables = _load_tables_from_folder(folder_path, allowed_entities, quiet=quiet)

    if not tables:
        log_warn("No metadata files found", quiet)
        return

    # Validate all tables before processing
    errors = _validate_all_tables(self, tables)
    if errors:
        s = "s" if len(errors) > 1 else ""
        log_warn(f"Invalid metadata - {len(errors)} error{s}:", quiet)
        for err in errors:
            print(f"    • {err}", file=sys.stderr)
        return

    # Process each entity table
    total_created = 0
    total_updated = 0

    for entity_name, (table, _file_name) in tables.items():
        created, updated = _process_entity_table(self, entity_name, table)
        total_created += created
        total_updated += updated

        if not quiet and (created or updated):
            log_done(f"{entity_name}: {created} created, {updated} updated", quiet)

    log_summary_metadata(total_created, total_updated, quiet, start_time)


def log_summary_metadata(
    created: int, updated: int, quiet: bool, start_time: float
) -> None:
    """Log final summary for metadata loading."""
    if quiet:
        return
    elapsed = time.perf_counter() - start_time
    print(
        f"  → {created} created, {updated} updated in {elapsed:.1f}s",
        file=sys.stderr,
    )
