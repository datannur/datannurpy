"""Load manually curated metadata from files or database."""

from __future__ import annotations

import json
import re
import sys
import time
from collections.abc import Hashable
from dataclasses import MISSING, fields
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple
from urllib.parse import urlparse

import ibis
import polars as pl

from .schema import (
    Config,
    ConfigFilter,
    Concept,
    Dataset,
    Doc,
    Folder,
    Enumeration,
    Frequency,
    Organization,
    Tag,
    Value,
    Variable,
)
from .scanner.csv import read_csv
from .scanner.excel import read_excel
from .scanner.statistical import read_statistical
from .utils import log_done, log_error, log_section, log_warn, timestamp_to_iso
from .utils.ids import build_frequency_id, build_value_id
from .utils.params import validate_params
from .errors import ConfigError

if TYPE_CHECKING:
    import pandas as pd

    from .catalog import Catalog, Depth

# Entity type to class mapping
ENTITY_CLASSES: dict[str, type] = {
    "folder": Folder,
    "dataset": Dataset,
    "variable": Variable,
    "enumeration": Enumeration,
    "value": Value,
    "frequency": Frequency,
    "organization": Organization,
    "tag": Tag,
    "doc": Doc,
    "concept": Concept,
    "configFilter": ConfigFilter,
}

# Entities without required id (use composite key)
ENTITIES_WITHOUT_ID = {"value", "frequency"}

TOMBSTONE_ENTITIES = {
    "folder",
    "dataset",
    "variable",
    "enumeration",
    "organization",
    "tag",
    "doc",
    "concept",
}

# List fields that should be merged (union)
LIST_FIELDS = {
    "tag_ids",
    "doc_ids",
    "enumeration_ids",
    "source_variable_ids",
    "implied_tag_ids",
}

BOOL_FIELDS = {"propagate_to_parents", "is_active_default"}

LOCALIZED_FIELD_RE = re.compile(
    r"^(?P<field>[A-Za-z_][A-Za-z0-9_]*):(?P<lang>[A-Za-z]{2,3}(?:-[A-Za-z0-9]+)*)$"
)

CLEAR_VALUE = "!"
REMOVE_PREFIX = "!"


class _ClearList:
    """Sentinel for explicit relation clear instructions."""

    def __repr__(self) -> str:
        return "CLEAR_LIST"


_CLEAR_LIST = _ClearList()

# Policy tag IDs
FREQ_HIDDEN_TAG = "policy---frequency-hidden"
LOCALIZED_CLEAR_SENTINEL = "__DATANNURPY_LOCALIZED_CLEAR__"

# Supported file extensions for metadata
SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls", ".json", ".sas7bdat"}

# Entities allowed per depth level
_DATASET_ENTITIES = {
    "folder",
    "dataset",
    "organization",
    "tag",
    "doc",
    "concept",
    "config",
    "configFilter",
}
_VARIABLE_ENTITIES = _DATASET_ENTITIES | {"variable"}
_VALUE_ENTITIES = _VARIABLE_ENTITIES | {"enumeration", "value", "frequency"}
DEPTH_ENTITIES: dict[str, set[str]] = {
    "dataset": _DATASET_ENTITIES,
    "variable": _VARIABLE_ENTITIES,
    "stat": _VARIABLE_ENTITIES,
    "value": _VALUE_ENTITIES,
}


@lru_cache(maxsize=None)
def _get_required_fields(entity_class: type) -> frozenset[str]:
    """Get required field names for an entity class (fields without defaults)."""
    return frozenset(
        f.name
        for f in fields(entity_class)
        if f.default is MISSING and f.default_factory is MISSING
    )


@lru_cache(maxsize=None)
def _get_field_names(entity_class: type) -> frozenset[str]:
    """Get all field names for an entity class."""
    return frozenset(f.name for f in fields(entity_class))


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
    check_required = set(required)
    if entity_name == "variable" and "id" in csv_columns:
        check_required.discard("name")

    missing_columns = check_required - csv_columns
    if missing_columns:
        missing_str = ", ".join(sorted(missing_columns))
        errors.append(f"{file_name}: missing column(s) {missing_str}")
        return errors  # No point checking rows if columns are missing

    # Pre-compute existing ids once (O(1) lookup per row)
    if not catalog_table.df.is_empty() and "id" in catalog_table.df.columns:
        existing_ids = set(catalog_table.df["id"].to_list())
    else:
        existing_ids = set()

    # Check for empty required values in each row (for new entities only)
    rows = df.to_dict(orient="records")
    for row_idx, row in enumerate(rows):
        row_data = _convert_row_to_dict(row, entity_class)

        entity_id = row_data.get("id")
        if entity_id is None:
            continue

        # Check if entity already exists (will be updated, not created)
        if str(entity_id) in existing_ids:
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


def _read_file(
    file_path: Path, *, quiet: bool = False, path_label: str | None = None
) -> pd.DataFrame | None:
    """Read a file into a pandas DataFrame using existing scanners."""
    suffix = file_path.suffix.lower()
    label = path_label or file_path.name

    if suffix == ".csv":
        return read_csv(file_path)
    elif suffix in {".xlsx", ".xls"}:
        return read_excel(file_path, quiet=quiet, path_label=label)
    elif suffix == ".json":
        return _read_json(file_path, quiet=quiet, path_label=label)
    elif suffix in {".sas7bdat", ".sav", ".dta"}:
        return read_statistical(file_path, quiet=quiet, path_label=label)
    return None


def _read_json(
    file_path: Path, *, quiet: bool = False, path_label: str | None = None
) -> pd.DataFrame | None:
    """Read JSON file into pandas DataFrame."""
    label = path_label or file_path.name
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

        import pandas as pd

        return pd.DataFrame(data)
    except Exception as e:
        log_error(label, e, quiet)
        return None


def _metadata_file_label(file_path: Path, folder_path: Path) -> str:
    """Return a metadata file label relative to the metadata source parent."""
    try:
        return file_path.relative_to(folder_path.parent).as_posix()
    except ValueError:
        return file_path.name


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
                file_label = _metadata_file_label(file_path, folder_path)
                df = _read_file(file_path, quiet=quiet, path_label=file_label)
                if df is not None and not df.empty:
                    tables[entity_name] = (df, file_path.name)
                break

    # Resolve the scan↔metadata match key. An explicit dataset._match_path is
    # strict and overrides data_path for the whole table; data_path stays as-is
    # in the export and is only used as the backward-compatible fallback.
    if "dataset" in tables:
        df, fname = tables["dataset"]
        if "_match_path" in df.columns:
            df = df.copy()
            df["_match_path"] = df["_match_path"].map(
                lambda p: _resolve_match_path(p, folder_path)
            )
            tables["dataset"] = (df, fname)
        elif "data_path" in df.columns:
            df = df.copy()
            df["_match_path"] = df["data_path"].map(
                lambda p: _resolve_match_path(p, folder_path)
            )
            tables["dataset"] = (df, fname)

    return tables


def _resolve_match_path(data_path: Any, base_dir: Path) -> str | None:
    """Resolve a CSV data_path to an absolute match path (or None if URL/missing)."""
    import pandas as pd

    if data_path is None or (isinstance(data_path, float) and pd.isna(data_path)):
        return None
    s = str(data_path).strip()
    if not s or "://" in s:
        return None
    candidate = Path(s)
    if not candidate.is_absolute():
        candidate = (base_dir / candidate).resolve()
    return str(candidate) if candidate.exists() else None


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
        log_error("database", e, quiet)
        return tables

    try:
        available_tables = set(con.list_tables())

        for entity_name in allowed_entities:
            if entity_name in available_tables:
                try:
                    table = con.table(entity_name)
                    tables[entity_name] = (table.to_pandas(), f"table '{entity_name}'")
                except Exception as e:
                    log_error(entity_name, e, quiet)
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


def _is_clear_value(value: Any) -> bool:
    """Return whether a metadata value is the explicit clear marker."""
    return isinstance(value, str) and value.strip() == CLEAR_VALUE


def _split_relation_instructions(values: list[str]) -> tuple[list[str], set[str]]:
    """Split relation list entries into additions and removals."""
    removals = {
        value.removeprefix(REMOVE_PREFIX)
        for value in values
        if value.startswith(REMOVE_PREFIX) and value != REMOVE_PREFIX
    }
    additions = [
        value
        for value in values
        if value != REMOVE_PREFIX
        and not value.startswith(REMOVE_PREFIX)
        and value not in removals
    ]
    return additions, removals


def _resolve_relation_list(existing_list: list[str], new_list: list[str]) -> list[str]:
    """Apply relation additions/removals and return a deduplicated final list."""
    additions, removals = _split_relation_instructions(new_list)
    kept_existing = [value for value in existing_list if value not in removals]
    kept_additions = [value for value in additions if value not in removals]
    return list(dict.fromkeys(kept_additions + kept_existing))


def _is_truthy_delete(value: Any) -> bool:
    """Return whether a metadata _delete value is truthy."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False

    import pandas as pd

    if not isinstance(value, (list, dict)) and bool(pd.isna(value)):
        return False
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _parse_bool_field(value: Any) -> bool:
    """Parse a boolean metadata value."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _extract_tombstone_ids(
    tables: dict[str, tuple[pd.DataFrame, str]],
) -> dict[str, set[str]]:
    """Extract _delete tombstone ids from loaded metadata tables."""
    tombstones: dict[str, set[str]] = {}
    for entity_name, (df, _file_name) in tables.items():
        if entity_name not in TOMBSTONE_ENTITIES:
            continue
        if "id" not in df.columns or "_delete" not in df.columns:
            continue
        ids = set()
        for entity_id, delete_value in zip(df["id"].tolist(), df["_delete"].tolist()):
            if entity_id is not None and _is_truthy_delete(delete_value):
                ids.add(str(entity_id))
        if ids:
            tombstones[entity_name] = ids
    return tombstones


def _merge_tombstones(target: dict[str, set[str]], source: dict[str, set[str]]) -> None:
    """Merge extracted tombstones into the catalog-level tombstone map."""
    for entity_name, ids in source.items():
        target.setdefault(entity_name, set()).update(ids)


def _convert_row_to_dict(
    row: dict[Hashable, Any], entity_class: type
) -> dict[str, Any]:
    """Convert a row dict to entity constructor kwargs."""
    valid_fields = _get_field_names(entity_class)

    import pandas as pd

    result: dict[str, Any] = {}
    for key, value in row.items():
        key_str = str(key)
        if key_str not in valid_fields:
            continue

        # Skip missing values (None, NaN, pd.NA, pd.NaT, np.datetime64('NaT'))
        if value is None or (
            not isinstance(value, (list, dict)) and bool(pd.isna(value))
        ):
            continue

        if _is_clear_value(value):
            result[key_str] = _CLEAR_LIST if key_str in LIST_FIELDS else None
            continue

        if key_str in {"last_update", "last_update_date"}:
            value = _normalize_update_value(value)

        # Coerce datetime / date / pd.Timestamp to YYYY/MM/DD —
        # CSV (DuckDB) and Excel parsers infer date columns natively, but the
        # schema declares date fields as `str | None`. Date-only precision is
        # appropriate for user-supplied metadata. The format is lexicographically
        # sortable and `iso_to_timestamp` handles both YYYY/MM/DD and the full
        # YYYY/MM/DDTHH:MM:SS produced by filesystem scans.
        if isinstance(value, (datetime, date)):
            value = value.strftime("%Y/%m/%d")

        # Handle list fields
        if key_str in LIST_FIELDS:
            parsed = _parse_list_field(value)
            if parsed:
                result[key_str] = parsed
        elif key_str in BOOL_FIELDS:
            result[key_str] = _parse_bool_field(value)
        else:
            result[key_str] = value

    return result


def _is_missing_metadata_value(value: Any) -> bool:
    """Return whether a metadata cell should leave existing values unchanged."""
    import pandas as pd

    return value is None or (
        not isinstance(value, (list, dict)) and bool(pd.isna(value))
    )


def _localized_field_columns(columns: list[Hashable], entity_class: type) -> list[str]:
    """Return localized metadata columns such as name:fr or description:fr."""
    valid_fields = _get_field_names(entity_class)
    localized: list[str] = []
    for column in columns:
        column_name = str(column)
        match = LOCALIZED_FIELD_RE.match(column_name)
        if (
            match
            and match.group("field") in valid_fields
            and column_name not in localized
        ):
            localized.append(column_name)
    return localized


def _merge_localized_fields(
    catalog_table: Any,
    entity_class: type,
    rows: list[dict[str, Any]],
    key_fields: list[str],
) -> None:
    """Preserve localized metadata columns that are outside dataclass fields."""
    localized_columns = _localized_field_columns(
        [column for row in rows for column in row], entity_class
    )
    if not localized_columns or catalog_table.df.is_empty():
        return

    records_by_key: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in rows:
        key_values: list[str] = []
        for key_field in key_fields:
            value = row.get(key_field)
            if _is_missing_metadata_value(value):
                key_values = []
                break
            key_values.append(str(value))
        if not key_values:
            continue

        key = tuple(key_values)
        record = records_by_key.setdefault(key, dict(zip(key_fields, key_values)))
        for column in localized_columns:
            value = row.get(column)
            if _is_missing_metadata_value(value):
                continue
            record[column] = (
                LOCALIZED_CLEAR_SENTINEL if _is_clear_value(value) else value
            )

    records = [
        record for record in records_by_key.values() if len(record) > len(key_fields)
    ]
    if not records:  # pragma: no cover - no-op guard for malformed metadata rows
        return

    localized_df = pl.DataFrame(records)
    if localized_df.is_empty():  # pragma: no cover - defensive after records guard
        return

    df = catalog_table._df
    for column in localized_columns:
        if column not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(column))
    joined = df.join(
        localized_df.select(key_fields + localized_columns),
        on=key_fields,
        how="left",
        suffix="__localized",
    )
    for column in localized_columns:
        update_column = f"{column}__localized"
        joined = joined.with_columns(
            pl.when(pl.col(update_column) == LOCALIZED_CLEAR_SENTINEL)
            .then(pl.lit(None, dtype=pl.Utf8))
            .otherwise(pl.coalesce(pl.col(update_column), pl.col(column)))
            .alias(column)
        ).drop(update_column)

    catalog_table._df = joined


def _string_keyed_rows(rows: list[dict[Hashable, Any]]) -> list[dict[str, Any]]:
    """Return row dictionaries with string keys for localized field handling."""
    return [{str(key): value for key, value in row.items()} for row in rows]


def _existing_localized_rows(
    catalog_table: Any,
    entity_class: type,
    key_fields: list[str],
    ids: set[str],
) -> list[dict[str, Any]]:
    """Return localized values already present on rows about to be replaced."""
    df = catalog_table._df
    localized_columns = _localized_field_columns(df.columns, entity_class)
    if not localized_columns or df.is_empty():
        return []

    records: list[dict[str, Any]] = []
    for row in df.select(key_fields + localized_columns).to_dicts():
        key = "---".join(str(row.get(field)) for field in key_fields)
        if key not in ids:
            continue
        record = {field: row[field] for field in key_fields}
        for column in localized_columns:
            value = row.get(column)
            if not _is_missing_metadata_value(value):
                record[column] = value
        if len(record) > len(key_fields):
            records.append(record)
    return records


def _normalize_update_value(value: Any) -> Any:
    """Normalize update timestamps while preserving manual date precision."""
    if isinstance(value, datetime):
        return value.strftime("%Y/%m/%dT%H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y/%m/%d")
    if isinstance(value, (int, float)):
        return timestamp_to_iso(value)
    return value


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
            if value is _CLEAR_LIST:
                setattr(existing, key, [])
                continue
            new_list = value if isinstance(value, list) else []
            if new_list:
                existing_list = getattr(existing, key, []) or []
                setattr(existing, key, _resolve_relation_list(existing_list, new_list))
        else:
            # Override with new value
            setattr(existing, key, value)


def _process_entity_table(
    catalog: Catalog,
    entity_name: str,
    df: pd.DataFrame,
) -> tuple[int, int]:
    """Process a single entity DataFrame and merge into catalog (batched)."""
    # Convert DataFrame to list of dicts once
    rows = df.to_dict(orient="records")

    if entity_name == "value":
        return _process_value_table(catalog, rows)
    if entity_name == "frequency":
        return _process_frequency_table(catalog, rows)
    return _process_standard_table(catalog, entity_name, rows)


def _process_standard_table(
    catalog: Catalog,
    entity_name: str,
    rows: list[dict[Hashable, Any]],
) -> tuple[int, int]:
    """Process an id-keyed entity table in batch mode.

    Builds an in-memory lookup of existing entities, merges all rows, then
    applies a single remove_all + add_all per group (O(N) instead of O(N²)).
    """
    entity_class = ENTITY_CLASSES[entity_name]
    catalog_table = _get_catalog_table(catalog, entity_name)
    assert catalog_table is not None  # entity_name is always valid

    existing_map: dict[str, Any] = {e.id: e for e in catalog_table.all()}
    updated_by_id: dict[str, Any] = {}
    new_by_id: dict[str, Any] = {}

    for row in rows:
        row_data = _convert_row_to_dict(row, entity_class)
        entity_id = row_data.get("id")
        if entity_id is None:
            continue
        entity_id = str(entity_id)
        row_data["id"] = entity_id

        # For Variable: infer name from id if not provided
        if entity_name == "variable" and "name" not in row_data:
            row_data["name"] = entity_id.split("---")[-1]

        existing = existing_map.get(entity_id)
        if existing is not None:
            # Mutate in-place (sets _seen, merges list fields, overrides scalars)
            _merge_entity(existing, row_data)
            updated_by_id[entity_id] = existing
        elif entity_id in new_by_id:
            # Same id appears twice in the CSV for a brand-new entity: merge
            _merge_entity(new_by_id[entity_id], row_data)
        else:
            row_data = {
                key: (
                    []
                    if value is _CLEAR_LIST
                    else _resolve_relation_list([], value)
                    if key in LIST_FIELDS and isinstance(value, list)
                    else value
                )
                for key, value in row_data.items()
            }
            new_entity = entity_class(**row_data)
            if hasattr(new_entity, "_seen"):
                new_entity._seen = True
            new_by_id[entity_id] = new_entity

    created = len(new_by_id)
    updated = len(updated_by_id)
    existing_localized_rows = _existing_localized_rows(
        catalog_table, entity_class, ["id"], set(updated_by_id)
    )

    if updated_by_id:
        catalog_table.remove_all(list(updated_by_id.keys()))
        catalog_table.add_all(list(updated_by_id.values()))
    if new_by_id:
        catalog_table.add_all(list(new_by_id.values()))

    _merge_localized_fields(
        catalog_table,
        entity_class,
        existing_localized_rows + _string_keyed_rows(rows),
        ["id"],
    )

    return created, updated


def _process_value_table(
    catalog: Catalog,
    rows: list[dict[Hashable, Any]],
) -> tuple[int, int]:
    """Batch-process the value table (composite key: enumeration_id + value)."""
    existing_map: dict[str, Value] = {v.id: v for v in catalog.value.all()}
    enumeration_ids: set[str] = (
        set(catalog.enumeration.df["id"].to_list())
        if (
            not catalog.enumeration.df.is_empty()
            and "id" in catalog.enumeration.df.columns
        )
        else set()
    )

    updated_by_id: dict[str, Value] = {}
    new_by_id: dict[str, Value] = {}
    enumerations_to_mark: set[str] = set()

    for row in rows:
        row_data = _convert_row_to_dict(row, Value)
        enumeration_id = row_data.get("enumeration_id")
        value_str = row_data.get("value")
        if enumeration_id is None or value_str is None:
            continue

        enumeration_id = str(enumeration_id)
        value_str = str(value_str)
        value_id = build_value_id(enumeration_id, value_str)
        description = row_data.get("description")

        if value_id in existing_map:
            target = existing_map[value_id]
            if description is not None:
                target.description = description
            updated_by_id[value_id] = target
        elif value_id in new_by_id:
            if description is not None:
                new_by_id[value_id].description = description
        else:
            new_by_id[value_id] = Value(
                id=value_id,
                enumeration_id=enumeration_id,
                value=value_str,
                description=description,
            )
            if enumeration_id in enumeration_ids:
                enumerations_to_mark.add(enumeration_id)

    created = len(new_by_id)
    updated = len(updated_by_id)
    existing_localized_rows = _existing_localized_rows(
        catalog.value, Value, ["enumeration_id", "value"], set(updated_by_id)
    )

    if updated_by_id:
        catalog.value.remove_all(list(updated_by_id.keys()))
        catalog.value.add_all(list(updated_by_id.values()))
    if new_by_id:
        catalog.value.add_all(list(new_by_id.values()))
    if enumerations_to_mark:
        catalog.enumeration.update_many(list(enumerations_to_mark), _seen=True)

    _merge_localized_fields(
        catalog.value,
        Value,
        existing_localized_rows + _string_keyed_rows(rows),
        ["enumeration_id", "value"],
    )

    return created, updated


def _process_frequency_table(
    catalog: Catalog,
    rows: list[dict[Hashable, Any]],
) -> tuple[int, int]:
    """Batch-process the frequency table (composite key: variable_id + value)."""
    existing_map: dict[str, Frequency] = {f.id: f for f in catalog.frequency.all()}

    updated_by_id: dict[str, Frequency] = {}
    new_by_id: dict[str, Frequency] = {}

    for row in rows:
        row_data = _convert_row_to_dict(row, Frequency)
        variable_id = row_data.get("variable_id")
        value_str = row_data.get("value")
        if variable_id is None or value_str is None:
            continue

        variable_id = str(variable_id)
        value_str = str(value_str)
        freq_id = build_frequency_id(variable_id, value_str)
        freq_count = int(row_data.get("frequency", 0))

        if freq_id in existing_map:
            target = existing_map[freq_id]
            target.frequency = freq_count
            updated_by_id[freq_id] = target
        elif freq_id in new_by_id:
            new_by_id[freq_id].frequency = freq_count
        else:
            new_by_id[freq_id] = Frequency(
                id=freq_id,
                variable_id=variable_id,
                value=value_str,
                frequency=freq_count,
            )

    created = len(new_by_id)
    updated = len(updated_by_id)
    existing_localized_rows = _existing_localized_rows(
        catalog.frequency, Frequency, ["variable_id", "value"], set(updated_by_id)
    )

    if updated_by_id:
        catalog.frequency.remove_all(list(updated_by_id.keys()))
        catalog.frequency.add_all(list(updated_by_id.values()))
    if new_by_id:
        catalog.frequency.add_all(list(new_by_id.values()))

    _merge_localized_fields(
        catalog.frequency,
        Frequency,
        existing_localized_rows + _string_keyed_rows(rows),
        ["variable_id", "value"],
    )

    return created, updated


def _get_catalog_table(catalog: Catalog, entity_name: str) -> Any | None:
    """Get the appropriate jsonjsdb table from catalog for an entity type."""
    mapping = {
        "folder": catalog.folder,
        "dataset": catalog.dataset,
        "variable": catalog.variable,
        "enumeration": catalog.enumeration,
        "value": catalog.value,
        "frequency": catalog.frequency,
        "organization": catalog.organization,
        "tag": catalog.tag,
        "doc": catalog.doc,
        "concept": catalog.concept,
        "configFilter": catalog.configFilter,
    }
    return mapping.get(entity_name)


def _load_tables(
    path: str | Path,
    allowed_entities: set[str],
    *,
    quiet: bool = False,
) -> dict[str, tuple[pd.DataFrame, str]]:
    """Load metadata tables from folder or database. Returns empty dict on error."""
    path_str = str(path)
    if _is_database_connection(path_str):
        return _load_tables_from_database(path_str, allowed_entities, quiet=quiet)
    folder_path = Path(path)
    if not folder_path.exists():
        raise ConfigError(f"Metadata folder not found: {folder_path}")
    if not folder_path.is_dir():
        raise ConfigError(f"Metadata source is not a directory: {folder_path}")
    return _load_tables_from_folder(folder_path, allowed_entities, quiet=quiet)


def _extract_freq_hidden_ids(
    tables: dict[str, tuple[pd.DataFrame, str]],
) -> set[str]:
    """Extract variable IDs tagged with policy---frequency-hidden."""
    if "variable" not in tables:
        return set()
    df, _ = tables["variable"]
    if "tag_ids" not in df.columns or "id" not in df.columns:
        return set()
    hidden: set[str] = set()
    for row in df.to_dict(orient="records"):
        var_id = row.get("id")
        tags = _parse_list_field(row.get("tag_ids"))
        if var_id and FREQ_HIDDEN_TAG in tags:
            hidden.add(str(var_id))
    return hidden


def _normalize_paths(
    path: str | Path | list[str | Path],
) -> list[str | Path]:
    """Normalize a metadata_path argument to a list of sources."""
    if isinstance(path, (list, tuple)):
        return list(path)
    return [path]


def load_metadata(
    catalog: Catalog,
    path: str | Path | list[str | Path],
) -> None:
    """Load metadata files into memory without applying to catalog tables."""
    allowed_entities = DEPTH_ENTITIES[catalog.depth]
    sources: list[dict[str, Any]] = []
    hidden: set[str] = set()
    for p in _normalize_paths(path):
        tables = _load_tables(p, allowed_entities, quiet=catalog.quiet)
        sources.append(tables)
        hidden |= _extract_freq_hidden_ids(tables)
    catalog._loaded_metadata = sources
    catalog._freq_hidden_ids = hidden
    # Reset the peek index; it will be rebuilt lazily on first lookup.
    catalog._dataset_match_index = None


class LoadedDatasetRef(NamedTuple):
    """Minimal reference to a dataset pre-loaded from metadata."""

    id: str
    folder_id: str | None


def _build_dataset_match_index(
    sources: list[dict[str, tuple[pd.DataFrame, str]]] | None,
) -> dict[str, LoadedDatasetRef]:
    """Build {abs_match_path: LoadedDatasetRef} from pre-loaded metadata sources."""
    index: dict[str, LoadedDatasetRef] = {}
    for source in sources or []:
        entry = source.get("dataset")
        if entry is not None:
            df = entry[0]
            if "_match_path" in df.columns and "id" in df.columns:
                for record in df.to_dict(orient="records"):
                    mp = _optional_str(record.get("_match_path"))
                    row_id = _optional_str(record.get("id"))
                    if mp is not None and row_id is not None:
                        index[mp] = LoadedDatasetRef(
                            id=row_id,
                            folder_id=_optional_str(record.get("folder_id")),
                        )
    return index


def _build_dataset_match_paths_by_id(
    sources: list[dict[str, tuple[pd.DataFrame, str]]] | None,
) -> dict[str, str]:
    """Build {dataset_id: abs_match_path} from pre-loaded metadata sources."""
    match_paths: dict[str, str] = {}
    for source in sources or []:
        entry = source.get("dataset")
        if entry is not None:
            df = entry[0]
            if "_match_path" in df.columns and "id" in df.columns:
                for record in df.to_dict(orient="records"):
                    mp = _optional_str(record.get("_match_path"))
                    row_id = _optional_str(record.get("id"))
                    if mp is not None and row_id is not None:
                        match_paths[row_id] = mp
    return match_paths


def _optional_str(value: Any) -> str | None:
    """Coerce a value to str, returning None for None/NaN/empty."""
    import pandas as pd

    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    return s if s else None


def find_loaded_dataset_by_match_path(
    catalog: Catalog, abs_match_path: str
) -> LoadedDatasetRef | None:
    """Return id/folder_id of pre-loaded dataset matching abs_match_path."""
    if catalog._dataset_match_index is None:
        catalog._dataset_match_index = _build_dataset_match_index(
            catalog._loaded_metadata
        )
    return catalog._dataset_match_index.get(abs_match_path)


def _apply_config_table(
    catalog: Catalog,
    df: pd.DataFrame,
    *,
    quiet: bool = False,
) -> None:
    """Populate catalog.config from a config metadata file.

    Skipped if catalog.config already has rows (user provided app_config).
    """
    if catalog.config.count > 0:
        return
    if "id" not in df.columns or "value" not in df.columns:
        log_warn("config: missing required columns 'id' and 'value'", quiet)
        return
    count = 0
    for row in df.to_dict(orient="records"):
        rid = row.get("id")
        val = row.get("value")
        if rid is not None and not (isinstance(rid, float) and rid != rid):
            if val is None or (isinstance(val, float) and val != val):
                val = ""
            catalog.config.add(Config(id=str(rid), value=str(val)))
            count += 1
    log_done(f"config: {count} entries loaded", quiet)


def _apply_tables(
    catalog: Catalog,
    tables: dict[str, tuple[pd.DataFrame, str]],
    *,
    quiet: bool = False,
    start_time: float,
) -> None:
    """Validate and merge pre-loaded tables into catalog."""
    if not tables:
        log_warn("No metadata files found", quiet)
        return

    # Handle config specially (not a merge-style entity)
    config_entry = tables.pop("config", None)
    if config_entry is not None:
        _apply_config_table(catalog, config_entry[0], quiet=quiet)

    if not tables:
        return

    errors = _validate_all_tables(catalog, tables)
    if errors:
        s = "s" if len(errors) > 1 else ""
        log_warn(f"Invalid metadata - {len(errors)} error{s}:", quiet)
        for err in errors:
            print(f"    • {err}", file=sys.stderr)
        return

    _merge_tombstones(
        catalog._metadata_tombstones,
        _extract_tombstone_ids(tables),
    )

    total_created = 0
    total_updated = 0

    for entity_name, (table, _file_name) in tables.items():
        created, updated = _process_entity_table(catalog, entity_name, table)
        total_created += created
        total_updated += updated

        if not quiet and (created or updated):
            log_done(f"{entity_name}: {created} created, {updated} updated", quiet)

    log_summary_metadata(total_created, total_updated, quiet, start_time)


@validate_params
def add_metadata(
    self: Catalog,
    path: str | Path | list[str | Path],
    *,
    depth: Depth | None = None,
    quiet: bool | None = None,
) -> None:
    """Load manually curated metadata from files or database."""
    if quiet is None:
        quiet = self.quiet
    resolved_depth = depth if depth is not None else self.depth
    allowed_entities = DEPTH_ENTITIES[resolved_depth]

    for p in _normalize_paths(path):
        start_time = log_section("add_metadata", str(p), quiet)
        tables = _load_tables(p, allowed_entities, quiet=quiet)
        _apply_tables(self, tables, quiet=quiet, start_time=start_time)
    apply_metadata_tombstones(self)


def ensure_metadata_applied(catalog: Catalog) -> None:
    """Apply pre-loaded metadata if configured and not yet applied."""
    if catalog._metadata_applied or catalog.metadata_path is None:
        return
    sources = getattr(catalog, "_loaded_metadata", None)
    paths = _normalize_paths(catalog.metadata_path)
    allowed_entities = DEPTH_ENTITIES[catalog.depth]
    for idx, p in enumerate(paths):
        start_time = log_section("add_metadata", str(p), catalog.quiet)
        if sources is not None and idx < len(sources):
            tables = sources[idx]
        else:
            tables = _load_tables(p, allowed_entities, quiet=catalog.quiet)
        _apply_tables(catalog, tables, quiet=catalog.quiet, start_time=start_time)
    catalog._metadata_applied = True


def apply_metadata_tombstones(catalog: Catalog) -> None:
    """Apply _delete metadata tombstones before export."""
    from .finalize import (
        remove_concepts_cascade,
        remove_datasets_cascade,
        remove_docs_cascade,
        remove_enumerations_cascade,
        remove_folders_cascade,
        remove_organizations_cascade,
        remove_tags_cascade,
        remove_variables_cascade,
    )

    tombstones = getattr(catalog, "_metadata_tombstones", None)
    if not tombstones:
        return

    if dataset_ids := tombstones.get("dataset"):
        remove_datasets_cascade(catalog, dataset_ids)
    if folder_ids := tombstones.get("folder"):
        remove_folders_cascade(catalog, folder_ids)
    if variable_ids := tombstones.get("variable"):
        remove_variables_cascade(catalog, variable_ids)
    if enumeration_ids := tombstones.get("enumeration"):
        remove_enumerations_cascade(catalog, enumeration_ids)
    if organization_ids := tombstones.get("organization"):
        remove_organizations_cascade(catalog, organization_ids)
    if tag_ids := tombstones.get("tag"):
        remove_tags_cascade(catalog, tag_ids)
    if doc_ids := tombstones.get("doc"):
        remove_docs_cascade(catalog, doc_ids)
    if concept_ids := tombstones.get("concept"):
        remove_concepts_cascade(catalog, concept_ids)


def log_summary_metadata(
    created: int, updated: int, quiet: bool, start_time: float
) -> None:
    """Log final summary for metadata loading."""
    if quiet:
        return
    elapsed = time.perf_counter() - start_time
    print(
        f"  →  {created} created, {updated} updated in {elapsed:.1f}s",
        file=sys.stderr,
    )
