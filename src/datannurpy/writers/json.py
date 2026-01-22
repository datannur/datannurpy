"""JSON writer for datannur format."""

from __future__ import annotations

import json
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Protocol

import pyarrow as pa

from ..entities.base import Entity


class HasToDict(Protocol):
    """Protocol for objects with to_dict method."""

    def to_dict(self) -> dict[str, Any]: ...


def write_json(
    entities: Sequence[Entity],
    name: str,
    output_dir: str | Path,
    *,
    write_js: bool = True,
) -> Path:
    """Write entities to {name}.json and {name}.json.js files."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Convert entities to dicts
    data = [entity.to_dict() for entity in entities]

    # Clean data (convert floats that are integers)
    data = [_clean_record(record) for record in data]

    # Write .json (pretty-printed)
    json_path = output_dir / f"{name}.json"
    _write_atomic(json_path, json.dumps(data, ensure_ascii=False, indent=2))

    # Write .json.js (compact array format for jsonjsdb)
    if write_js and data:
        jsonjs_path = output_dir / f"{name}.json.js"
        jsonjs_content = _build_jsonjs(data, name)
        _write_atomic(jsonjs_path, jsonjs_content)

    return json_path


def write_values_json(
    values: Sequence[HasToDict],
    output_dir: str | Path,
    *,
    write_js: bool = True,
) -> Path:
    """Write Value objects to value.json and value.json.js files."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    data = [v.to_dict() for v in values]

    json_path = output_dir / "value.json"
    _write_atomic(json_path, json.dumps(data, ensure_ascii=False, indent=2))

    if write_js and data:
        jsonjs_path = output_dir / "value.json.js"
        jsonjs_content = _build_jsonjs(
            data, "value", columns=["modality_id", "value", "description"]
        )
        _write_atomic(jsonjs_path, jsonjs_content)

    return json_path


def _clean_record(record: dict[str, Any]) -> dict[str, Any]:
    """Clean record for JSON output."""
    cleaned = {}
    for key, value in record.items():
        cleaned[key] = _clean_value(value)
    return cleaned


def _clean_value(value: Any) -> Any:
    """Clean value for JSON (convert whole floats to int)."""
    # Convert float to int if it's a whole number
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _build_jsonjs(
    data: list[dict[str, Any]], name: str, *, columns: list[str] | None = None
) -> str:
    """Build jsonjs format: [columns, row1, row2, ...]."""
    if not data:
        return f"jsonjs.data['{name}'] = []"

    # Use provided columns or discover from data
    if columns is None:
        columns = []
        seen: set[str] = set()
        for record in data:
            for key in record.keys():
                if key not in seen:
                    columns.append(key)
                    seen.add(key)

    # Build array format: [columns, row1, row2, ...]
    rows: list[list[Any]] = [columns]
    for record in data:
        row = [record.get(col) for col in columns]
        rows.append(row)

    # Minified JSON
    json_minified = json.dumps(rows, ensure_ascii=False, separators=(",", ":"))

    return f"jsonjs.data['{name}'] = {json_minified}"


def _write_atomic(path: Path, content: str) -> None:
    """Write file atomically (temp + rename)."""
    temp_path = path.with_suffix(path.suffix + ".temp")
    try:
        with open(temp_path, "w", encoding="utf-8") as f:
            f.write(content)
        temp_path.rename(path)
    except Exception:
        # Cleanup temp file on error
        temp_path.unlink(missing_ok=True)
        raise


def write_freq_json(
    freq_table: pa.Table,
    output_dir: str | Path,
    *,
    write_js: bool = True,
) -> Path:
    """Write freq table to freq.json and freq.json.js files."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Convert to list of dicts
    data: list[dict[str, Any]] = freq_table.to_pylist()

    # Write .json (pretty-printed)
    json_path = output_dir / "freq.json"
    _write_atomic(json_path, json.dumps(data, ensure_ascii=False, indent=2))

    # Write .json.js (compact array format for jsonjsdb)
    if write_js and data:
        jsonjs_path = output_dir / "freq.json.js"
        # Fixed column order for freq table
        jsonjs_content = _build_jsonjs(
            data, "freq", columns=["variable_id", "value", "freq"]
        )
        _write_atomic(jsonjs_path, jsonjs_content)

    return json_path


def write_table_registry(
    output_dir: Path, tables: list[str], *, write_js: bool = True
) -> None:
    """Write __table__.json registry for jsonjsdb."""
    now = int(time.time())

    # Build table registry
    registry = [{"name": name, "last_modif": now} for name in tables]
    registry.append({"name": "__table__", "last_modif": now})

    # Write __table__.json
    json_path = output_dir / "__table__.json"
    _write_atomic(json_path, json.dumps(registry, ensure_ascii=False, indent=2))

    # Write __table__.json.js
    if write_js:
        jsonjs_path = output_dir / "__table__.json.js"
        jsonjs_content = _build_jsonjs(
            registry, "__table__", columns=["name", "last_modif"]
        )
        _write_atomic(jsonjs_path, jsonjs_content)
