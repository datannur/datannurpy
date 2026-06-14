"""Tests for Catalog.add_metadata."""

from __future__ import annotations

import sqlite3
import json
from collections.abc import Hashable
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import polars as pl
import pytest

from datannurpy import Catalog, EntityMetadata, Folder
from datannurpy.errors import ConfigError
from datannurpy.schema import (
    ConfigFilter,
    Concept,
    Dataset,
    Doc,
    Enumeration,
    Frequency,
    Organization,
    Tag,
    Value,
    Variable,
)
from datannurpy.utils.ids import build_frequency_id, build_value_id
from datannurpy.add_metadata import (
    DEPTH_ENTITIES,
    FREQ_HIDDEN_TAG,
    _CLEAR_LIST,
    _apply_config_table,
    _convert_row_to_dict,
    _existing_localized_rows,
    _extract_freq_hidden_ids,
    _extract_tombstone_ids,
    _get_catalog_table,
    _get_required_fields,
    _is_missing_metadata_value,
    _localized_field_columns,
    _merge_localized_fields,
    _normalize_integral_float_value,
    _is_clear_value,
    _is_database_connection,
    _is_truthy_delete,
    _load_tables_from_database,
    _load_tables_from_folder,
    _metadata_file_label,
    _merge_entity,
    _parse_list_field,
    _process_entity_table,
    _read_file,
    _read_json,
    _validate_all_tables,
    _validate_entity_table,
    add_metadata,
    ensure_metadata_applied,
)

ALL_ENTITIES = DEPTH_ENTITIES["value"]


class TestGetRequiredFields:
    """Test _get_required_fields function."""

    def test_variable_required_fields(self):
        """Variable should require id, name, dataset_id."""
        required = _get_required_fields(Variable)
        assert "id" in required
        assert "name" in required
        assert "dataset_id" in required
        # Optional fields should not be required
        assert "description" not in required
        assert "tag_ids" not in required

    def test_folder_required_fields(self):
        """Folder should only require id (name has default)."""
        required = _get_required_fields(Folder)
        assert required == {"id"}
        # name is optional (has default None)
        assert "name" not in required

    def test_value_required_fields(self):
        """Value has no required fields (all have defaults, id is computed)."""
        required = _get_required_fields(Value)
        assert "id" not in required  # id is a runtime field
        assert "enumeration_id" not in required  # has default ""


class TestIsDatabaseConnection:
    """Test _is_database_connection function."""

    def test_sqlite_connection(self):
        """SQLite connection strings should be recognized."""
        assert _is_database_connection("sqlite:///path/to/db.sqlite")
        assert _is_database_connection("sqlite:////absolute/path.db")

    def test_postgresql_connection(self):
        """PostgreSQL connection strings should be recognized."""
        assert _is_database_connection("postgresql://user:pass@host:5432/db")
        assert _is_database_connection("postgres://user:pass@host/db")

    def test_other_databases(self):
        """Other database connection strings should be recognized."""
        assert _is_database_connection("mysql://user:pass@host/db")
        assert _is_database_connection("oracle://user:pass@host/db")
        assert _is_database_connection("mssql://user:pass@host/db")

    def test_file_paths_not_database(self):
        """File paths should not be recognized as database connections."""
        assert not _is_database_connection("/path/to/folder")
        assert not _is_database_connection("./relative/path")
        assert not _is_database_connection("C:\\Windows\\path")

    def test_invalid_urls(self):
        """Invalid URLs should return False."""
        assert not _is_database_connection("not-a-url")
        assert not _is_database_connection("")


class TestParseListField:
    """Test _parse_list_field function."""

    def test_none_value(self):
        """None should return empty list."""
        assert _parse_list_field(None) == []

    def test_list_value(self):
        """List should be converted to strings."""
        assert _parse_list_field(["a", "b", "c"]) == ["a", "b", "c"]
        assert _parse_list_field([1, 2, 3]) == ["1", "2", "3"]

    def test_list_with_none(self):
        """None values in list should be filtered out."""
        assert _parse_list_field(["a", None, "b"]) == ["a", "b"]

    def test_comma_separated_string(self):
        """Comma-separated string should be split."""
        assert _parse_list_field("a, b, c") == ["a", "b", "c"]
        assert _parse_list_field("a,b,c") == ["a", "b", "c"]

    def test_empty_string(self):
        """Empty or whitespace string should return empty list."""
        assert _parse_list_field("") == []
        assert _parse_list_field("   ") == []

    def test_string_with_empty_parts(self):
        """Empty parts in comma-separated string should be filtered."""
        assert _parse_list_field("a,,b") == ["a", "b"]
        assert _parse_list_field(",a,") == ["a"]

    def test_other_types(self):
        """Other types should return empty list."""
        assert _parse_list_field(123) == []
        assert _parse_list_field({"key": "value"}) == []


class TestConvertRowToDict:
    """Test _convert_row_to_dict function."""

    def test_basic_conversion(self):
        """Basic row should be converted correctly."""
        row: dict[Hashable, Any] = {"id": "test", "name": "Test", "dataset_id": "ds"}
        result = _convert_row_to_dict(row, Variable)
        assert result == {"id": "test", "name": "Test", "dataset_id": "ds"}

    def test_ignores_invalid_fields(self):
        """Invalid field names should be ignored."""
        row: dict[Hashable, Any] = {
            "id": "test",
            "name": "Test",
            "dataset_id": "ds",
            "invalid_field": "x",
        }
        result = _convert_row_to_dict(row, Variable)
        assert "invalid_field" not in result

    def test_handles_none_values(self):
        """None values should be skipped."""
        row: dict[Hashable, Any] = {
            "id": "test",
            "name": "Test",
            "dataset_id": "ds",
            "description": None,
        }
        result = _convert_row_to_dict(row, Variable)
        assert "description" not in result

    def test_handles_nan_values(self):
        """NaN values should be skipped."""
        row: dict[Hashable, Any] = {
            "id": "test",
            "name": "Test",
            "dataset_id": "ds",
            "description": float("nan"),
        }
        result = _convert_row_to_dict(row, Variable)
        assert "description" not in result

    def test_handles_pandas_na_and_nat(self):
        """pd.NA and pd.NaT should be skipped (e.g. fully-empty CSV column)."""
        import pandas as pd

        row: dict[Hashable, Any] = {
            "id": "test",
            "name": "Test",
            "dataset_id": "ds",
            "nb_distinct": pd.NA,
            "description": pd.NaT,
        }
        result = _convert_row_to_dict(row, Variable)
        assert "nb_distinct" not in result
        assert "description" not in result

    def test_coerces_update_fields_to_datetime_when_precision_exists(self):
        """Update fields preserve time precision when available.

        DuckDB-based CSV reader and Excel parser auto-infer ISO-8601 columns
        as datetime; the schema declares date fields as `str | None`. Update
        fields keep seconds when the source has a time component, while true
        date-only values remain date-only.
        """
        import datetime as _dt

        import pandas as pd

        ts = pd.Timestamp("2026-04-23T12:11:38.685876")
        row: dict[Hashable, Any] = {
            "id": "f1",
            "name": "F1",
            "last_update_date": ts,
        }
        result = _convert_row_to_dict(row, Folder)
        assert result["last_update_date"] == "2026/04/23T12:11:38"
        assert isinstance(result["last_update_date"], str)

        row_doc_int: dict[Hashable, Any] = {
            "id": "doc2",
            "name": "Doc 2",
            "last_update": 1706239962,
        }
        result_doc_int = _convert_row_to_dict(row_doc_int, Doc)
        assert result_doc_int["last_update"] == "2024/01/26T03:32:42"

        row_doc_text: dict[Hashable, Any] = {
            "id": "doc3",
            "name": "Doc 3",
            "last_update": "2024/01/26T03:32:42",
        }
        result_doc_text = _convert_row_to_dict(row_doc_text, Doc)
        assert result_doc_text["last_update"] == "2024/01/26T03:32:42"

        row2: dict[Hashable, Any] = {
            "id": "f2",
            "name": "F2",
            "last_update_date": _dt.date(2026, 4, 23),
        }
        result2 = _convert_row_to_dict(row2, Folder)
        assert result2["last_update_date"] == "2026/04/23"

        row3: dict[Hashable, Any] = {
            "id": "d1",
            "name": "D1",
            "start_date": ts,
        }
        result3 = _convert_row_to_dict(row3, Dataset)
        assert result3["start_date"] == "2026/04/23"

    def test_handles_list_fields(self):
        """List fields should be parsed correctly."""
        row: dict[Hashable, Any] = {
            "id": "test",
            "name": "Test",
            "dataset_id": "ds",
            "tag_ids": "a,b,c",
        }
        result = _convert_row_to_dict(row, Variable)
        assert result["tag_ids"] == ["a", "b", "c"]

    def test_empty_list_fields_skipped(self):
        """Empty list fields should be skipped."""
        row: dict[Hashable, Any] = {
            "id": "test",
            "name": "Test",
            "dataset_id": "ds",
            "tag_ids": "",
        }
        result = _convert_row_to_dict(row, Variable)
        assert "tag_ids" not in result

    def test_clear_scalar_field(self):
        """Exact ! should clear scalar fields."""
        row: dict[Hashable, Any] = {
            "id": "test",
            "name": "Test",
            "dataset_id": "ds",
            "description": "!",
        }

        result = _convert_row_to_dict(row, Variable)

        assert result["description"] is None

    def test_strips_scalar_string_fields_and_skips_empty_results(self):
        """Scalar strings should be stripped and blank results treated as missing."""
        row: dict[Hashable, Any] = {
            "id": " test ",
            "name": " Test ",
            "dataset_id": " ds ",
            "description": "   ",
        }

        result = _convert_row_to_dict(row, Variable)

        assert result == {"id": "test", "name": "Test", "dataset_id": "ds"}

    def test_normalizes_integral_float_values(self):
        """Excel-style integral floats should not keep a trailing .0."""
        dataset_result = _convert_row_to_dict(
            {"id": 1.0, "name": 2022.0, "nb_row": 10.0}, Dataset
        )
        assert dataset_result == {"id": 1, "name": 2022, "nb_row": 10}

        variable_result = _convert_row_to_dict(
            {"id": "v1", "name": "Var", "dataset_id": "ds", "min": 1.0},
            Variable,
        )
        assert variable_result["min"] == 1

    def test_clear_marker_still_clears_id_field(self):
        """Exact ! should not become a literal id value."""
        result = _convert_row_to_dict({"id": "!", "name": "Bang"}, Folder)

        assert result["id"] is None

    def test_preserves_clear_marker_in_composite_value_fields(self):
        """Exact ! should remain literal in Value/Frequency value fields."""
        value_result = _convert_row_to_dict(
            {"enumeration_id": "enum1", "value": "!", "description": "!"}, Value
        )
        assert value_result["enumeration_id"] == "enum1"
        assert value_result["value"] == "!"
        assert value_result["description"] is None

        frequency_result = _convert_row_to_dict(
            {"variable_id": "var1", "value": "!", "frequency": 2}, Frequency
        )
        assert frequency_result["variable_id"] == "var1"
        assert frequency_result["value"] == "!"

    def test_preserves_empty_composite_value_fields(self):
        """Blank Value/Frequency values should remain valid category codes."""
        value_result = _convert_row_to_dict(
            {"enumeration_id": "enum1", "value": "   ", "description": " blank "},
            Value,
        )
        assert value_result == {
            "enumeration_id": "enum1",
            "value": "",
            "description": "blank",
        }

        frequency_result = _convert_row_to_dict(
            {"variable_id": "var1", "value": "   ", "frequency": 2}, Frequency
        )
        assert frequency_result["value"] == ""

    def test_clear_marker_still_clears_composite_reference_fields(self):
        """Exact ! should not become a literal reference id for composite entities."""
        assert (
            _convert_row_to_dict({"enumeration_id": "!", "value": "a"}, Value)[
                "enumeration_id"
            ]
            is None
        )
        assert (
            _convert_row_to_dict(
                {"variable_id": "!", "value": "a", "frequency": 1}, Frequency
            )["variable_id"]
            is None
        )

    def test_clear_list_field(self):
        """Exact ! should clear relation list fields."""
        row: dict[Hashable, Any] = {
            "id": "test",
            "name": "Test",
            "dataset_id": "ds",
            "tag_ids": "!",
        }

        result = _convert_row_to_dict(row, Variable)

        assert result["tag_ids"] is _CLEAR_LIST
        assert repr(result["tag_ids"]) == "CLEAR_LIST"

    def test_clear_value_detection(self):
        """Only exact ! strings are clear markers."""
        assert _is_clear_value("!")
        assert _is_clear_value(" ! ")
        assert not _is_clear_value("!tag")
        assert not _is_clear_value(["!"])

    def test_normalizes_integral_float_helper(self):
        """Integral floats from metadata readers should normalize cleanly."""
        assert _normalize_integral_float_value(1.0) == 1
        assert _normalize_integral_float_value(1.5) == 1.5
        assert _normalize_integral_float_value("1") == "1"


class TestMergeEntity:
    """Test _merge_entity function."""

    def test_override_simple_field(self):
        """Simple fields should be overridden."""
        entity = Variable(id="test", name="Old", dataset_id="ds")
        _merge_entity(entity, {"name": "New", "description": "Desc"})
        assert entity.name == "New"
        assert entity.description == "Desc"

    def test_id_never_overridden(self):
        """ID should never be overridden."""
        entity = Variable(id="test", name="Test", dataset_id="ds")
        _merge_entity(entity, {"id": "new_id", "name": "New"})
        assert entity.id == "test"

    def test_merge_list_fields(self):
        """List fields should be merged with deduplication."""
        entity = Variable(id="test", name="Test", dataset_id="ds", tag_ids=["a", "b"])
        _merge_entity(entity, {"tag_ids": ["b", "c"]})
        # New values first, then existing, deduplicated
        assert entity.tag_ids == ["b", "c", "a"]

    def test_merge_empty_existing_list(self):
        """Merging into empty list should work."""
        entity = Variable(id="test", name="Test", dataset_id="ds")
        _merge_entity(entity, {"tag_ids": ["a", "b"]})
        assert entity.tag_ids == ["a", "b"]

    def test_clear_list_field(self):
        """Merging an empty list clears an existing relation list."""
        entity = Variable(id="test", name="Test", dataset_id="ds", tag_ids=["a"])
        _merge_entity(entity, {"tag_ids": _CLEAR_LIST})
        assert entity.tag_ids == []

    def test_empty_list_field_does_not_clear(self):
        """Merging a plain empty list keeps an existing relation list."""
        entity = Variable(id="test", name="Test", dataset_id="ds", tag_ids=["a"])
        _merge_entity(entity, {"tag_ids": []})
        assert entity.tag_ids == ["a"]

    def test_remove_relation_id(self):
        """!id removes a relation from the accumulated list."""
        entity = Variable(id="test", name="Test", dataset_id="ds", tag_ids=["a", "b"])
        _merge_entity(entity, {"tag_ids": ["c", "!a"]})
        assert entity.tag_ids == ["c", "b"]

    def test_relation_removal_wins_over_addition(self):
        """Removing and adding the same relation in one row removes it."""
        entity = Variable(id="test", name="Test", dataset_id="ds", tag_ids=["a", "b"])
        _merge_entity(entity, {"tag_ids": ["a", "!a", "c"]})
        assert entity.tag_ids == ["c", "b"]


class TestGetCatalogTable:
    """Test _get_catalog_table function."""

    def test_all_entity_types(self):
        """Should return correct table for all entity types."""
        catalog = Catalog()

        assert _get_catalog_table(catalog, "folder") is catalog.folder
        assert _get_catalog_table(catalog, "dataset") is catalog.dataset
        assert _get_catalog_table(catalog, "variable") is catalog.variable
        assert _get_catalog_table(catalog, "enumeration") is catalog.enumeration
        assert _get_catalog_table(catalog, "value") is catalog.value
        assert _get_catalog_table(catalog, "frequency") is catalog.frequency
        assert _get_catalog_table(catalog, "organization") is catalog.organization
        assert _get_catalog_table(catalog, "tag") is catalog.tag
        assert _get_catalog_table(catalog, "doc") is catalog.doc

    def test_unknown_entity(self):
        """Should return None for unknown entity type."""
        catalog = Catalog()
        assert _get_catalog_table(catalog, "unknown") is None


class TestReadFile:
    """Test _read_file function."""

    def test_read_csv(self, tmp_path: Path):
        """Should read CSV files."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("id,name\n1,Test\n")

        df = _read_file(csv_path)
        assert df is not None
        assert len(df) == 1
        assert "id" in df.columns

    def test_read_json(self, tmp_path: Path):
        """Should read JSON files."""
        json_path = tmp_path / "test.json"
        json_path.write_text('[{"id": "1", "name": "Test"}]')

        df = _read_file(json_path)
        assert df is not None
        assert len(df) == 1

    def test_read_excel(self, tmp_path: Path):
        """Should read Excel files."""
        xlsx_path = tmp_path / "test.xlsx"
        pd.DataFrame({"id": ["1"], "name": ["Test"]}).to_excel(xlsx_path, index=False)

        df = _read_file(xlsx_path)
        assert df is not None
        assert len(df) == 1

    def test_unsupported_extension(self, tmp_path: Path):
        """Should return None for unsupported extensions."""
        txt_path = tmp_path / "test.txt"
        txt_path.write_text("hello")

        assert _read_file(txt_path) is None


class TestReadJson:
    """Test _read_json function."""

    def test_read_array(self, tmp_path: Path):
        """Should read JSON array."""
        json_path = tmp_path / "test.json"
        json_path.write_text('[{"id": "1"}, {"id": "2"}]')

        df = _read_json(json_path)
        assert df is not None
        assert len(df) == 2

    def test_read_object_with_entity_key(self, tmp_path: Path):
        """Should read JSON object with entity array."""
        json_path = tmp_path / "test.json"
        json_path.write_text('{"variables": [{"id": "1"}]}')

        df = _read_json(json_path)
        assert df is not None
        assert len(df) == 1

    def test_empty_json(self, tmp_path: Path):
        """Should return None for empty JSON."""
        json_path = tmp_path / "test.json"
        json_path.write_text("[]")

        assert _read_json(json_path) is None

    def test_invalid_json(self, tmp_path: Path, capsys):
        """Should return None and warn for invalid JSON."""
        json_path = tmp_path / "test.json"
        json_path.write_text("not valid json")

        result = _read_json(json_path, quiet=False)
        captured = capsys.readouterr()
        assert "✗  test.json" in captured.err
        assert result is None

    def test_invalid_json_uses_path_label(self, tmp_path: Path, capsys):
        """Metadata JSON warnings should use the supplied path label."""
        json_path = tmp_path / "test.json"
        json_path.write_text("not valid json")

        result = _read_json(
            json_path, quiet=False, path_label="metadata/source/test.json"
        )

        captured = capsys.readouterr()
        assert "✗  metadata/source/test.json" in captured.err
        assert "✗  test.json" not in captured.err
        assert result is None

    def test_non_array_json(self, tmp_path: Path):
        """Should return None for JSON without array."""
        json_path = tmp_path / "test.json"
        json_path.write_text('{"key": "value"}')

        assert _read_json(json_path) is None


class TestLoadTablesFromFolder:
    """Test _load_tables_from_folder function."""

    def test_load_csv_files(self, tmp_path: Path):
        """Should load entity CSV files."""
        (tmp_path / "variable.csv").write_text("id,name,dataset_id\nv1,Var1,ds1\n")
        (tmp_path / "tag.csv").write_text("id,name\nt1,Tag1\n")

        tables = _load_tables_from_folder(tmp_path, ALL_ENTITIES)

        assert "variable" in tables
        assert "tag" in tables
        assert len(tables["variable"][0]) == 1
        assert tables["variable"][1] == "variable.csv"

    def test_read_error_uses_metadata_relative_label(self, tmp_path: Path, capsys):
        """Metadata read errors should include the metadata folder label."""
        (tmp_path / "dataset.json").write_text("not valid json")

        tables = _load_tables_from_folder(tmp_path, {"dataset"}, quiet=False)

        captured = capsys.readouterr()
        assert tables == {}
        assert f"✗  {tmp_path.name}/dataset.json" in captured.err
        assert "✗  dataset.json" not in captured.err

    def test_metadata_file_label_falls_back_to_name(self):
        """Should fall back to basename when paths cannot be relativized."""
        label = _metadata_file_label(
            Path("relative/dataset.json"), Path("/tmp/metadata")
        )

        assert label == "dataset.json"

    def test_ignores_non_entity_files(self, tmp_path: Path):
        """Should ignore files that don't match entity names."""
        (tmp_path / "other.csv").write_text("id,name\n1,Test\n")

        tables = _load_tables_from_folder(tmp_path, ALL_ENTITIES)
        assert "other" not in tables

    def test_loads_first_matching_extension(self, tmp_path: Path):
        """Should load only one file per entity (first extension found)."""
        (tmp_path / "variable.csv").write_text("id,name,dataset_id\ncsv,CSV,ds\n")
        (tmp_path / "variable.json").write_text(
            '[{"id": "json", "name": "JSON", "dataset_id": "ds"}]'
        )

        tables = _load_tables_from_folder(tmp_path, ALL_ENTITIES)
        # Should only load one file, not both
        assert "variable" in tables
        # The filename should be one of the two (order depends on set iteration)
        assert tables["variable"][1] in ("variable.csv", "variable.json")

    def test_empty_folder(self, tmp_path: Path):
        """Should return empty dict for empty folder."""
        tables = _load_tables_from_folder(tmp_path, ALL_ENTITIES)
        assert tables == {}

    def test_skips_empty_dataframe(self, tmp_path: Path):
        """Should skip entity files that result in empty DataFrames."""
        # Create an empty CSV (headers only)
        (tmp_path / "folder.csv").write_text("id,name\n")

        tables = _load_tables_from_folder(tmp_path, ALL_ENTITIES)
        # Empty DataFrame should be skipped
        assert "folder" not in tables


class TestLoadTablesFromDatabase:
    """Test _load_tables_from_database function."""

    def test_load_sqlite_tables(self, tmp_path: Path):
        """Should load tables from SQLite database."""
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE variable (id TEXT, name TEXT, dataset_id TEXT)")
        conn.execute("INSERT INTO variable VALUES ('v1', 'Var1', 'ds1')")
        conn.commit()
        conn.close()

        tables = _load_tables_from_database(f"sqlite:///{db_path}", ALL_ENTITIES)

        assert "variable" in tables
        assert len(tables["variable"][0]) == 1

    def test_invalid_connection(self, capsys):
        """Should return empty dict and warn for invalid connection."""
        tables = _load_tables_from_database(
            "sqlite:///nonexistent/path/db.sqlite", ALL_ENTITIES, quiet=False
        )
        captured = capsys.readouterr()
        assert "✗  database" in captured.err
        assert tables == {}

    def test_ignores_non_entity_tables(self, tmp_path: Path):
        """Should ignore tables that don't match entity names."""
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE other_table (id TEXT)")
        conn.commit()
        conn.close()

        tables = _load_tables_from_database(f"sqlite:///{db_path}", ALL_ENTITIES)
        assert "other_table" not in tables
        # All entity types should have been checked but not found
        assert tables == {}

    def test_empty_database(self, tmp_path: Path):
        """Should return empty dict when no entity tables exist."""
        db_path = tmp_path / "empty.db"
        conn = sqlite3.connect(db_path)
        conn.commit()
        conn.close()

        tables = _load_tables_from_database(f"sqlite:///{db_path}", ALL_ENTITIES)
        assert tables == {}

    def test_connection_without_disconnect(self, tmp_path: Path):
        """Should handle connections without disconnect method."""
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.commit()
        conn.close()

        # Mock connection without disconnect method
        with patch("datannurpy.add_metadata.ibis") as mock_ibis:
            mock_con = MagicMock(spec=["list_tables", "table"])
            mock_con.list_tables.return_value = []
            mock_ibis.connect.return_value = mock_con

            tables = _load_tables_from_database(f"sqlite:///{db_path}", ALL_ENTITIES)
            assert tables == {}
            # Verify disconnect was not called (doesn't exist)
            assert not hasattr(mock_con, "disconnect")


class TestValidateEntityTable:
    """Test _validate_entity_table function."""

    def test_valid_table(self):
        """Should return no errors for valid table."""
        catalog = Catalog()
        df = pd.DataFrame({"id": ["v1"], "name": ["Var1"], "dataset_id": ["ds1"]})

        errors = _validate_entity_table(catalog, "variable", df, "variable.csv")
        assert errors == []

    def test_missing_required_column(self):
        """Should report missing required columns."""
        catalog = Catalog()
        df = pd.DataFrame({"id": ["v1"], "name": ["Var1"]})  # Missing dataset_id

        errors = _validate_entity_table(catalog, "variable", df, "variable.csv")
        assert len(errors) == 1
        assert "dataset_id" in errors[0]

    def test_variable_name_inferred_from_id(self):
        """Variable name can be inferred from id, so not required."""
        catalog = Catalog()
        df = pd.DataFrame({"id": ["ds---v1"], "dataset_id": ["ds"]})  # No name column

        errors = _validate_entity_table(catalog, "variable", df, "variable.csv")
        assert errors == []

    def test_empty_required_value_for_new_entity(self):
        """Should report empty required values for new entities."""
        catalog = Catalog()
        # dataset_id missing for v2
        df = pd.DataFrame(
            {
                "id": ["v1", "v2"],
                "name": ["Var1", "Var2"],
                "dataset_id": ["ds1", None],
            }
        )

        errors = _validate_entity_table(catalog, "variable", df, "variable.csv")
        assert len(errors) == 1
        assert "line 3" in errors[0]  # Row index 1 + 2 = line 3
        assert "dataset_id" in errors[0]
        catalog = Catalog()
        catalog.variable.add(Variable(id="v1", name="Existing", dataset_id="ds"))

        # v1 exists, so empty name should be OK (will be updated)
        df = pd.DataFrame({"id": ["v1"], "dataset_id": ["ds"]})

        errors = _validate_entity_table(catalog, "variable", df, "variable.csv")
        assert errors == []

    def test_skip_validation_for_value_entity(self):
        """Value entity uses composite key, no id validation."""
        catalog = Catalog()
        df = pd.DataFrame({"enumeration_id": ["m1"], "value": ["a"]})

        errors = _validate_entity_table(catalog, "value", df, "value.csv")
        assert errors == []

    def test_skip_validation_for_frequency_entity(self):
        """Frequency entity uses composite key, no id validation."""
        catalog = Catalog()
        df = pd.DataFrame({"variable_id": ["v1"], "value": ["a"], "frequency": [5]})

        errors = _validate_entity_table(catalog, "frequency", df, "frequency.csv")
        assert errors == []


class TestValidateAllTables:
    """Test _validate_all_tables function."""

    def test_collects_all_errors(self):
        """Should collect errors from all tables."""
        catalog = Catalog()
        tables = {
            "variable": (
                pd.DataFrame({"id": ["v1"]}),
                "variable.csv",
            ),  # Missing dataset_id, name
        }

        errors = _validate_all_tables(catalog, tables)
        assert len(errors) == 1
        assert "dataset_id" in errors[0] or "name" in errors[0]


class TestProcessEntityTable:
    """Test _process_entity_table function."""

    def test_create_new_entity(self):
        """Should create new entities."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "id": ["f1"],
                "name": ["Folder1"],
            }
        )

        created, updated = _process_entity_table(catalog, "folder", df)

        assert created == 1
        assert updated == 0
        assert len(catalog.folder.all()) == 1
        assert catalog.folder.all()[0].id == "f1"

    def test_create_new_entity_with_clear_list_field(self):
        """Clear markers on new entities become empty relation lists."""
        catalog = Catalog()
        df = pd.DataFrame({"id": ["f1"], "tag_ids": ["!"]})

        created, updated = _process_entity_table(catalog, "folder", df)

        assert created == 1
        assert updated == 0
        assert catalog.folder.all()[0].tag_ids == []

    def test_create_folder_with_schema_metadata_fields(self):
        """Folder metadata should preserve fields declared in the app schema."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "id": ["folder1"],
                "name": ["Folder 1"],
                "survey_type": ["registry"],
                "delivery_format": ["csv"],
                "metadata_path": ["/path/to/metadata"],
                "git_code": ["https://example.org/repo"],
            }
        )

        created, updated = _process_entity_table(catalog, "folder", df)

        assert created == 1
        assert updated == 0
        row = catalog.folder.df.to_dicts()[0]
        assert row["survey_type"] == "registry"
        assert row["delivery_format"] == "csv"
        assert row["metadata_path"] == "/path/to/metadata"
        assert row["git_code"] == "https://example.org/repo"

    def test_update_existing_entity(self):
        """Should update existing entities."""
        catalog = Catalog()
        catalog.folder.add(Folder(id="f1", name="Old"))

        df = pd.DataFrame(
            {
                "id": ["f1"],
                "name": ["New"],
                "description": ["Updated"],
            }
        )

        created, updated = _process_entity_table(catalog, "folder", df)

        assert created == 0
        assert updated == 1
        assert catalog.folder.all()[0].name == "New"
        assert catalog.folder.all()[0].description == "Updated"

    def test_strips_standard_entity_id_before_matching(self):
        """Whitespace around metadata ids should not create duplicate entities."""
        catalog = Catalog()
        catalog.folder.add(Folder(id="f1", name="Old"))
        df = pd.DataFrame({"id": [" f1 "], "name": [" New "]})

        created, updated = _process_entity_table(catalog, "folder", df)

        assert created == 0
        assert updated == 1
        assert len(catalog.folder.all()) == 1
        assert catalog.folder.all()[0].id == "f1"
        assert catalog.folder.all()[0].name == "New"

    def test_preserves_localized_columns_for_standard_entities(self):
        """Localized metadata fields should be kept in the table DataFrame."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "id": ["f1"],
                "name": ["Folder"],
                "name:fr": ["Dossier"],
                "description:fr": ["Description française"],
                "unknown:fr": ["ignored"],
            }
        )

        created, updated = _process_entity_table(catalog, "folder", df)

        assert created == 1
        assert updated == 0
        row = catalog.folder.df.to_dicts()[0]
        assert row["name"] == "Folder"
        assert row["name:fr"] == "Dossier"
        assert row["description:fr"] == "Description française"
        assert "unknown:fr" not in row

    def test_updates_localized_columns_without_overwriting_blank_cells(self):
        """Blank localized cells should leave existing localized values unchanged."""
        catalog = Catalog()
        catalog.folder.add(Folder(id="f1", name="Folder"))
        catalog.folder._df = catalog.folder._df.with_columns(
            pl.lit("Ancien").alias("name:fr")
        )
        df = pd.DataFrame(
            {
                "id": ["f1"],
                "name": ["Folder updated"],
                "name:fr": [None],
                "description:fr": ["Nouvelle description"],
            }
        )

        created, updated = _process_entity_table(catalog, "folder", df)

        assert created == 0
        assert updated == 1
        row = catalog.folder.df.to_dicts()[0]
        assert row["name"] == "Folder updated"
        assert row["name:fr"] == "Ancien"
        assert row["description:fr"] == "Nouvelle description"

    def test_create_value_entity(self):
        """Should create Value entities with composite key."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "enumeration_id": ["m1"],
                "value": ["a"],
                "description": ["Value A"],
            }
        )

        created, updated = _process_entity_table(catalog, "value", df)

        assert created == 1
        assert updated == 0
        assert len(catalog.value.all()) == 1
        assert catalog.value.all()[0].enumeration_id == "m1"

    def test_create_value_entity_with_bang_value(self):
        """Value.value should allow ! as a literal composite-key value."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "enumeration_id": ["enum1", "enum1"],
                "value": ["!", "?"],
                "description": ["No information", "Unknown"],
            }
        )

        created, updated = _process_entity_table(catalog, "value", df)

        assert created == 2
        assert updated == 0
        by_value = {value.value: value for value in catalog.value.all()}
        assert by_value["!"].description == "No information"
        assert by_value["?"].description == "Unknown"

    def test_create_value_entity_with_empty_value_from_csv(self, tmp_path: Path):
        """Value.value should allow an explicit empty CSV cell."""
        metadata_dir = tmp_path / "metadata"
        metadata_dir.mkdir()
        (metadata_dir / "value.csv").write_text(
            "enumeration_id,value,description\ngeneric---example,,Empty / missing\n",
            encoding="utf-8",
        )

        catalog = Catalog(metadata_path=metadata_dir, quiet=True)
        ensure_metadata_applied(catalog)

        values = catalog.value.all()
        assert len(values) == 1
        assert values[0].enumeration_id == "generic---example"
        assert values[0].value == ""
        assert values[0].description == "Empty / missing"

    def test_update_value_entity(self):
        """Should update existing Value entities."""
        catalog = Catalog()
        catalog.value.add(
            Value(id=build_value_id("m1", "a"), enumeration_id="m1", value="a")
        )

        df = pd.DataFrame(
            {
                "enumeration_id": ["m1"],
                "value": ["a"],
                "description": ["Updated"],
            }
        )

        created, updated = _process_entity_table(catalog, "value", df)

        assert created == 0
        assert updated == 1
        assert catalog.value.all()[0].description == "Updated"

    def test_strips_value_composite_key_before_matching(self):
        """Whitespace around Value composite keys should not create duplicates."""
        catalog = Catalog()
        catalog.value.add(
            Value(
                id=build_value_id("enum1", "a"),
                enumeration_id="enum1",
                value="a",
                description="Old",
            )
        )
        df = pd.DataFrame(
            {
                "enumeration_id": [" enum1 "],
                "value": [" a "],
                "description": [" New "],
            }
        )

        created, updated = _process_entity_table(catalog, "value", df)

        assert created == 0
        assert updated == 1
        assert len(catalog.value.all()) == 1
        value = catalog.value.all()[0]
        assert value.enumeration_id == "enum1"
        assert value.value == "a"
        assert value.description == "New"

    def test_preserves_localized_columns_for_value_entities(self):
        """Localized value descriptions should merge by enumeration_id and value."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "enumeration_id": ["m1"],
                "value": ["a"],
                "description": ["Value A"],
                "description:fr": ["Valeur A"],
            }
        )

        created, updated = _process_entity_table(catalog, "value", df)

        assert created == 1
        assert updated == 0
        row = catalog.value.df.to_dicts()[0]
        assert row["description"] == "Value A"
        assert row["description:fr"] == "Valeur A"

    def test_value_localized_clear_and_blank_merge(self):
        """Value localized fields should support clear and blank preserve semantics."""
        catalog = Catalog()
        catalog.value.add(Value(enumeration_id="m1", value="a", description="A"))
        catalog.value._df = catalog.value._df.with_columns(
            pl.lit("Ancienne valeur").alias("description:fr")
        )

        _process_entity_table(
            catalog,
            "value",
            pd.DataFrame(
                {
                    "enumeration_id": ["m1", "m1"],
                    "value": ["a", "a"],
                    "description:fr": [None, "!"],
                }
            ),
        )

        assert catalog.value.df.to_dicts()[0]["description:fr"] is None

    def test_frequency_localized_columns_merge_by_composite_key(self):
        """Frequency localized fields should merge by variable_id and value."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "variable_id": ["v1"],
                "value": ["a"],
                "frequency": [3],
                "value:fr": ["a-fr"],
            }
        )

        created, updated = _process_entity_table(catalog, "frequency", df)

        assert created == 1
        assert updated == 0
        assert catalog.frequency.df.to_dicts()[0]["value:fr"] == "a-fr"

    def test_localized_helpers_cover_empty_and_missing_paths(self):
        """Localized helper functions should handle empty inputs and missing keys."""
        catalog = Catalog()

        assert _localized_field_columns(["id", "unknown:fr", "name:fr"], Folder) == [
            "name:fr"
        ]
        assert not _is_missing_metadata_value(["tag"])
        assert not _is_missing_metadata_value({"label": "Tag"})
        assert _existing_localized_rows(catalog.folder, Folder, ["id"], {"f1"}) == []

        _merge_localized_fields(catalog.folder, Folder, [{"id": "f1"}], ["id"])

        catalog.folder.add(Folder(id="f1", name="Folder"))
        _merge_localized_fields(
            catalog.folder,
            Folder,
            [
                {"id": None, "name:fr": "Sans id"},
                {"id": "f1", "name:fr": None},
                {"id": "f1", "name:fr": "Dossier", "description:fr": "Desc"},
            ],
            ["id"],
        )
        row = catalog.folder.df.to_dicts()[0]
        assert row["name:fr"] == "Dossier"
        assert row["description:fr"] == "Desc"
        assert (
            _existing_localized_rows(catalog.folder, Folder, ["id"], {"missing"}) == []
        )
        assert _existing_localized_rows(catalog.folder, Folder, ["id"], {"f1"}) == [
            {"id": "f1", "name:fr": "Dossier", "description:fr": "Desc"}
        ]
        catalog.folder.add(Folder(id="f2", name="Folder 2"))
        catalog.folder._df = catalog.folder._df.with_columns(
            pl.when(pl.col("id") == "f2")
            .then(pl.lit(None, dtype=pl.Utf8))
            .otherwise(pl.col("name:fr"))
            .alias("name:fr"),
            pl.when(pl.col("id") == "f2")
            .then(pl.lit(None, dtype=pl.Utf8))
            .otherwise(pl.col("description:fr"))
            .alias("description:fr"),
        )
        assert _existing_localized_rows(catalog.folder, Folder, ["id"], {"f2"}) == []

    def test_skip_value_without_required_fields(self):
        """Should skip Value without enumeration_id or value."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "enumeration_id": ["m1", None],
                "value": [None, "a"],
            }
        )

        created, updated = _process_entity_table(catalog, "value", df)
        assert created == 0  # Both skipped

    def test_skip_value_with_blank_enumeration_id_after_trim(self):
        """Should skip Value rows whose enumeration_id trims to blank."""
        catalog = Catalog()
        df = pd.DataFrame({"enumeration_id": ["   "], "value": ["a"]})

        created, updated = _process_entity_table(catalog, "value", df)

        assert created == 0
        assert updated == 0

    def test_infer_variable_name_from_id(self):
        """Should infer variable name from id."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "id": ["folder---dataset---my_var"],
                "dataset_id": ["folder---dataset"],
            }
        )

        created, _ = _process_entity_table(catalog, "variable", df)

        assert created == 1
        assert catalog.variable.all()[0].name == "my_var"

    def test_skip_entity_without_id(self):
        """Should skip entities without id."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "id": [None, "f1"],
                "name": ["NoId", "HasId"],
            }
        )

        created, _ = _process_entity_table(catalog, "folder", df)
        assert created == 1  # Only f1 created

    def test_skip_entity_with_blank_id_after_trim(self):
        """Should skip id-keyed entities whose id trims to blank."""
        catalog = Catalog()
        df = pd.DataFrame({"id": ["   "], "name": ["Blank"]})

        created, updated = _process_entity_table(catalog, "folder", df)

        assert created == 0
        assert updated == 0

    def test_create_frequency_entity(self):
        """Should create Frequency entities with composite key."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "variable_id": ["v1"],
                "value": ["a"],
                "frequency": [5],
            }
        )

        created, updated = _process_entity_table(catalog, "frequency", df)

        assert created == 1
        assert updated == 0
        assert len(catalog.frequency.all()) == 1
        assert catalog.frequency.all()[0].variable_id == "v1"
        assert catalog.frequency.all()[0].value == "a"
        assert catalog.frequency.all()[0].frequency == 5

    def test_create_frequency_entity_with_bang_value(self):
        """Frequency.value should allow ! as a literal composite-key value."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "variable_id": ["v1", "v1"],
                "value": ["!", "?"],
                "frequency": [1, 2],
            }
        )

        created, updated = _process_entity_table(catalog, "frequency", df)

        assert created == 2
        assert updated == 0
        by_value = {frequency.value: frequency for frequency in catalog.frequency.all()}
        assert by_value["!"].frequency == 1
        assert by_value["?"].frequency == 2

    def test_update_frequency_entity(self):
        """Should update existing Frequency entities."""
        catalog = Catalog()
        frequency_id = build_frequency_id("v1", "a")
        catalog.frequency.add(
            Frequency(id=frequency_id, variable_id="v1", value="a", frequency=3)
        )

        df = pd.DataFrame(
            {
                "variable_id": ["v1"],
                "value": ["a"],
                "frequency": [10],
            }
        )

        created, updated = _process_entity_table(catalog, "frequency", df)

        assert created == 0
        assert updated == 1
        assert catalog.frequency.all()[0].frequency == 10

    def test_strips_frequency_composite_key_before_matching(self):
        """Whitespace around Frequency composite keys should not create duplicates."""
        catalog = Catalog()
        catalog.frequency.add(
            Frequency(
                id=build_frequency_id("var1", "a"),
                variable_id="var1",
                value="a",
                frequency=1,
            )
        )
        df = pd.DataFrame(
            {"variable_id": [" var1 "], "value": [" a "], "frequency": [3]}
        )

        created, updated = _process_entity_table(catalog, "frequency", df)

        assert created == 0
        assert updated == 1
        assert len(catalog.frequency.all()) == 1
        frequency = catalog.frequency.all()[0]
        assert frequency.variable_id == "var1"
        assert frequency.value == "a"
        assert frequency.frequency == 3

    def test_skip_frequency_without_required_fields(self):
        """Should skip Frequency without variable_id or value."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "variable_id": ["v1", None],
                "value": [None, "a"],
                "frequency": [5, 5],
            }
        )

        created, updated = _process_entity_table(catalog, "frequency", df)
        assert created == 0  # Both skipped

    def test_skip_frequency_with_blank_variable_id_after_trim(self):
        """Should skip Frequency rows whose variable_id trims to blank."""
        catalog = Catalog()
        df = pd.DataFrame({"variable_id": ["   "], "value": ["a"], "frequency": [1]})

        created, updated = _process_entity_table(catalog, "frequency", df)

        assert created == 0
        assert updated == 0

    def test_update_value_without_description(self):
        """Updating an existing Value without description in CSV should be a no-op update."""
        catalog = Catalog()
        value_id = build_value_id("m1", "a")
        catalog.value.add(
            Value(
                id=value_id,
                enumeration_id="m1",
                value="a",
                description="kept",
            )
        )

        df = pd.DataFrame({"enumeration_id": ["m1"], "value": ["a"]})

        created, updated = _process_entity_table(catalog, "value", df)

        assert created == 0
        assert updated == 1
        assert catalog.value.all()[0].description == "kept"

    def test_duplicate_id_in_same_csv_standard(self):
        """Duplicate id for a new entity in the same CSV should be merged."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "id": ["f1", "f1"],
                "name": ["First", "Second"],
                "description": [None, "Merged"],
            }
        )

        created, updated = _process_entity_table(catalog, "folder", df)

        assert created == 1
        assert updated == 0
        folder = catalog.folder.all()[0]
        # Second row overrides scalars (last-wins merge semantics)
        assert folder.name == "Second"
        assert folder.description == "Merged"

    def test_duplicate_composite_key_in_same_csv_value(self):
        """Duplicate composite key for a new Value in same CSV should be merged."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "enumeration_id": ["m1", "m1", "m2", "m2"],
                "value": ["a", "a", "b", "b"],
                # First pair: later row overrides (None → "Second").
                # Second pair: later row has no description, keeps first.
                "description": [None, "Second", "First", None],
            }
        )

        created, updated = _process_entity_table(catalog, "value", df)

        assert created == 2
        assert updated == 0
        by_id = {v.id: v for v in catalog.value.all()}
        assert by_id[build_value_id("m1", "a")].description == "Second"
        assert by_id[build_value_id("m2", "b")].description == "First"

    def test_duplicate_composite_key_in_same_csv_frequency(self):
        """Duplicate composite key for a new Frequency in same CSV should be merged."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "variable_id": ["v1", "v1"],
                "value": ["a", "a"],
                "frequency": [1, 7],
            }
        )

        created, updated = _process_entity_table(catalog, "frequency", df)

        assert created == 1
        assert updated == 0
        assert catalog.frequency.all()[0].frequency == 7


class TestUnknownEntityType:
    """Test unknown entity type handling."""

    def test_unknown_entity_type_raises(self):
        """Should raise KeyError for unknown entity type."""
        catalog = Catalog()
        df = pd.DataFrame({"id": ["x1"]})

        with pytest.raises(KeyError):
            _process_entity_table(catalog, "unknown", df)


class TestAddMetadataIntegration:
    """Integration tests for Catalog.add_metadata."""

    def test_add_metadata_from_folder(self, tmp_path: Path):
        """Should load metadata from folder."""
        (tmp_path / "folder.csv").write_text("id,name\nf1,Folder1\n")
        (tmp_path / "tag.csv").write_text("id,name\nt1,Tag1\n")

        catalog = Catalog()
        add_metadata(catalog, tmp_path, quiet=True)

        assert len(catalog.folder.all()) == 1
        assert len(catalog.tag.all()) == 1

    def test_add_metadata_from_sqlite(self, tmp_path: Path):
        """Should load metadata from SQLite database."""
        db_path = tmp_path / "metadata.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE folder (id TEXT, name TEXT)")
        conn.execute("INSERT INTO folder VALUES ('f1', 'Folder1')")
        conn.commit()
        conn.close()

        catalog = Catalog()
        add_metadata(catalog, f"sqlite:///{db_path}", quiet=True)

        assert len(catalog.folder.all()) == 1
        assert catalog.folder.all()[0].name == "Folder1"

    def test_add_metadata_updates_existing(self, tmp_path: Path):
        """Should update existing entities."""
        (tmp_path / "variable.csv").write_text(
            "id,name,dataset_id,description\nv1,Var1,ds1,New description\n"
        )

        catalog = Catalog()
        catalog.variable.add(Variable(id="v1", name="Var1", dataset_id="ds1"))

        add_metadata(catalog, tmp_path, quiet=True)

        assert catalog.variable.all()[0].description == "New description"

    def test_add_metadata_merges_list_fields(self, tmp_path: Path):
        """Should merge list fields."""
        (tmp_path / "variable.csv").write_text(
            'id,name,dataset_id,tag_ids\nv1,Var1,ds1,"t2,t3"\n'
        )

        catalog = Catalog()
        catalog.variable.add(
            Variable(id="v1", name="Var1", dataset_id="ds1", tag_ids=["t1"])
        )

        add_metadata(catalog, tmp_path, quiet=True)

        # New values first, then existing
        assert catalog.variable.all()[0].tag_ids == ["t2", "t3", "t1"]

    def test_add_metadata_folder_not_found(self):
        """Should raise FileNotFoundError for missing folder."""
        catalog = Catalog()

        with pytest.raises(ConfigError, match="Metadata folder not found"):
            add_metadata(catalog, "/nonexistent/path", quiet=True)

    def test_add_metadata_not_a_directory(self, tmp_path: Path):
        """Should raise ValueError if path is not a directory."""
        file_path = tmp_path / "file.txt"
        file_path.write_text("hello")

        catalog = Catalog()

        with pytest.raises(ConfigError, match="not a directory"):
            add_metadata(catalog, file_path, quiet=True)

    def test_add_metadata_no_files_found(self, tmp_path: Path, capsys):
        """Should warn if no metadata files found."""
        catalog = Catalog()
        add_metadata(catalog, tmp_path, quiet=False)

        captured = capsys.readouterr()
        assert "No metadata files found" in captured.err

    def test_add_metadata_validation_errors(self, tmp_path: Path, capsys):
        """Should report validation errors and not process."""
        (tmp_path / "variable.csv").write_text("id\nv1\n")  # Missing required columns

        catalog = Catalog()
        add_metadata(catalog, tmp_path, quiet=False)

        captured = capsys.readouterr()
        assert "Invalid metadata" in captured.err
        assert len(catalog.variable.all()) == 0  # Not processed

    def test_add_metadata_quiet_mode(self, tmp_path: Path, capsys):
        """Should suppress output in quiet mode."""
        (tmp_path / "folder.csv").write_text("id,name\nf1,Folder1\n")

        catalog = Catalog()
        add_metadata(catalog, tmp_path, quiet=True)

        captured = capsys.readouterr()
        assert captured.err == ""

    def test_add_metadata_uses_catalog_quiet(self, tmp_path: Path, capsys):
        """Should use catalog.quiet if quiet not specified."""
        (tmp_path / "folder.csv").write_text("id,name\nf1,Folder1\n")

        catalog = Catalog(quiet=True)
        add_metadata(catalog, tmp_path)

        captured = capsys.readouterr()
        assert captured.err == ""

    def test_add_metadata_all_entity_types(self, tmp_path: Path):
        """Should handle all entity types."""
        (tmp_path / "folder.csv").write_text("id,name\nf1,Folder\n")
        (tmp_path / "dataset.csv").write_text("id,name\nd1,Dataset\n")
        (tmp_path / "variable.csv").write_text("id,name,dataset_id\nv1,Var,d1\n")
        (tmp_path / "enumeration.csv").write_text("id,name,type\nm1,Mod,string\n")
        (tmp_path / "value.csv").write_text(
            "enumeration_id,value,description\nm1,a,Val A\n"
        )
        (tmp_path / "organization.csv").write_text("id,name\ni1,Org\n")
        (tmp_path / "tag.csv").write_text("id,name\nt1,Tag\n")
        (tmp_path / "doc.csv").write_text("id,name\ndoc1,Doc\n")

        catalog = Catalog()
        add_metadata(catalog, tmp_path, quiet=True)

        assert len(catalog.folder.all()) == 1
        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 1
        assert len(catalog.enumeration.all()) == 1
        assert len(catalog.value.all()) == 1
        assert len(catalog.organization.all()) == 1
        assert len(catalog.tag.all()) == 1
        assert len(catalog.doc.all()) == 1

    def test_add_metadata_with_output(self, tmp_path: Path, capsys):
        """Should print progress when not quiet."""
        (tmp_path / "folder.csv").write_text("id,name\nf1,Folder1\n")

        catalog = Catalog()
        add_metadata(catalog, tmp_path, quiet=False)

        captured = capsys.readouterr()
        assert "add_metadata" in captured.err
        assert "folder:" in captured.err
        assert "created" in captured.err


class TestEdgeCases:
    """Test edge cases for better coverage."""

    def test_validate_entity_id_none_skipped(self):
        """Rows without id should be skipped in validation."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "id": [None, "f1"],
                "name": ["NoId", "HasId"],
            }
        )

        errors = _validate_entity_table(catalog, "folder", df, "folder.csv")
        # No errors because row without id is skipped, and f1 is valid
        assert errors == []

    def test_read_statistical_file(self, tmp_path: Path):
        """Should read SAS files via _read_file."""
        # We need an actual SAS file for this, use the one in data/
        data_dir = Path(__file__).parent.parent / "data"
        sas_file = data_dir / "cars.sas7bdat"
        if sas_file.exists():
            df = _read_file(sas_file)
            assert df is not None
            assert len(df) > 0

    def test_load_database_with_table_read_error(self, tmp_path: Path, capsys):
        """Should warn when table read fails."""
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE folder (id TEXT, name TEXT)")
        conn.commit()
        conn.close()

        # Mock con.table to raise an exception
        with patch("datannurpy.add_metadata.ibis") as mock_ibis:
            mock_con = MagicMock()
            mock_con.list_tables.return_value = ["folder"]
            mock_con.table.side_effect = Exception("Table read error")
            mock_ibis.connect.return_value = mock_con

            tables = _load_tables_from_database(
                f"sqlite:///{db_path}", ALL_ENTITIES, quiet=False
            )

            captured = capsys.readouterr()
            assert "✗  folder" in captured.err
            assert "Table read error" in captured.err
            assert "folder" not in tables

    def test_add_metadata_shows_summary_with_updates_only(self, tmp_path: Path, capsys):
        """Should show summary even with only updates."""
        (tmp_path / "folder.csv").write_text("id,name,description\nf1,Folder,Updated\n")

        catalog = Catalog()
        catalog.folder.add(Folder(id="f1", name="Folder"))
        add_metadata(catalog, tmp_path, quiet=False)

        captured = capsys.readouterr()
        assert "\n  →  0 created, 1 updated in " in captured.err

    def test_merge_entity_marks_seen(self):
        """_merge_entity should set _seen=True on entities with that attribute."""
        folder = Folder(id="f1", name="Old")
        assert folder._seen is False
        _merge_entity(folder, {"name": "New"})
        assert folder._seen is True

    def test_value_update_with_description(self, tmp_path: Path):
        """Updating existing value with description should apply it."""
        (tmp_path / "value.csv").write_text(
            "enumeration_id,value,description\nm1,A,Updated desc\n"
        )

        catalog = Catalog()
        catalog.enumeration.add(Enumeration(id="m1", name="Mod"))
        catalog.value.add(
            Value(id=build_value_id("m1", "A"), enumeration_id="m1", value="A")
        )

        add_metadata(catalog, tmp_path, quiet=True)
        val = catalog.value.all()[0]
        assert val.description == "Updated desc"

    def test_value_update_without_description(self, tmp_path: Path):
        """Updating existing value without description should keep None."""
        (tmp_path / "value.csv").write_text("enumeration_id,value\nm1,A\n")

        catalog = Catalog()
        catalog.enumeration.add(Enumeration(id="m1", name="Mod"))
        catalog.value.add(
            Value(
                id=build_value_id("m1", "A"),
                enumeration_id="m1",
                value="A",
                description="Old",
            )
        )

        add_metadata(catalog, tmp_path, quiet=True)
        val = catalog.value.all()[0]
        assert val.description == "Old"

    def test_value_create_marks_parent_enumeration_seen(self, tmp_path: Path):
        """Creating new value should mark parent enumeration as seen."""
        (tmp_path / "value.csv").write_text(
            "enumeration_id,value,description\nm1,B,New val\n"
        )

        catalog = Catalog()
        # Add existing enumeration with _seen=False
        catalog.enumeration.add(Enumeration(id="m1", name="Mod", _seen=False))

        add_metadata(catalog, tmp_path, quiet=True)

        # New value should be created
        assert len(catalog.value.all()) == 1
        # Parent enumeration should be marked as seen
        enumeration = catalog.enumeration.get("m1")
        assert enumeration is not None
        assert enumeration._seen is True

    def test_frequency_create_from_csv(self, tmp_path: Path):
        """Should create frequency entries from CSV."""
        (tmp_path / "frequency.csv").write_text(
            "variable_id,value,frequency\nv1,red,10\nv1,blue,5\n"
        )

        catalog = Catalog()
        add_metadata(catalog, tmp_path, quiet=True)

        assert len(catalog.frequency.all()) == 2
        frequencies = {f.value: f.frequency for f in catalog.frequency.all()}
        assert frequencies["red"] == 10
        assert frequencies["blue"] == 5

    def test_frequency_update_from_csv(self, tmp_path: Path):
        """Should update existing frequency from CSV."""
        frequency_id = build_frequency_id("v1", "red")
        catalog = Catalog()
        catalog.frequency.add(
            Frequency(id=frequency_id, variable_id="v1", value="red", frequency=3)
        )

        (tmp_path / "frequency.csv").write_text(
            "variable_id,value,frequency\nv1,red,20\n"
        )
        add_metadata(catalog, tmp_path, quiet=True)

        assert len(catalog.frequency.all()) == 1
        assert catalog.frequency.all()[0].frequency == 20


class TestEnsureMetadataApplied:
    """Tests for ensure_metadata_applied and metadata_path."""

    def test_no_metadata_path_is_noop(self):
        """Should do nothing when metadata_path is not set."""
        catalog = Catalog()
        ensure_metadata_applied(catalog)
        assert not catalog._metadata_applied

    def test_applies_metadata_from_folder(self, tmp_path: Path):
        """Should apply metadata when metadata_path is a folder."""
        (tmp_path / "folder.csv").write_text("id,name\nf1,Folder1\n")
        catalog = Catalog(metadata_path=tmp_path, quiet=True)
        ensure_metadata_applied(catalog)
        assert catalog._metadata_applied
        assert len(catalog.folder.all()) == 1

    def test_idempotent(self, tmp_path: Path):
        """Should only apply metadata once."""
        (tmp_path / "folder.csv").write_text("id,name\nf1,Folder1\n")
        catalog = Catalog(metadata_path=tmp_path, quiet=True)
        ensure_metadata_applied(catalog)
        ensure_metadata_applied(catalog)
        assert len(catalog.folder.all()) == 1

    def test_invalid_path_raises(self):
        """Should raise ConfigError for nonexistent path."""
        with pytest.raises(ConfigError, match="Metadata folder not found"):
            Catalog(metadata_path="/nonexistent/path", quiet=True)

    def test_file_not_dir_raises(self, tmp_path: Path):
        """Should raise ConfigError when path is a file, not directory."""
        file_path = tmp_path / "file.txt"
        file_path.write_text("hello")
        with pytest.raises(ConfigError, match="not a directory"):
            Catalog(metadata_path=file_path, quiet=True)

    def test_triggered_by_export_db(self, tmp_path: Path):
        """export_db should trigger metadata application."""
        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        (meta_dir / "tag.csv").write_text("id,name\nt1,MyTag\n")
        out_dir = tmp_path / "output"
        catalog = Catalog(
            app_path=out_dir, metadata_path=meta_dir, refresh=True, quiet=True
        )
        catalog.export_db()
        assert catalog._metadata_applied
        assert len(catalog.tag.all()) == 1

    def test_triggered_by_export_app(self, tmp_path: Path):
        """export_app should trigger metadata application."""
        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        (meta_dir / "tag.csv").write_text("id,name\nt1,MyTag\n")
        out_dir = tmp_path / "output"
        catalog = Catalog(
            app_path=out_dir, metadata_path=meta_dir, refresh=True, quiet=True
        )
        catalog.export_app()
        assert catalog._metadata_applied
        assert len(catalog.tag.all()) == 1

    def test_metadata_path_with_database_uri(self, tmp_path: Path):
        """Should work with database URI."""
        import sqlite3

        db_path = tmp_path / "metadata.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE folder (id TEXT, name TEXT)")
        conn.execute("INSERT INTO folder VALUES ('f1', 'Folder1')")
        conn.commit()
        conn.close()
        catalog = Catalog(metadata_path=f"sqlite:///{db_path}", quiet=True)
        ensure_metadata_applied(catalog)
        assert catalog._metadata_applied
        assert len(catalog.folder.all()) == 1


class TestLoadMetadata:
    """Tests for load_metadata and _extract_freq_hidden_ids."""

    def test_loads_tables_into_memory(self, tmp_path: Path):
        """Should populate _loaded_metadata at init."""
        (tmp_path / "tag.csv").write_text("id,name\nt1,Tag1\n")
        catalog = Catalog(metadata_path=tmp_path, quiet=True)
        assert catalog._loaded_metadata is not None
        assert len(catalog._loaded_metadata) == 1
        assert "tag" in catalog._loaded_metadata[0]

    def test_no_metadata_path_leaves_none(self):
        """Should leave _loaded_metadata as None when no path."""
        catalog = Catalog()
        assert catalog._loaded_metadata is None
        assert catalog._freq_hidden_ids == set()

    def test_extracts_freq_hidden_ids(self, tmp_path: Path):
        """Should extract variable IDs with policy---frequency-hidden tag."""
        (tmp_path / "variable.csv").write_text(
            "id,name,dataset_id,tag_ids\n"
            f'd---v1,v1,d,"{FREQ_HIDDEN_TAG}"\n'
            "d---v2,v2,d,other\n"
        )
        catalog = Catalog(metadata_path=tmp_path, quiet=True)
        assert catalog._freq_hidden_ids == {"d---v1"}

    def test_frequency_hidden_with_multiple_tags(self, tmp_path: Path):
        """Should detect frequency-hidden even with other tags."""
        (tmp_path / "variable.csv").write_text(
            f'id,name,dataset_id,tag_ids\nd---v1,v1,d,"rh,{FREQ_HIDDEN_TAG},finance"\n'
        )
        catalog = Catalog(metadata_path=tmp_path, quiet=True)
        assert catalog._freq_hidden_ids == {"d---v1"}

    def test_no_variable_file_empty_hidden(self, tmp_path: Path):
        """Should have empty _freq_hidden_ids when no variable.csv."""
        (tmp_path / "tag.csv").write_text("id,name\nt1,Tag1\n")
        catalog = Catalog(metadata_path=tmp_path, quiet=True)
        assert catalog._freq_hidden_ids == set()

    def test_ensure_uses_preloaded(self, tmp_path: Path):
        """ensure_metadata_applied should use _loaded_metadata, not re-read."""
        (tmp_path / "folder.csv").write_text("id,name\nf1,Folder1\n")
        catalog = Catalog(metadata_path=tmp_path, quiet=True)
        assert catalog._loaded_metadata is not None
        # Remove the folder so re-reading would fail
        (tmp_path / "folder.csv").unlink()
        ensure_metadata_applied(catalog)
        assert catalog._metadata_applied
        assert len(catalog.folder.all()) == 1

    def test_extract_freq_hidden_no_tag_ids_column(self):
        """Should return empty set when tag_ids column missing."""
        df = pd.DataFrame({"id": ["v1"], "name": ["var1"]})
        tables: dict[str, tuple[pd.DataFrame, str]] = {"variable": (df, "variable.csv")}
        assert _extract_freq_hidden_ids(tables) == set()

    def test_extract_freq_hidden_no_id_column(self):
        """Should return empty set when id column missing."""
        df = pd.DataFrame({"tag_ids": [FREQ_HIDDEN_TAG], "name": ["v1"]})
        tables: dict[str, tuple[pd.DataFrame, str]] = {"variable": (df, "variable.csv")}
        assert _extract_freq_hidden_ids(tables) == set()

    def test_extract_freq_hidden_empty_table(self):
        """Should return empty set for empty dict."""
        assert _extract_freq_hidden_ids({}) == set()

    def test_ensure_fallback_when_not_preloaded(self, tmp_path: Path):
        """ensure_metadata_applied should load from disk if _loaded_metadata is None."""
        (tmp_path / "folder.csv").write_text("id,name\nf1,Folder1\n")
        catalog = Catalog(metadata_path=tmp_path, quiet=True)
        catalog._loaded_metadata = None  # simulate missing preload
        ensure_metadata_applied(catalog)
        assert catalog._metadata_applied
        assert len(catalog.folder.all()) == 1


class TestFrequencyHiddenPolicy:
    """Tests for policy---frequency-hidden during scan and export."""

    def _make_csv(self, path: Path) -> Path:
        """Create a CSV with two low-cardinality columns."""
        csv_path = path / "data.csv"
        csv_path.write_text("name,color\nAlice,red\nBob,blue\nAlice,red\n")
        return csv_path

    def _make_metadata(self, path: Path, folder_id: str) -> Path:
        """Create metadata tagging 'name' column as frequency-hidden."""
        meta = path / "meta"
        meta.mkdir()
        (meta / "variable.csv").write_text(
            f"id,name,dataset_id,tag_ids\n"
            f"{folder_id}---data_csv---name,name,{folder_id}---data_csv,"
            f'"{FREQ_HIDDEN_TAG}"\n'
        )
        return meta

    def test_hidden_var_has_no_frequency(self, tmp_path: Path):
        """Frequency-hidden variable should have no frequency rows."""
        self._make_csv(tmp_path)
        meta = self._make_metadata(tmp_path, "src")
        catalog = Catalog(metadata_path=meta, quiet=True)
        catalog.add_folder(tmp_path, metadata=EntityMetadata(id="src"), include="*.csv")
        ensure_metadata_applied(catalog)

        name_var = catalog.variable.get("src---data_csv---name")
        color_var = catalog.variable.get("src---data_csv---color")
        assert name_var is not None
        assert color_var is not None

        # name: no enumeration, no frequency rows
        assert not name_var.enumeration_ids
        frequency_rows = [
            f
            for f in catalog.frequency.all()
            if f.variable_id == "src---data_csv---name"
        ]
        assert frequency_rows == []

        # color: has enumeration and frequency rows (not hidden)
        assert color_var.enumeration_ids
        color_frequencies = [
            f
            for f in catalog.frequency.all()
            if f.variable_id == "src---data_csv---color"
        ]
        assert len(color_frequencies) > 0

    def test_hidden_var_keeps_stats(self, tmp_path: Path):
        """Frequency-hidden variable should still have stats."""
        self._make_csv(tmp_path)
        meta = self._make_metadata(tmp_path, "src")
        catalog = Catalog(metadata_path=meta, quiet=True)
        catalog.add_folder(tmp_path, metadata=EntityMetadata(id="src"), include="*.csv")

        name_var = catalog.variable.get("src---data_csv---name")
        assert name_var is not None
        assert name_var.nb_distinct == 2

    def test_hidden_tag_applied_after_metadata(self, tmp_path: Path):
        """The policy tag should appear on the variable after metadata apply."""
        self._make_csv(tmp_path)
        meta = self._make_metadata(tmp_path, "src")
        catalog = Catalog(metadata_path=meta, quiet=True)
        catalog.add_folder(tmp_path, metadata=EntityMetadata(id="src"), include="*.csv")
        ensure_metadata_applied(catalog)

        name_var = catalog.variable.get("src---data_csv---name")
        assert name_var is not None
        assert FREQ_HIDDEN_TAG in (name_var.tag_ids or [])

    def test_no_hidden_ids_without_metadata(self, tmp_path: Path):
        """Without metadata_path, no variables are hidden."""
        self._make_csv(tmp_path)
        catalog = Catalog(quiet=True)
        catalog.add_folder(tmp_path, metadata=EntityMetadata(id="src"), include="*.csv")

        name_var = catalog.variable.get("src---data_csv---name")
        assert name_var is not None
        # name should have frequency rows (not hidden)
        frequency_rows = [
            f
            for f in catalog.frequency.all()
            if f.variable_id == "src---data_csv---name"
        ]
        assert len(frequency_rows) > 0

    def test_multiple_hidden_variables(self, tmp_path: Path):
        """Multiple variables tagged frequency-hidden should all be suppressed."""
        csv_path = tmp_path / "data.csv"
        csv_path.write_text("name,color,age\nAlice,red,30\nBob,blue,25\nAlice,red,30\n")
        meta = tmp_path / "meta"
        meta.mkdir()
        (meta / "variable.csv").write_text(
            f"id,name,dataset_id,tag_ids\n"
            f'src---data_csv---name,name,src---data_csv,"{FREQ_HIDDEN_TAG}"\n'
            f'src---data_csv---color,color,src---data_csv,"{FREQ_HIDDEN_TAG}"\n'
        )
        catalog = Catalog(metadata_path=meta, quiet=True)
        assert catalog._freq_hidden_ids == {
            "src---data_csv---name",
            "src---data_csv---color",
        }
        catalog.add_folder(tmp_path, metadata=EntityMetadata(id="src"), include="*.csv")
        ensure_metadata_applied(catalog)

        for col in ("name", "color"):
            var = catalog.variable.get(f"src---data_csv---{col}")
            assert var is not None
            assert not var.enumeration_ids
            assert [f for f in catalog.frequency.all() if f.variable_id == var.id] == []

        # age is not hidden — should have frequency rows
        age_var = catalog.variable.get("src---data_csv---age")
        assert age_var is not None
        age_frequencies = [
            f for f in catalog.frequency.all() if f.variable_id == age_var.id
        ]
        assert len(age_frequencies) > 0


class TestConceptEntity:
    """Test concept entity loading."""

    def test_tag_extended_fields_load_from_csv(self, tmp_path: Path):
        """Tag relation and propagation fields should be loaded."""
        (tmp_path / "tag.csv").write_text(
            'id,implied_tag_ids,propagate_to_parents\nbase,"t1, t2",true\n'
        )
        catalog = Catalog(metadata_path=tmp_path, quiet=True)
        ensure_metadata_applied(catalog)

        tag = catalog.tag.get("base")
        assert tag is not None
        assert tag.implied_tag_ids == ["t1", "t2"]
        assert tag.propagate_to_parents is True

    def test_tag_propagate_to_parents_bool_values(self, tmp_path: Path):
        """propagate_to_parents should parse common metadata bool values."""
        (tmp_path / "tag.csv").write_text(
            "id,propagate_to_parents\n"
            "true_text,true\n"
            "false_text,false\n"
            "one_number,1\n"
            "zero_number,0\n"
        )
        catalog = Catalog(metadata_path=tmp_path, quiet=True)
        ensure_metadata_applied(catalog)

        true_text = catalog.tag.get("true_text")
        false_text = catalog.tag.get("false_text")
        one_number = catalog.tag.get("one_number")
        zero_number = catalog.tag.get("zero_number")
        assert true_text is not None
        assert false_text is not None
        assert one_number is not None
        assert zero_number is not None
        assert true_text.propagate_to_parents is True
        assert false_text.propagate_to_parents is False
        assert one_number.propagate_to_parents is True
        assert zero_number.propagate_to_parents is False

    def test_tag_propagate_to_parents_json_bool_values(self, tmp_path: Path):
        """propagate_to_parents should parse JSON bool values."""
        (tmp_path / "tag.json").write_text(
            '[{"id":"true_bool","propagate_to_parents":true},'
            '{"id":"false_bool","propagate_to_parents":false},'
            '{"id":"one_number","propagate_to_parents":1},'
            '{"id":"zero_number","propagate_to_parents":0},'
            '{"id":"empty_list","propagate_to_parents":[]}]'
        )
        catalog = Catalog(metadata_path=tmp_path, quiet=True)
        ensure_metadata_applied(catalog)

        true_bool = catalog.tag.get("true_bool")
        false_bool = catalog.tag.get("false_bool")
        one_number = catalog.tag.get("one_number")
        zero_number = catalog.tag.get("zero_number")
        empty_list = catalog.tag.get("empty_list")
        assert true_bool is not None
        assert false_bool is not None
        assert one_number is not None
        assert zero_number is not None
        assert empty_list is not None
        assert true_bool.propagate_to_parents is True
        assert false_bool.propagate_to_parents is False
        assert one_number.propagate_to_parents is True
        assert zero_number.propagate_to_parents is False
        assert empty_list.propagate_to_parents is False

    def test_concept_loads_from_csv(self, tmp_path: Path):
        """Concept rows from concept.csv should be loaded into catalog.concept."""
        (tmp_path / "concept.csv").write_text(
            "id,parent_id,name,description\n"
            "revenu,,Revenu,Total revenue\n"
            "revenu_net,revenu,Revenu net,Net revenue\n"
        )
        catalog = Catalog(metadata_path=tmp_path, quiet=True)
        ensure_metadata_applied(catalog)
        assert catalog.concept.count == 2
        net = catalog.concept.get("revenu_net")
        assert net is not None
        assert net.parent_id == "revenu"
        assert net.name == "Revenu net"

    def test_concept_list_fields(self, tmp_path: Path):
        """tag_ids and doc_ids on concept should be parsed as lists."""
        (tmp_path / "concept.csv").write_text('id,tag_ids,doc_ids\nc1,"t1, t2","d1"\n')
        catalog = Catalog(metadata_path=tmp_path, quiet=True)
        ensure_metadata_applied(catalog)
        c1 = catalog.concept.get("c1")
        assert c1 is not None
        assert c1.tag_ids == ["t1", "t2"]
        assert c1.doc_ids == ["d1"]

    def test_variable_concept_id_loaded(self, tmp_path: Path):
        """concept_id column on variable should be loaded."""
        (tmp_path / "concept.csv").write_text("id,name\nc1,Concept 1\n")
        (tmp_path / "variable.csv").write_text(
            "id,name,dataset_id,concept_id\nds---v1,v1,ds,c1\n"
        )
        catalog = Catalog(metadata_path=tmp_path, quiet=True)
        ensure_metadata_applied(catalog)
        var = catalog.variable.get("ds---v1")
        assert var is not None
        assert var.concept_id == "c1"

    def test_variable_business_key_loaded(self, tmp_path: Path):
        """business_key column on variable should be loaded."""
        (tmp_path / "variable.csv").write_text(
            "id,name,dataset_id,business_key\nds---v1,v1,ds,1\n"
        )
        catalog = Catalog(metadata_path=tmp_path, quiet=True)
        ensure_metadata_applied(catalog)

        var = catalog.variable.get("ds---v1")
        assert var is not None
        assert var.business_key == 1

    def test_unseen_concept_removed_on_finalize(self, tmp_path: Path):
        """Concepts with _seen=False should be removed on finalize."""
        (tmp_path / "concept.csv").write_text("id,name\nc1,Concept 1\n")
        (tmp_path / "data.csv").write_text("a,b\n1,2\n")
        meta = tmp_path / "meta"
        meta.mkdir()
        (meta / "concept.csv").write_text("id,name\nc1,C1\n")
        catalog = Catalog(metadata_path=meta, quiet=True)
        catalog.add_folder(
            tmp_path, metadata=EntityMetadata(id="src"), include="data.csv"
        )
        # concept not referenced by any variable -> unseen -> removed
        assert catalog.concept.count == 0


class TestConfigAutoLoad:
    """Test auto-loading of config.<ext> from metadata folder."""

    def test_config_csv_loaded(self, tmp_path: Path):
        """config.csv should populate catalog.config."""
        (tmp_path / "config.csv").write_text(
            "id,value\ncontact_email,a@b.c\nmore_info,https://x\n"
        )
        catalog = Catalog(metadata_path=tmp_path, quiet=True)
        ensure_metadata_applied(catalog)
        assert catalog.config.count == 2
        entry = catalog.config.get("contact_email")
        assert entry is not None
        assert entry.value == "a@b.c"

    def test_app_config_param_takes_precedence(self, tmp_path: Path):
        """If app_config is provided, config.csv is ignored."""
        (tmp_path / "config.csv").write_text("id,value\nk,from_file\n")
        catalog = Catalog(
            metadata_path=tmp_path,
            app_config={"k": "from_param"},
            quiet=True,
        )
        ensure_metadata_applied(catalog)
        assert catalog.config.count == 1
        entry = catalog.config.get("k")
        assert entry is not None
        assert entry.value == "from_param"

    def test_config_missing_columns_warns(self, tmp_path: Path, capsys):
        """config.csv without id/value columns should warn and skip."""
        (tmp_path / "config.csv").write_text("foo,bar\n1,2\n")
        catalog = Catalog(metadata_path=tmp_path, quiet=False)
        ensure_metadata_applied(catalog)
        assert catalog.config.count == 0
        captured = capsys.readouterr()
        assert "config" in captured.err.lower()

    def test_config_skips_none_and_nan(self, tmp_path: Path):
        """Rows with None id or None value should be handled."""
        (tmp_path / "config.csv").write_text("id,value\nk1,v1\n,v2\nk3,\n")
        catalog = Catalog(metadata_path=tmp_path, quiet=True)
        ensure_metadata_applied(catalog)
        # Row with empty id skipped, row with empty value kept as ""
        assert catalog.config.count == 2
        k3 = catalog.config.get("k3")
        assert k3 is not None
        assert k3.value == ""

    def test_config_coerces_none_value_to_empty_string(self) -> None:
        """config metadata keeps None values as empty strings."""
        catalog = Catalog(quiet=True)

        _apply_config_table(catalog, pd.DataFrame({"id": ["k1"], "value": [None]}))

        config = catalog.config.get("k1")
        assert config is not None
        assert config.value == ""

    def test_config_filter_csv_loaded_and_exported(self, tmp_path: Path):
        """configFilter.csv should populate configFilter.json outputs."""
        (tmp_path / "configFilter.csv").write_text(
            "id,name,entity,field,value,is_active_default\n"
            "public,Public datasets,dataset,tag_ids,public,true\n"
        )
        catalog = Catalog(metadata_path=tmp_path, quiet=True)
        ensure_metadata_applied(catalog)

        entry = catalog.configFilter.get("public")
        assert entry == ConfigFilter(
            id="public",
            name="Public datasets",
            entity="dataset",
            field="tag_ids",
            value="public",
            is_active_default=True,
        )

        catalog.export_db(tmp_path / "out")
        data = json.loads((tmp_path / "out" / "configFilter.json").read_text())
        assert data == [
            {
                "id": "public",
                "name": "Public datasets",
                "entity": "dataset",
                "field": "tag_ids",
                "value": "public",
                "is_active_default": True,
            }
        ]
        assert (tmp_path / "out" / "configFilter.json.js").exists()


class TestMetadataPathList:
    """Test support for a list of metadata_path sources (overlay pattern)."""

    def test_list_applies_in_order(self, tmp_path: Path):
        """Later sources override earlier ones (scalar fields)."""
        base = tmp_path / "base"
        base.mkdir()
        (base / "tag.csv").write_text("id,name\nt1,Base\n")
        overlay = tmp_path / "overlay"
        overlay.mkdir()
        (overlay / "tag.csv").write_text("id,name\nt1,Override\n")

        catalog = Catalog(metadata_path=[base, overlay], quiet=True)
        ensure_metadata_applied(catalog)
        tag = catalog.tag.get("t1")
        assert tag is not None
        assert tag.name == "Override"

    def test_list_merges_list_fields(self, tmp_path: Path):
        """List fields (tag_ids) from later sources are unioned."""
        base = tmp_path / "base"
        base.mkdir()
        (base / "variable.csv").write_text(
            "id,name,dataset_id,tag_ids\nds---v1,v1,ds,base_tag\n"
        )
        overlay = tmp_path / "overlay"
        overlay.mkdir()
        (overlay / "variable.csv").write_text(
            "id,dataset_id,tag_ids\nds---v1,ds,overlay_tag\n"
        )

        catalog = Catalog(metadata_path=[base, overlay], quiet=True)
        ensure_metadata_applied(catalog)
        var = catalog.variable.get("ds---v1")
        assert var is not None
        assert set(var.tag_ids) == {"base_tag", "overlay_tag"}

    def test_list_unions_frequency_hidden_ids(self, tmp_path: Path):
        """frequency-hidden ids are unioned across all sources."""
        base = tmp_path / "base"
        base.mkdir()
        (base / "variable.csv").write_text(
            f"id,name,dataset_id,tag_ids\nds---a,a,ds,{FREQ_HIDDEN_TAG}\n"
        )
        overlay = tmp_path / "overlay"
        overlay.mkdir()
        (overlay / "variable.csv").write_text(
            f"id,dataset_id,tag_ids\nds---b,ds,{FREQ_HIDDEN_TAG}\n"
        )
        catalog = Catalog(metadata_path=[base, overlay], quiet=True)
        assert catalog._freq_hidden_ids == {"ds---a", "ds---b"}


class TestMetadataClearInstructions:
    """Test exact ! metadata clear instructions."""

    def test_clear_scalar_from_csv(self, tmp_path: Path):
        """A scalar ! clears the accumulated value."""
        base = tmp_path / "base"
        base.mkdir()
        (base / "dataset.csv").write_text("id,description\nds1,Description\n")
        overlay = tmp_path / "overlay"
        overlay.mkdir()
        (overlay / "dataset.csv").write_text("id,description\nds1,!\n")

        catalog = Catalog(metadata_path=[base, overlay], quiet=True)
        ensure_metadata_applied(catalog)

        dataset = catalog.dataset.get("ds1")
        assert dataset is not None
        assert dataset.description is None

    def test_clear_relation_from_csv(self, tmp_path: Path):
        """A relation ! clears all accumulated relation IDs."""
        base = tmp_path / "base"
        base.mkdir()
        (base / "dataset.csv").write_text("id,tag_ids\nds1,t1\n")
        overlay = tmp_path / "overlay"
        overlay.mkdir()
        (overlay / "dataset.csv").write_text("id,tag_ids\nds1,!\n")

        catalog = Catalog(metadata_path=[base, overlay], quiet=True)
        ensure_metadata_applied(catalog)

        dataset = catalog.dataset.get("ds1")
        assert dataset is not None
        assert dataset.tag_ids == []

    def test_empty_values_still_leave_existing_values_unchanged(self, tmp_path: Path):
        """Empty metadata values are not clear instructions."""
        base = tmp_path / "base"
        base.mkdir()
        (base / "dataset.csv").write_text(
            "id,description,tag_ids\nds1,Description,t1\n"
        )
        overlay = tmp_path / "overlay"
        overlay.mkdir()
        (overlay / "dataset.csv").write_text("id,description,tag_ids\nds1,,\n")

        catalog = Catalog(metadata_path=[base, overlay], quiet=True)
        ensure_metadata_applied(catalog)

        dataset = catalog.dataset.get("ds1")
        assert dataset is not None
        assert dataset.description == "Description"
        assert dataset.tag_ids == ["t1"]

    def test_empty_json_array_still_leaves_existing_relation_unchanged(
        self, tmp_path: Path
    ):
        """JSON [] is not a clear instruction."""
        base = tmp_path / "base"
        base.mkdir()
        (base / "dataset.json").write_text('[{"id":"ds1","tag_ids":["t1"]}]')
        overlay = tmp_path / "overlay"
        overlay.mkdir()
        (overlay / "dataset.json").write_text('[{"id":"ds1","tag_ids":[]}]')

        catalog = Catalog(metadata_path=[base, overlay], quiet=True)
        ensure_metadata_applied(catalog)

        dataset = catalog.dataset.get("ds1")
        assert dataset is not None
        assert dataset.tag_ids == ["t1"]


class TestMetadataRelationRemovalInstructions:
    """Test !id metadata relation removal instructions."""

    def test_remove_relation_from_csv(self, tmp_path: Path):
        """A !id relation entry removes an accumulated relation ID."""
        base = tmp_path / "base"
        base.mkdir()
        (base / "dataset.csv").write_text('id,tag_ids\nds1,"t1,t2"\n')
        overlay = tmp_path / "overlay"
        overlay.mkdir()
        (overlay / "dataset.csv").write_text('id,tag_ids\nds1,"t3,!t1"\n')

        catalog = Catalog(metadata_path=[base, overlay], quiet=True)
        ensure_metadata_applied(catalog)

        dataset = catalog.dataset.get("ds1")
        assert dataset is not None
        assert dataset.tag_ids == ["t3", "t2"]

    def test_remove_relation_from_json(self, tmp_path: Path):
        """JSON !id relation entries are consumed as instructions."""
        base = tmp_path / "base"
        base.mkdir()
        (base / "dataset.json").write_text('[{"id":"ds1","tag_ids":["t1","t2"]}]')
        overlay = tmp_path / "overlay"
        overlay.mkdir()
        (overlay / "dataset.json").write_text('[{"id":"ds1","tag_ids":["t3","!t1"]}]')

        catalog = Catalog(metadata_path=[base, overlay], quiet=True)
        ensure_metadata_applied(catalog)

        dataset = catalog.dataset.get("ds1")
        assert dataset is not None
        assert dataset.tag_ids == ["t3", "t2"]

    def test_create_new_entity_consumes_relation_removal(self, tmp_path: Path):
        """New entities never store !id instruction entries."""
        metadata = tmp_path / "metadata"
        metadata.mkdir()
        (metadata / "dataset.csv").write_text('id,tag_ids\nds1,"t1,!t1,t2"\n')

        catalog = Catalog(metadata_path=metadata, quiet=True)
        ensure_metadata_applied(catalog)

        dataset = catalog.dataset.get("ds1")
        assert dataset is not None
        assert dataset.tag_ids == ["t2"]


class TestMetadataTombstones:
    """Test _delete metadata tombstones."""

    def test_tombstone_removes_entity_on_export(self, tmp_path: Path):
        """A _delete row removes the entity from the final export."""
        base = tmp_path / "base"
        base.mkdir()
        (base / "tag.csv").write_text("id,name\nt1,Base\n")
        overlay = tmp_path / "overlay"
        overlay.mkdir()
        (overlay / "tag.csv").write_text("id,_delete\nt1,true\n")
        out = tmp_path / "out"

        catalog = Catalog(metadata_path=[base, overlay], quiet=True)
        catalog.export_db(out)

        assert catalog.tag.get("t1") is None
        assert not (out / "tag.json").exists()

    def test_tombstone_cascades_tag_references(self, tmp_path: Path):
        """Tag tombstones remove references from relation fields."""
        meta = tmp_path / "meta"
        meta.mkdir()
        (meta / "tag.csv").write_text("id,name,_delete\nt1,Tag,true\n")
        (meta / "dataset.csv").write_text("id,tag_ids\nds1,t1\n")

        catalog = Catalog(metadata_path=meta, quiet=True)
        catalog.export_db(tmp_path / "out")

        dataset = catalog.dataset.get("ds1")
        assert dataset is not None
        assert dataset.tag_ids == []
        assert catalog.tag.get("t1") is None

    def test_tombstone_cascades_dataset_children(self, tmp_path: Path):
        """Dataset tombstones remove child variables and frequencies."""
        meta = tmp_path / "meta"
        meta.mkdir()
        (meta / "dataset.csv").write_text("id,name,_delete\nds1,Dataset,true\n")
        (meta / "variable.csv").write_text("id,name,dataset_id\nds1---v1,v1,ds1\n")
        (meta / "frequency.csv").write_text(
            "variable_id,value,frequency\nds1---v1,a,1\n"
        )

        catalog = Catalog(metadata_path=meta, quiet=True)
        catalog.export_db(tmp_path / "out")

        assert catalog.dataset.get("ds1") is None
        assert catalog.variable.get("ds1---v1") is None
        assert catalog.frequency.count == 0

    def test_add_metadata_applies_tombstones_immediately(self, tmp_path: Path):
        """Direct add_metadata consumes tombstones after applying rows."""
        catalog = Catalog(quiet=True)
        catalog.tag.add(Tag(id="t1", name="Tag"))
        catalog.dataset.add(Dataset(id="ds1", tag_ids=["t1"]))
        meta = tmp_path / "meta"
        meta.mkdir()
        (meta / "tag.csv").write_text("id,_delete\nt1,true\n")

        add_metadata(catalog, meta, quiet=True)

        dataset = catalog.dataset.get("ds1")
        assert dataset is not None
        assert dataset.tag_ids == []
        assert catalog.tag.get("t1") is None

    def test_tombstones_apply_all_supported_entity_cascades(self, tmp_path: Path):
        """Tombstones reuse cascade cleanup for every supported entity type."""
        catalog = Catalog(quiet=True)
        catalog.dataset.add(Dataset(id="ds1"))
        catalog.variable.add(
            Variable(
                id="ds1---v1",
                name="v1",
                dataset_id="ds1",
                enumeration_ids=["e1"],
                concept_id="c1",
            )
        )
        catalog.enumeration.add(Enumeration(id="e1"))
        catalog.value.add(Value(enumeration_id="e1", value="a"))
        catalog.organization.add(Organization(id="org1"))
        catalog.folder.add(
            Folder(
                id="f1", owner_organization_id="org1", manager_organization_id="org1"
            )
        )
        catalog.doc.add(Doc(id="doc1"))
        catalog.tag.add(Tag(id="t1"))
        catalog.concept.add(Concept(id="c1"))
        catalog.dataset.add(Dataset(id="ds2", tag_ids=["t1"], doc_ids=["doc1"]))
        catalog.variable.add(Variable(id="ds2---v1", name="v1", dataset_id="ds2"))
        meta = tmp_path / "meta"
        meta.mkdir()
        (meta / "variable.csv").write_text("id,dataset_id,_delete\nds2---v1,ds2,true\n")
        (meta / "enumeration.csv").write_text("id,_delete\ne1,true\n")
        (meta / "organization.csv").write_text("id,_delete\norg1,true\n")
        (meta / "tag.csv").write_text("id,_delete\nt1,true\n")
        (meta / "doc.csv").write_text("id,_delete\ndoc1,true\n")
        (meta / "concept.csv").write_text("id,_delete\nc1,true\n")

        add_metadata(catalog, meta, quiet=True)

        assert catalog.variable.get("ds2---v1") is None
        assert catalog.enumeration.get("e1") is None
        assert catalog.value.count == 0
        assert catalog.organization.get("org1") is None
        assert catalog.tag.get("t1") is None
        assert catalog.doc.get("doc1") is None
        assert catalog.concept.get("c1") is None
        folder = catalog.folder.get("f1")
        assert folder is not None
        assert folder.owner_organization_id is None
        kept_dataset = catalog.dataset.get("ds2")
        assert kept_dataset is not None
        assert kept_dataset.tag_ids == []
        assert kept_dataset.doc_ids == []
        kept_variable = catalog.variable.get("ds1---v1")
        assert kept_variable is not None
        assert kept_variable.enumeration_ids == []
        assert kept_variable.concept_id is None

    def test_folder_tombstone_removes_descendants_and_contents(self, tmp_path: Path):
        """Folder tombstones remove descendants and contained entities."""
        catalog = Catalog(quiet=True)
        catalog.folder.add(Folder(id="root"))
        catalog.folder.add(Folder(id="child", parent_id="root"))
        catalog.folder.add(Folder(id="other"))
        catalog.dataset.add(Dataset(id="ds1", folder_id="child"))
        catalog.dataset.add(Dataset(id="ds2", folder_id="other"))
        catalog.variable.add(Variable(id="ds1---v1", name="v1", dataset_id="ds1"))
        catalog.variable.add(Variable(id="ds2---v1", name="v1", dataset_id="ds2"))
        catalog.frequency.add(Frequency(variable_id="ds1---v1", value="a", frequency=1))
        catalog.enumeration.add(Enumeration(id="e1", folder_id="child"))
        catalog.value.add(Value(enumeration_id="e1", value="a"))
        meta = tmp_path / "meta"
        meta.mkdir()
        (meta / "folder.csv").write_text("id,_delete\nroot,true\n")

        add_metadata(catalog, meta, quiet=True)

        assert catalog.folder.get("root") is None
        assert catalog.folder.get("child") is None
        assert catalog.folder.get("other") is not None
        assert catalog.dataset.get("ds1") is None
        assert catalog.dataset.get("ds2") is not None
        assert catalog.variable.get("ds1---v1") is None
        assert catalog.variable.get("ds2---v1") is not None
        assert catalog.frequency.count == 0
        assert catalog.enumeration.get("e1") is None
        assert catalog.value.count == 0

    def test_tombstones_ignore_value_and_frequency(self, tmp_path: Path):
        """Composite-key tables are out of scope for direct tombstones."""
        meta = tmp_path / "meta"
        meta.mkdir()
        (meta / "value.csv").write_text("enumeration_id,value,_delete\ne1,a,true\n")
        (meta / "frequency.csv").write_text(
            "variable_id,value,frequency,_delete\nv1,a,1,true\n"
        )

        catalog = Catalog(metadata_path=meta, quiet=True)

        assert catalog._metadata_tombstones == {}

    def test_delete_value_parsing(self):
        """_delete accepts explicit truthy values only."""
        import math

        assert _is_truthy_delete(True)
        assert _is_truthy_delete(1)
        assert _is_truthy_delete("true")
        assert _is_truthy_delete("YES")
        assert _is_truthy_delete(["value"])
        assert not _is_truthy_delete(False)
        assert not _is_truthy_delete(0)
        assert not _is_truthy_delete("false")
        assert not _is_truthy_delete(None)
        assert not _is_truthy_delete(math.nan)

    def test_tombstone_extraction_edge_cases(self):
        """Tombstone extraction ignores unsupported and incomplete rows."""
        import pandas as pd

        tables: dict[str, tuple[pd.DataFrame, str]] = {
            "value": (
                pd.DataFrame({"id": ["ignored"], "_delete": [True]}),
                "value.csv",
            ),
            "tag": (
                pd.DataFrame({"name": ["Missing id"], "_delete": [True]}),
                "tag.csv",
            ),
            "doc": (pd.DataFrame({"id": [None], "_delete": [True]}), "doc.csv"),
            "concept": (
                pd.DataFrame({"id": ["c1"], "_delete": [False]}),
                "concept.csv",
            ),
        }

        assert _extract_tombstone_ids(tables) == {}


class TestAppMetadataOverlayPath:
    """Test automatic app_path/data/db-ui metadata source discovery."""

    def test_app_db_ui_is_used_without_metadata_path(self, tmp_path: Path):
        """app_path/data/db-ui is applied even when metadata_path is not configured."""
        app_path = tmp_path / "app"
        db_ui = app_path / "data" / "db-ui"
        db_ui.mkdir(parents=True)
        (db_ui / "tag.json").write_text(
            '[{"id":"ui","name":"UI","name:fr":"Interface"}]'
        )

        catalog = Catalog(app_path=app_path, quiet=True)
        ensure_metadata_applied(catalog)

        tag = catalog.tag.get("ui")
        assert tag is not None
        assert tag.name == "UI"
        row = catalog.tag.df.to_dicts()[0]
        assert row["name:fr"] == "Interface"

    def test_app_db_ui_localized_fields_are_exported(self, tmp_path: Path):
        """Localized fields from app_path/data/db-ui should survive export."""
        app_path = tmp_path / "app"
        db_ui = app_path / "data" / "db-ui"
        db_ui.mkdir(parents=True)
        (db_ui / "dataset.json").write_text(
            '[{"id":"ds","name":"Dataset","name:fr":"Jeu de données"}]'
        )

        catalog = Catalog(app_path=app_path, quiet=True)
        catalog.export_db()

        rows = json.loads((app_path / "data" / "db" / "dataset.json").read_text())
        assert rows == [
            {
                "id": "ds",
                "name": "Dataset",
                "name:fr": "Jeu de données",
            }
        ]

    def test_app_db_ui_is_applied_after_configured_metadata_path(self, tmp_path: Path):
        """app_path/data/db-ui overrides configured metadata sources."""
        base = tmp_path / "base"
        base.mkdir()
        (base / "tag.csv").write_text("id,name\nt1,Base\n")
        app_path = tmp_path / "app"
        db_ui = app_path / "data" / "db-ui"
        db_ui.mkdir(parents=True)
        (db_ui / "tag.json").write_text('[{"id":"t1","name":"UI"}]')

        catalog = Catalog(app_path=app_path, metadata_path=base, quiet=True)
        ensure_metadata_applied(catalog)

        tag = catalog.tag.get("t1")
        assert tag is not None
        assert tag.name == "UI"

    def test_app_db_ui_is_not_duplicated_when_configured(self, tmp_path: Path):
        """Explicitly configured app_path/data/db-ui is not loaded twice."""
        app_path = tmp_path / "app"
        db_ui = app_path / "data" / "db-ui"
        db_ui.mkdir(parents=True)
        (db_ui / "tag.json").write_text('[{"id":"ui","name":"UI"}]')

        catalog = Catalog(app_path=app_path, metadata_path=db_ui, quiet=True)

        assert catalog.metadata_path == db_ui
        assert len(catalog._loaded_metadata or []) == 1

    def test_app_db_ui_does_not_mutate_metadata_path_list(self, tmp_path: Path):
        """Composing the effective path does not mutate caller-owned lists."""
        base = tmp_path / "base"
        base.mkdir()
        (base / "tag.csv").write_text("id,name\nt1,Base\n")
        app_path = tmp_path / "app"
        db_ui = app_path / "data" / "db-ui"
        db_ui.mkdir(parents=True)
        (db_ui / "tag.json").write_text('[{"id":"t1","name":"UI"}]')
        metadata_path: list[str | Path] = [base]

        catalog = Catalog(app_path=app_path, metadata_path=metadata_path, quiet=True)

        assert metadata_path == [base]
        assert catalog.metadata_path == [base, db_ui]
