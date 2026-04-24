"""Tests for Catalog.add_metadata."""

from __future__ import annotations

import sqlite3
from collections.abc import Hashable
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from datannurpy import Catalog, Folder
from datannurpy.errors import ConfigError
from datannurpy.schema import Freq, Value, Variable
from datannurpy.utils.ids import build_freq_id, build_value_id
from datannurpy.add_metadata import (
    DEPTH_ENTITIES,
    FREQ_HIDDEN_TAG,
    _convert_row_to_dict,
    _extract_freq_hidden_ids,
    _get_catalog_table,
    _get_required_fields,
    _is_database_connection,
    _load_tables_from_database,
    _load_tables_from_folder,
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
        assert "modality_id" not in required  # has default ""


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


class TestGetCatalogTable:
    """Test _get_catalog_table function."""

    def test_all_entity_types(self):
        """Should return correct table for all entity types."""
        catalog = Catalog()

        assert _get_catalog_table(catalog, "folder") is catalog.folder
        assert _get_catalog_table(catalog, "dataset") is catalog.dataset
        assert _get_catalog_table(catalog, "variable") is catalog.variable
        assert _get_catalog_table(catalog, "modality") is catalog.modality
        assert _get_catalog_table(catalog, "value") is catalog.value
        assert _get_catalog_table(catalog, "freq") is catalog.freq
        assert _get_catalog_table(catalog, "institution") is catalog.institution
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
        assert "✗ test.json" in captured.err
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
        assert "✗ database" in captured.err
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
        df = pd.DataFrame({"modality_id": ["m1"], "value": ["a"]})

        errors = _validate_entity_table(catalog, "value", df, "value.csv")
        assert errors == []

    def test_skip_validation_for_freq_entity(self):
        """Freq entity uses composite key, no id validation."""
        catalog = Catalog()
        df = pd.DataFrame({"variable_id": ["v1"], "value": ["a"], "freq": [5]})

        errors = _validate_entity_table(catalog, "freq", df, "freq.csv")
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

    def test_create_value_entity(self):
        """Should create Value entities with composite key."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "modality_id": ["m1"],
                "value": ["a"],
                "description": ["Value A"],
            }
        )

        created, updated = _process_entity_table(catalog, "value", df)

        assert created == 1
        assert updated == 0
        assert len(catalog.value.all()) == 1
        assert catalog.value.all()[0].modality_id == "m1"

    def test_update_value_entity(self):
        """Should update existing Value entities."""
        catalog = Catalog()
        catalog.value.add(
            Value(id=build_value_id("m1", "a"), modality_id="m1", value="a")
        )

        df = pd.DataFrame(
            {
                "modality_id": ["m1"],
                "value": ["a"],
                "description": ["Updated"],
            }
        )

        created, updated = _process_entity_table(catalog, "value", df)

        assert created == 0
        assert updated == 1
        assert catalog.value.all()[0].description == "Updated"

    def test_skip_value_without_required_fields(self):
        """Should skip Value without modality_id or value."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "modality_id": ["m1", None],
                "value": [None, "a"],
            }
        )

        created, updated = _process_entity_table(catalog, "value", df)
        assert created == 0  # Both skipped

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

    def test_create_freq_entity(self):
        """Should create Freq entities with composite key."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "variable_id": ["v1"],
                "value": ["a"],
                "freq": [5],
            }
        )

        created, updated = _process_entity_table(catalog, "freq", df)

        assert created == 1
        assert updated == 0
        assert len(catalog.freq.all()) == 1
        assert catalog.freq.all()[0].variable_id == "v1"
        assert catalog.freq.all()[0].value == "a"
        assert catalog.freq.all()[0].freq == 5

    def test_update_freq_entity(self):
        """Should update existing Freq entities."""
        catalog = Catalog()
        freq_id = build_freq_id("v1", "a")
        catalog.freq.add(Freq(id=freq_id, variable_id="v1", value="a", freq=3))

        df = pd.DataFrame(
            {
                "variable_id": ["v1"],
                "value": ["a"],
                "freq": [10],
            }
        )

        created, updated = _process_entity_table(catalog, "freq", df)

        assert created == 0
        assert updated == 1
        assert catalog.freq.all()[0].freq == 10

    def test_skip_freq_without_required_fields(self):
        """Should skip Freq without variable_id or value."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "variable_id": ["v1", None],
                "value": [None, "a"],
                "freq": [5, 5],
            }
        )

        created, updated = _process_entity_table(catalog, "freq", df)
        assert created == 0  # Both skipped

    def test_update_value_without_description(self):
        """Updating an existing Value without description in CSV should be a no-op update."""
        catalog = Catalog()
        value_id = build_value_id("m1", "a")
        catalog.value.add(
            Value(id=value_id, modality_id="m1", value="a", description="kept")
        )

        df = pd.DataFrame({"modality_id": ["m1"], "value": ["a"]})

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
                "modality_id": ["m1", "m1", "m2", "m2"],
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

    def test_duplicate_composite_key_in_same_csv_freq(self):
        """Duplicate composite key for a new Freq in same CSV should be merged."""
        catalog = Catalog()
        df = pd.DataFrame(
            {
                "variable_id": ["v1", "v1"],
                "value": ["a", "a"],
                "freq": [1, 7],
            }
        )

        created, updated = _process_entity_table(catalog, "freq", df)

        assert created == 1
        assert updated == 0
        assert catalog.freq.all()[0].freq == 7


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
        (tmp_path / "modality.csv").write_text("id,name,type\nm1,Mod,string\n")
        (tmp_path / "value.csv").write_text(
            "modality_id,value,description\nm1,a,Val A\n"
        )
        (tmp_path / "institution.csv").write_text("id,name\ni1,Inst\n")
        (tmp_path / "tag.csv").write_text("id,name\nt1,Tag\n")
        (tmp_path / "doc.csv").write_text("id,name\ndoc1,Doc\n")

        catalog = Catalog()
        add_metadata(catalog, tmp_path, quiet=True)

        assert len(catalog.folder.all()) == 1
        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 1
        assert len(catalog.modality.all()) == 1
        assert len(catalog.value.all()) == 1
        assert len(catalog.institution.all()) == 1
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
            assert "✗ folder" in captured.err
            assert "Table read error" in captured.err
            assert "folder" not in tables

    def test_add_metadata_shows_summary_with_updates_only(self, tmp_path: Path, capsys):
        """Should show summary even with only updates."""
        (tmp_path / "folder.csv").write_text("id,name,description\nf1,Folder,Updated\n")

        catalog = Catalog()
        catalog.folder.add(Folder(id="f1", name="Folder"))
        add_metadata(catalog, tmp_path, quiet=False)

        captured = capsys.readouterr()
        assert "0 created, 1 updated" in captured.err

    def test_merge_entity_marks_seen(self):
        """_merge_entity should set _seen=True on entities with that attribute."""
        folder = Folder(id="f1", name="Old")
        assert folder._seen is False
        _merge_entity(folder, {"name": "New"})
        assert folder._seen is True

    def test_value_update_with_description(self, tmp_path: Path):
        """Updating existing value with description should apply it."""
        from datannurpy.schema import Modality

        (tmp_path / "value.csv").write_text(
            "modality_id,value,description\nm1,A,Updated desc\n"
        )

        catalog = Catalog()
        catalog.modality.add(Modality(id="m1", name="Mod"))
        catalog.value.add(
            Value(id=build_value_id("m1", "A"), modality_id="m1", value="A")
        )

        add_metadata(catalog, tmp_path, quiet=True)
        val = catalog.value.all()[0]
        assert val.description == "Updated desc"

    def test_value_update_without_description(self, tmp_path: Path):
        """Updating existing value without description should keep None."""
        from datannurpy.schema import Modality

        (tmp_path / "value.csv").write_text("modality_id,value\nm1,A\n")

        catalog = Catalog()
        catalog.modality.add(Modality(id="m1", name="Mod"))
        catalog.value.add(
            Value(
                id=build_value_id("m1", "A"),
                modality_id="m1",
                value="A",
                description="Old",
            )
        )

        add_metadata(catalog, tmp_path, quiet=True)
        val = catalog.value.all()[0]
        assert val.description == "Old"

    def test_value_create_marks_parent_modality_seen(self, tmp_path: Path):
        """Creating new value should mark parent modality as seen."""
        from datannurpy.schema import Modality

        (tmp_path / "value.csv").write_text(
            "modality_id,value,description\nm1,B,New val\n"
        )

        catalog = Catalog()
        # Add existing modality with _seen=False
        catalog.modality.add(Modality(id="m1", name="Mod", _seen=False))

        add_metadata(catalog, tmp_path, quiet=True)

        # New value should be created
        assert len(catalog.value.all()) == 1
        # Parent modality should be marked as seen
        mod = catalog.modality.get("m1")
        assert mod is not None
        assert mod._seen is True

    def test_freq_create_from_csv(self, tmp_path: Path):
        """Should create freq entries from CSV."""
        (tmp_path / "freq.csv").write_text(
            "variable_id,value,freq\nv1,red,10\nv1,blue,5\n"
        )

        catalog = Catalog()
        add_metadata(catalog, tmp_path, quiet=True)

        assert len(catalog.freq.all()) == 2
        freqs = {f.value: f.freq for f in catalog.freq.all()}
        assert freqs["red"] == 10
        assert freqs["blue"] == 5

    def test_freq_update_from_csv(self, tmp_path: Path):
        """Should update existing freq from CSV."""
        freq_id = build_freq_id("v1", "red")
        catalog = Catalog()
        catalog.freq.add(Freq(id=freq_id, variable_id="v1", value="red", freq=3))

        (tmp_path / "freq.csv").write_text("variable_id,value,freq\nv1,red,20\n")
        add_metadata(catalog, tmp_path, quiet=True)

        assert len(catalog.freq.all()) == 1
        assert catalog.freq.all()[0].freq == 20


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
        """Should extract variable IDs with policy---freq-hidden tag."""
        (tmp_path / "variable.csv").write_text(
            "id,name,dataset_id,tag_ids\n"
            f'd---v1,v1,d,"{FREQ_HIDDEN_TAG}"\n'
            "d---v2,v2,d,other\n"
        )
        catalog = Catalog(metadata_path=tmp_path, quiet=True)
        assert catalog._freq_hidden_ids == {"d---v1"}

    def test_freq_hidden_with_multiple_tags(self, tmp_path: Path):
        """Should detect freq-hidden even with other tags."""
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


class TestFreqHiddenPolicy:
    """Tests for policy---freq-hidden during scan and export."""

    def _make_csv(self, path: Path) -> Path:
        """Create a CSV with two low-cardinality columns."""
        csv_path = path / "data.csv"
        csv_path.write_text("name,color\nAlice,red\nBob,blue\nAlice,red\n")
        return csv_path

    def _make_metadata(self, path: Path, folder_id: str) -> Path:
        """Create metadata tagging 'name' column as freq-hidden."""
        meta = path / "meta"
        meta.mkdir()
        (meta / "variable.csv").write_text(
            f"id,name,dataset_id,tag_ids\n"
            f"{folder_id}---data_csv---name,name,{folder_id}---data_csv,"
            f'"{FREQ_HIDDEN_TAG}"\n'
        )
        return meta

    def test_hidden_var_has_no_freq(self, tmp_path: Path):
        """Freq-hidden variable should have no freq rows."""
        self._make_csv(tmp_path)
        meta = self._make_metadata(tmp_path, "src")
        catalog = Catalog(metadata_path=meta, quiet=True)
        catalog.add_folder(tmp_path, folder=Folder(id="src"), include="*.csv")
        ensure_metadata_applied(catalog)

        name_var = catalog.variable.get("src---data_csv---name")
        color_var = catalog.variable.get("src---data_csv---color")
        assert name_var is not None
        assert color_var is not None

        # name: no modality, no freq
        assert not name_var.modality_ids
        freq_rows = [
            f for f in catalog.freq.all() if f.variable_id == "src---data_csv---name"
        ]
        assert freq_rows == []

        # color: has modality and freq (not hidden)
        assert color_var.modality_ids
        color_freqs = [
            f for f in catalog.freq.all() if f.variable_id == "src---data_csv---color"
        ]
        assert len(color_freqs) > 0

    def test_hidden_var_keeps_stats(self, tmp_path: Path):
        """Freq-hidden variable should still have stats."""
        self._make_csv(tmp_path)
        meta = self._make_metadata(tmp_path, "src")
        catalog = Catalog(metadata_path=meta, quiet=True)
        catalog.add_folder(tmp_path, folder=Folder(id="src"), include="*.csv")

        name_var = catalog.variable.get("src---data_csv---name")
        assert name_var is not None
        assert name_var.nb_distinct == 2

    def test_hidden_tag_applied_after_metadata(self, tmp_path: Path):
        """The policy tag should appear on the variable after metadata apply."""
        self._make_csv(tmp_path)
        meta = self._make_metadata(tmp_path, "src")
        catalog = Catalog(metadata_path=meta, quiet=True)
        catalog.add_folder(tmp_path, folder=Folder(id="src"), include="*.csv")
        ensure_metadata_applied(catalog)

        name_var = catalog.variable.get("src---data_csv---name")
        assert name_var is not None
        assert FREQ_HIDDEN_TAG in (name_var.tag_ids or [])

    def test_no_hidden_ids_without_metadata(self, tmp_path: Path):
        """Without metadata_path, no variables are hidden."""
        self._make_csv(tmp_path)
        catalog = Catalog(quiet=True)
        catalog.add_folder(tmp_path, folder=Folder(id="src"), include="*.csv")

        name_var = catalog.variable.get("src---data_csv---name")
        assert name_var is not None
        # name should have freq (not hidden)
        freq_rows = [
            f for f in catalog.freq.all() if f.variable_id == "src---data_csv---name"
        ]
        assert len(freq_rows) > 0

    def test_multiple_hidden_variables(self, tmp_path: Path):
        """Multiple variables tagged freq-hidden should all be suppressed."""
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
        catalog.add_folder(tmp_path, folder=Folder(id="src"), include="*.csv")
        ensure_metadata_applied(catalog)

        for col in ("name", "color"):
            var = catalog.variable.get(f"src---data_csv---{col}")
            assert var is not None
            assert not var.modality_ids
            assert [f for f in catalog.freq.all() if f.variable_id == var.id] == []

        # age is not hidden — should have freq
        age_var = catalog.variable.get("src---data_csv---age")
        assert age_var is not None
        age_freqs = [f for f in catalog.freq.all() if f.variable_id == age_var.id]
        assert len(age_freqs) > 0


class TestConceptEntity:
    """Test concept entity loading."""

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

    def test_unseen_concept_removed_on_finalize(self, tmp_path: Path):
        """Concepts with _seen=False should be removed on finalize."""
        (tmp_path / "concept.csv").write_text("id,name\nc1,Concept 1\n")
        (tmp_path / "data.csv").write_text("a,b\n1,2\n")
        meta = tmp_path / "meta"
        meta.mkdir()
        (meta / "concept.csv").write_text("id,name\nc1,C1\n")
        catalog = Catalog(metadata_path=meta, quiet=True)
        catalog.add_folder(tmp_path, folder=Folder(id="src"), include="data.csv")
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

    def test_list_unions_freq_hidden_ids(self, tmp_path: Path):
        """freq-hidden ids are unioned across all sources."""
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
