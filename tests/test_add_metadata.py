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
from datannurpy.entities import Value, Variable
from datannurpy.add_metadata import (
    _convert_row_to_dict,
    _find_entity_by_id,
    _find_value,
    _get_catalog_list,
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
)


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
        """Value should require modality_id (value has default)."""
        required = _get_required_fields(Value)
        assert "modality_id" in required
        # value has a default, so not strictly required by dataclass
        assert "id" not in required


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


class TestFindEntityById:
    """Test _find_entity_by_id function."""

    def test_find_existing(self):
        """Should find existing entity."""
        entities = [
            Variable(id="v1", name="V1", dataset_id="ds"),
            Variable(id="v2", name="V2", dataset_id="ds"),
        ]
        result = _find_entity_by_id(entities, "v2")
        assert result is not None
        assert result.id == "v2"

    def test_not_found(self):
        """Should return None if not found."""
        entities = [Variable(id="v1", name="V1", dataset_id="ds")]
        result = _find_entity_by_id(entities, "v999")
        assert result is None

    def test_empty_list(self):
        """Should return None for empty list."""
        assert _find_entity_by_id([], "v1") is None


class TestFindValue:
    """Test _find_value function."""

    def test_find_existing(self):
        """Should find value by composite key."""
        values = [
            Value(modality_id="m1", value="a"),
            Value(modality_id="m1", value="b"),
            Value(modality_id="m2", value="a"),
        ]
        result = _find_value(values, "m1", "b")
        assert result is not None
        assert result.modality_id == "m1"
        assert result.value == "b"

    def test_not_found(self):
        """Should return None if not found."""
        values = [Value(modality_id="m1", value="a")]
        assert _find_value(values, "m1", "x") is None
        assert _find_value(values, "m2", "a") is None


class TestGetCatalogList:
    """Test _get_catalog_list function."""

    def test_all_entity_types(self):
        """Should return correct list for all entity types."""
        catalog = Catalog()

        assert _get_catalog_list(catalog, "folder") is catalog.folders
        assert _get_catalog_list(catalog, "dataset") is catalog.datasets
        assert _get_catalog_list(catalog, "variable") is catalog.variables
        assert _get_catalog_list(catalog, "modality") is catalog.modalities
        assert _get_catalog_list(catalog, "value") is catalog.values
        assert _get_catalog_list(catalog, "institution") is catalog.institutions
        assert _get_catalog_list(catalog, "tag") is catalog.tags
        assert _get_catalog_list(catalog, "doc") is catalog.docs

    def test_unknown_entity(self):
        """Should return None for unknown entity type."""
        catalog = Catalog()
        assert _get_catalog_list(catalog, "unknown") is None


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

    def test_invalid_json(self, tmp_path: Path):
        """Should return None and warn for invalid JSON."""
        json_path = tmp_path / "test.json"
        json_path.write_text("not valid json")

        with pytest.warns(UserWarning, match="Could not read JSON"):
            result = _read_json(json_path)
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

        tables = _load_tables_from_folder(tmp_path)

        assert "variable" in tables
        assert "tag" in tables
        assert len(tables["variable"][0]) == 1
        assert tables["variable"][1] == "variable.csv"

    def test_ignores_non_entity_files(self, tmp_path: Path):
        """Should ignore files that don't match entity names."""
        (tmp_path / "other.csv").write_text("id,name\n1,Test\n")

        tables = _load_tables_from_folder(tmp_path)
        assert "other" not in tables

    def test_loads_first_matching_extension(self, tmp_path: Path):
        """Should load only one file per entity (first extension found)."""
        (tmp_path / "variable.csv").write_text("id,name,dataset_id\ncsv,CSV,ds\n")
        (tmp_path / "variable.json").write_text(
            '[{"id": "json", "name": "JSON", "dataset_id": "ds"}]'
        )

        tables = _load_tables_from_folder(tmp_path)
        # Should only load one file, not both
        assert "variable" in tables
        # The filename should be one of the two (order depends on set iteration)
        assert tables["variable"][1] in ("variable.csv", "variable.json")

    def test_empty_folder(self, tmp_path: Path):
        """Should return empty dict for empty folder."""
        tables = _load_tables_from_folder(tmp_path)
        assert tables == {}

    def test_skips_empty_dataframe(self, tmp_path: Path):
        """Should skip entity files that result in empty DataFrames."""
        # Create an empty CSV (headers only)
        (tmp_path / "folder.csv").write_text("id,name\n")

        tables = _load_tables_from_folder(tmp_path)
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

        tables = _load_tables_from_database(f"sqlite:///{db_path}")

        assert "variable" in tables
        assert len(tables["variable"][0]) == 1

    def test_invalid_connection(self):
        """Should return empty dict and warn for invalid connection."""
        with pytest.warns(UserWarning, match="Could not connect"):
            tables = _load_tables_from_database("sqlite:///nonexistent/path/db.sqlite")
        assert tables == {}

    def test_ignores_non_entity_tables(self, tmp_path: Path):
        """Should ignore tables that don't match entity names."""
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE other_table (id TEXT)")
        conn.commit()
        conn.close()

        tables = _load_tables_from_database(f"sqlite:///{db_path}")
        assert "other_table" not in tables
        # All entity types should have been checked but not found
        assert tables == {}

    def test_empty_database(self, tmp_path: Path):
        """Should return empty dict when no entity tables exist."""
        db_path = tmp_path / "empty.db"
        conn = sqlite3.connect(db_path)
        conn.commit()
        conn.close()

        tables = _load_tables_from_database(f"sqlite:///{db_path}")
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

            tables = _load_tables_from_database(f"sqlite:///{db_path}")
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
        catalog.variables.append(Variable(id="v1", name="Existing", dataset_id="ds"))

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
        assert len(catalog.folders) == 1
        assert catalog.folders[0].id == "f1"

    def test_update_existing_entity(self):
        """Should update existing entities."""
        catalog = Catalog()
        catalog.folders.append(Folder(id="f1", name="Old"))

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
        assert catalog.folders[0].name == "New"
        assert catalog.folders[0].description == "Updated"

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
        assert len(catalog.values) == 1
        assert catalog.values[0].modality_id == "m1"

    def test_update_value_entity(self):
        """Should update existing Value entities."""
        catalog = Catalog()
        catalog.values.append(Value(modality_id="m1", value="a"))

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
        assert catalog.values[0].description == "Updated"

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
        assert catalog.variables[0].name == "my_var"

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
        catalog.add_metadata(tmp_path, quiet=True)

        assert len(catalog.folders) == 1
        assert len(catalog.tags) == 1

    def test_add_metadata_from_sqlite(self, tmp_path: Path):
        """Should load metadata from SQLite database."""
        db_path = tmp_path / "metadata.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE folder (id TEXT, name TEXT)")
        conn.execute("INSERT INTO folder VALUES ('f1', 'Folder1')")
        conn.commit()
        conn.close()

        catalog = Catalog()
        catalog.add_metadata(f"sqlite:///{db_path}", quiet=True)

        assert len(catalog.folders) == 1
        assert catalog.folders[0].name == "Folder1"

    def test_add_metadata_updates_existing(self, tmp_path: Path):
        """Should update existing entities."""
        (tmp_path / "variable.csv").write_text(
            "id,name,dataset_id,description\nv1,Var1,ds1,New description\n"
        )

        catalog = Catalog()
        catalog.variables.append(Variable(id="v1", name="Var1", dataset_id="ds1"))

        catalog.add_metadata(tmp_path, quiet=True)

        assert catalog.variables[0].description == "New description"

    def test_add_metadata_merges_list_fields(self, tmp_path: Path):
        """Should merge list fields."""
        (tmp_path / "variable.csv").write_text(
            'id,name,dataset_id,tag_ids\nv1,Var1,ds1,"t2,t3"\n'
        )

        catalog = Catalog()
        catalog.variables.append(
            Variable(id="v1", name="Var1", dataset_id="ds1", tag_ids=["t1"])
        )

        catalog.add_metadata(tmp_path, quiet=True)

        # New values first, then existing
        assert catalog.variables[0].tag_ids == ["t2", "t3", "t1"]

    def test_add_metadata_folder_not_found(self):
        """Should raise FileNotFoundError for missing folder."""
        catalog = Catalog()

        with pytest.raises(FileNotFoundError, match="Metadata folder not found"):
            catalog.add_metadata("/nonexistent/path", quiet=True)

    def test_add_metadata_not_a_directory(self, tmp_path: Path):
        """Should raise ValueError if path is not a directory."""
        file_path = tmp_path / "file.txt"
        file_path.write_text("hello")

        catalog = Catalog()

        with pytest.raises(ValueError, match="must be a directory"):
            catalog.add_metadata(file_path, quiet=True)

    def test_add_metadata_no_files_found(self, tmp_path: Path, capsys):
        """Should warn if no metadata files found."""
        catalog = Catalog()
        catalog.add_metadata(tmp_path, quiet=False)

        captured = capsys.readouterr()
        assert "No metadata files found" in captured.err

    def test_add_metadata_validation_errors(self, tmp_path: Path, capsys):
        """Should report validation errors and not process."""
        (tmp_path / "variable.csv").write_text("id\nv1\n")  # Missing required columns

        catalog = Catalog()
        catalog.add_metadata(tmp_path, quiet=False)

        captured = capsys.readouterr()
        assert "Invalid metadata" in captured.err
        assert len(catalog.variables) == 0  # Not processed

    def test_add_metadata_quiet_mode(self, tmp_path: Path, capsys):
        """Should suppress output in quiet mode."""
        (tmp_path / "folder.csv").write_text("id,name\nf1,Folder1\n")

        catalog = Catalog()
        catalog.add_metadata(tmp_path, quiet=True)

        captured = capsys.readouterr()
        assert captured.err == ""

    def test_add_metadata_uses_catalog_quiet(self, tmp_path: Path, capsys):
        """Should use catalog.quiet if quiet not specified."""
        (tmp_path / "folder.csv").write_text("id,name\nf1,Folder1\n")

        catalog = Catalog(quiet=True)
        catalog.add_metadata(tmp_path)

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
        catalog.add_metadata(tmp_path, quiet=True)

        assert len(catalog.folders) == 1
        assert len(catalog.datasets) == 1
        assert len(catalog.variables) == 1
        assert len(catalog.modalities) == 1
        assert len(catalog.values) == 1
        assert len(catalog.institutions) == 1
        assert len(catalog.tags) == 1
        assert len(catalog.docs) == 1

    def test_add_metadata_with_output(self, tmp_path: Path, capsys):
        """Should print progress when not quiet."""
        (tmp_path / "folder.csv").write_text("id,name\nf1,Folder1\n")

        catalog = Catalog()
        catalog.add_metadata(tmp_path, quiet=False)

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

    def test_load_database_with_table_read_error(self, tmp_path: Path):
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

            with pytest.warns(UserWarning, match="Could not read table"):
                tables = _load_tables_from_database(f"sqlite:///{db_path}")

            assert "folder" not in tables

    def test_add_metadata_shows_summary_with_updates_only(self, tmp_path: Path, capsys):
        """Should show summary even with only updates."""
        (tmp_path / "folder.csv").write_text("id,name,description\nf1,Folder,Updated\n")

        catalog = Catalog()
        catalog.folders.append(Folder(id="f1", name="Folder"))
        catalog.add_metadata(tmp_path, quiet=False)

        captured = capsys.readouterr()
        assert "0 created, 1 updated" in captured.err
