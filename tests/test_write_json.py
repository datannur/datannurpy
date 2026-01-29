"""Tests for Catalog.write method."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from datannurpy import Catalog, Folder
from datannurpy.exporter.db import build_jsonjs, clean_value, write_atomic

DATA_DIR = Path(__file__).parent.parent / "data"
CSV_DIR = DATA_DIR / "csv"


class TestCleanValue:
    """Test clean_value helper function."""

    def test_float_whole_number_to_int(self):
        """Whole floats like 5.0 should be converted to int."""
        assert clean_value(5.0) == 5
        assert isinstance(clean_value(5.0), int)

    def test_float_decimal_unchanged(self):
        """Floats with decimals should remain floats."""
        assert clean_value(5.5) == 5.5
        assert isinstance(clean_value(5.5), float)

    def test_int_unchanged(self):
        """Integers should remain integers."""
        assert clean_value(5) == 5
        assert isinstance(clean_value(5), int)

    def test_string_unchanged(self):
        """Strings should remain strings."""
        assert clean_value("hello") == "hello"


class TestBuildJsonJs:
    """Test build_jsonjs helper function."""

    def test_empty_data(self):
        """Empty data should return empty array assignment."""
        result = build_jsonjs([], "test")
        assert result == "jsonjs.data['test'] = []"


class TestWriteAtomic:
    """Test write_atomic helper function."""

    def test_cleanup_on_error(self, tmp_path: Path):
        """Temp file should be cleaned up on rename error."""
        path = tmp_path / "test.json"

        with patch("pathlib.Path.rename", side_effect=OSError("disk full")):
            with pytest.raises(OSError, match="disk full"):
                write_atomic(path, "content")

        # Temp file should be cleaned up
        assert not (tmp_path / "test.json.temp").exists()


class TestFreq:
    """Test frequency computation."""

    def test_freq_default_enabled(self, tmp_path: Path):
        """Catalog should compute freq by default (threshold=100)."""
        (tmp_path / "data.csv").write_text("color\nred\nblue\nred\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)
        catalog.export_db(tmp_path / "output")

        assert (tmp_path / "output" / "freq.json").exists()

    def test_freq_disabled(self, tmp_path: Path):
        """Catalog(freq_threshold=0) should not compute freq."""
        (tmp_path / "data.csv").write_text("color\nred\nblue\n")

        catalog = Catalog(freq_threshold=0)
        catalog.add_folder(tmp_path)
        catalog.export_db(tmp_path / "output")

        assert not (tmp_path / "output" / "freq.json").exists()

    def test_freq_threshold(self, tmp_path: Path):
        """freq_threshold should filter columns by nb_distinct."""
        (tmp_path / "data.csv").write_text("a,b\n1,x\n2,y\n3,z\n")

        catalog = Catalog(freq_threshold=2)
        catalog.add_folder(tmp_path)
        catalog.export_db(tmp_path / "output")

        # Both columns have 3 distinct > threshold 2
        assert not (tmp_path / "output" / "freq.json").exists()

    def test_freq_content(self, tmp_path: Path):
        """freq.json should contain value counts."""
        (tmp_path / "data.csv").write_text("color\nred\nred\nblue\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)
        catalog.export_db(tmp_path / "output")

        with open(tmp_path / "output" / "freq.json") as f:
            data = json.load(f)

        values = {d["value"]: d["freq"] for d in data}
        assert values["red"] == 2
        assert values["blue"] == 1

    def test_freq_multiple_files(self, tmp_path: Path):
        """freq should work with multiple files (union of lazy tables)."""
        # Create two separate CSV files with freq-eligible columns
        (tmp_path / "file1.csv").write_text("status\nactive\nactive\ninactive\n")
        (tmp_path / "file2.csv").write_text("status\npending\npending\nactive\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)
        catalog.export_db(tmp_path / "output")

        # Should not raise CatalogException about missing table
        assert (tmp_path / "output" / "freq.json").exists()

        with open(tmp_path / "output" / "freq.json") as f:
            data = json.load(f)

        # Should have freq data from both files
        assert len(data) > 0


class TestCatalogWrite:
    """Test Catalog.write method."""

    def test_write_empty_catalog(self, tmp_path):
        """export_db on empty catalog should not create any files."""
        catalog = Catalog()
        catalog.export_db(tmp_path)

        # No entity files should be created
        assert not (tmp_path / "folder.json").exists()
        assert not (tmp_path / "dataset.json").exists()
        assert not (tmp_path / "variable.json").exists()
        assert not (tmp_path / "__table__.json").exists()

    def test_write_creates_json_files(self, tmp_path):
        """write should create .json files for each entity type."""
        catalog = Catalog()
        catalog.add_folder(
            CSV_DIR, Folder(id="test", name="Test"), include=["employees.csv"]
        )
        catalog.export_db(tmp_path)

        assert (tmp_path / "variable.json").exists()
        assert (tmp_path / "dataset.json").exists()
        assert (tmp_path / "folder.json").exists()

    def test_write_creates_jsonjs_files(self, tmp_path):
        """write should create .json.js files by default."""
        catalog = Catalog()
        catalog.add_folder(
            CSV_DIR, Folder(id="test", name="Test"), include=["employees.csv"]
        )
        catalog.export_db(tmp_path)

        assert (tmp_path / "variable.json.js").exists()
        assert (tmp_path / "dataset.json.js").exists()
        assert (tmp_path / "folder.json.js").exists()

    def test_write_variable_json_content(self, tmp_path):
        """write should produce valid variable JSON."""
        catalog = Catalog()
        catalog.add_folder(
            CSV_DIR, Folder(id="test", name="Test"), include=["employees.csv"]
        )
        catalog.export_db(tmp_path)

        with open(tmp_path / "variable.json") as f:
            data = json.load(f)

        assert isinstance(data, list)
        assert len(data) == 9
        assert all("id" in item for item in data)

    def test_write_dataset_json_content(self, tmp_path):
        """write should produce valid dataset JSON."""
        catalog = Catalog()
        catalog.add_folder(
            CSV_DIR, Folder(id="test", name="Test"), include=["employees.csv"]
        )
        catalog.export_db(tmp_path)

        with open(tmp_path / "dataset.json") as f:
            data = json.load(f)

        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["id"] == "test---employees_csv"
        assert data[0]["folder_id"] == "test"

    def test_write_folder_json_content(self, tmp_path):
        """write should produce valid folder JSON."""
        catalog = Catalog()
        catalog.add_folder(
            CSV_DIR, Folder(id="test", name="Test"), include=["employees.csv"]
        )
        catalog.export_db(tmp_path)

        with open(tmp_path / "folder.json") as f:
            data = json.load(f)

        assert isinstance(data, list)
        # Filter out auto-generated _modalities folder
        user_folders = [f for f in data if f["id"] != "_modalities"]
        assert len(user_folders) == 1
        assert user_folders[0]["id"] == "test"
        assert user_folders[0]["name"] == "Test"

    def test_write_jsonjs_format(self, tmp_path):
        """write should produce correct jsonjs format."""
        catalog = Catalog()
        catalog.add_folder(
            CSV_DIR, Folder(id="test", name="Test"), include=["employees.csv"]
        )
        catalog.export_db(tmp_path)

        content = (tmp_path / "variable.json.js").read_text()

        # Should start with jsonjs.data assignment
        assert content.startswith("jsonjs.data['variable'] = ")

        # Extract JSON part and parse
        json_part = content.replace("jsonjs.data['variable'] = ", "")
        data = json.loads(json_part)

        # First element should be column names
        assert isinstance(data[0], list)
        assert "id" in data[0]
        assert "name" in data[0]

        # Remaining elements should be data rows
        assert len(data) == 10  # 1 header + 9 variables

    def test_write_skip_jsonjs(self, tmp_path):
        """write with write_js=False should skip .json.js."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["employees.csv"]
        )
        catalog.export_db(tmp_path, write_js=False)

        assert not (tmp_path / "variable.json.js").exists()

    def test_write_creates_output_dir(self, tmp_path):
        """write should create output directory if needed."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["employees.csv"]
        )
        nested_dir = tmp_path / "nested" / "path"

        catalog.export_db(nested_dir)

        assert (nested_dir / "variable.json").exists()

    def test_write_float_to_int(self, tmp_path):
        """write should convert whole floats to ints."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["employees.csv"]
        )
        catalog.export_db(tmp_path)

        with open(tmp_path / "variable.json") as f:
            data = json.load(f)

        # nb_distinct should be int, not float
        for item in data:
            if "nb_distinct" in item:
                assert isinstance(item["nb_distinct"], int)

    def test_write_creates_table_registry(self, tmp_path):
        """write should create __table__.json registry."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["employees.csv"]
        )
        catalog.export_db(tmp_path)

        assert (tmp_path / "__table__.json").exists()
        assert (tmp_path / "__table__.json.js").exists()

        with open(tmp_path / "__table__.json") as f:
            data = json.load(f)

        table_names = [t["name"] for t in data]
        assert "folder" in table_names
        assert "dataset" in table_names
        assert "variable" in table_names
        assert "__table__" in table_names
        assert all("last_modif" in t for t in data)

    def test_write_institutions(self, tmp_path: Path):
        """export_db should write institution.json when institutions exist."""
        from datannurpy.entities import Institution

        catalog = Catalog()
        catalog.institutions.append(Institution(id="inst1", name="Institution 1"))
        catalog.export_db(tmp_path)

        assert (tmp_path / "institution.json").exists()
        assert (tmp_path / "institution.json.js").exists()

        with open(tmp_path / "institution.json") as f:
            data = json.load(f)
        assert len(data) == 1
        assert data[0]["id"] == "inst1"

    def test_write_tags(self, tmp_path: Path):
        """export_db should write tag.json when tags exist."""
        from datannurpy.entities import Tag

        catalog = Catalog()
        catalog.tags.append(Tag(id="tag1", name="Tag 1"))
        catalog.export_db(tmp_path)

        assert (tmp_path / "tag.json").exists()
        assert (tmp_path / "tag.json.js").exists()

        with open(tmp_path / "tag.json") as f:
            data = json.load(f)
        assert len(data) == 1
        assert data[0]["id"] == "tag1"

    def test_write_docs(self, tmp_path: Path):
        """export_db should write doc.json when docs exist."""
        from datannurpy.entities import Doc

        catalog = Catalog()
        catalog.docs.append(Doc(id="doc1", name="Doc 1"))
        catalog.export_db(tmp_path)

        assert (tmp_path / "doc.json").exists()
        assert (tmp_path / "doc.json.js").exists()

        with open(tmp_path / "doc.json") as f:
            data = json.load(f)
        assert len(data) == 1
        assert data[0]["id"] == "doc1"
