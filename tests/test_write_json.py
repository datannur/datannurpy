"""Tests for Catalog.write method."""

import json
from pathlib import Path

from datannurpy import Catalog, Folder

DATA_DIR = Path(__file__).parent.parent / "data"
CSV_DIR = DATA_DIR / "csv"


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
