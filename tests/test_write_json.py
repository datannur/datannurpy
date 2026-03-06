"""Tests for Catalog.export_db method."""

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

    def test_write_empty_catalog(self, tmp_path):
        """export_db on empty catalog should only create __table__.json."""
        catalog = Catalog()
        catalog.export_db(tmp_path)

        # No entity files should be created (only __table__.json registry)
        assert not (tmp_path / "folder.json").exists()
        assert not (tmp_path / "dataset.json").exists()
        assert not (tmp_path / "variable.json").exists()
        # jsonjsdb always creates __table__.json as table registry
        assert (tmp_path / "__table__.json").exists()

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

        with open(tmp_path / "__table__.json") as f:
            data = json.load(f)

        table_names = [t["name"] for t in data]
        assert "folder" in table_names
        assert "dataset" in table_names
        assert "variable" in table_names
        assert all("last_modif" in t for t in data)

    def test_write_institutions(self, tmp_path: Path):
        """export_db should write institution.json when institutions exist."""
        from datannurpy.schema import Institution

        catalog = Catalog()
        catalog.institution.add(Institution(id="inst1", name="Institution 1"))
        catalog.export_db(tmp_path)

        assert (tmp_path / "institution.json").exists()
        assert (tmp_path / "institution.json.js").exists()

        with open(tmp_path / "institution.json") as f:
            data = json.load(f)
        assert len(data) == 1
        assert data[0]["id"] == "inst1"

    def test_write_tags(self, tmp_path: Path):
        """export_db should write tag.json when tags exist."""
        from datannurpy.schema import Tag

        catalog = Catalog()
        catalog.tag.add(Tag(id="tag1", name="Tag 1"))
        catalog.export_db(tmp_path)

        assert (tmp_path / "tag.json").exists()
        assert (tmp_path / "tag.json.js").exists()

        with open(tmp_path / "tag.json") as f:
            data = json.load(f)
        assert len(data) == 1
        assert data[0]["id"] == "tag1"

    def test_write_docs(self, tmp_path: Path):
        """export_db should write doc.json when docs exist."""
        from datannurpy.schema import Doc

        catalog = Catalog()
        catalog.doc.add(Doc(id="doc1", name="Doc 1"))
        catalog.export_db(tmp_path)

        assert (tmp_path / "doc.json").exists()
        assert (tmp_path / "doc.json.js").exists()

        with open(tmp_path / "doc.json") as f:
            data = json.load(f)
        assert len(data) == 1
        assert data[0]["id"] == "doc1"


class TestDatasetIncrementalFields:
    """Test Dataset incremental scan fields export."""

    def test_dataset_last_update_timestamp_exported(self, tmp_path: Path):
        """last_update_timestamp should be exported to JSON when set."""
        from datannurpy.schema import Dataset

        catalog = Catalog()
        ds = Dataset(
            id="test",
            name="Test",
            last_update_timestamp=1706745600,  # 2024-02-01 00:00:00 UTC
        )
        catalog.dataset.add(ds)
        catalog.export_db(tmp_path)

        with open(tmp_path / "dataset.json") as f:
            data = json.load(f)
        assert data[0]["last_update_timestamp"] == 1706745600

    def test_dataset_schema_signature_exported(self, tmp_path: Path):
        """schema_signature should be exported to JSON when set."""
        from datannurpy.schema import Dataset

        catalog = Catalog()
        ds = Dataset(
            id="test",
            name="Test",
            schema_signature="abc123hash",
        )
        catalog.dataset.add(ds)
        catalog.export_db(tmp_path)

        with open(tmp_path / "dataset.json") as f:
            data = json.load(f)
        assert data[0]["schema_signature"] == "abc123hash"

    def test_dataset_incremental_fields_not_exported_when_none(self, tmp_path: Path):
        """Incremental fields should not appear in JSON when None."""
        from datannurpy.schema import Dataset

        catalog = Catalog()
        ds = Dataset(id="test", name="Test")
        catalog.dataset.add(ds)
        catalog.export_db(tmp_path)

        with open(tmp_path / "dataset.json") as f:
            data = json.load(f)
        # Fields should be null when not set (jsonjsdb includes all fields)
        assert data[0]["last_update_timestamp"] is None
        assert data[0]["schema_signature"] is None


class TestSerializationEdgeCases:
    """Test edge cases in catalog export serialization."""

    def test_export_empty_freq_tables(self, tmp_path: Path):
        """export_db with empty freq table should not create freq.json."""
        catalog = Catalog()
        # No freq entries added
        catalog.export_db(tmp_path)
        assert not (tmp_path / "freq.json").exists()


class TestEvolutionTracking:
    """Test evolution tracking in export_db."""

    def test_track_evolution_disabled(self, tmp_path: Path):
        """export_db(track_evolution=False) should not create evolution.json."""
        catalog = Catalog()
        catalog.folder.add(Folder(id="test", name="Test"))
        catalog.export_db(tmp_path, track_evolution=False)

        assert not (tmp_path / "evolution.json").exists()
        assert (tmp_path / "folder.json").exists()

    def test_track_evolution_no_changes(self, tmp_path: Path):
        """export_db should not create evolution.json when no changes detected."""
        app_dir = tmp_path / "app"
        db_dir = app_dir / "data" / "db"

        # First export - no evolution.json should be created (initial state)
        catalog1 = Catalog(app_path=app_dir)
        catalog1.folder.add(Folder(id="test", name="Test"))
        catalog1.export_db()
        assert not (db_dir / "evolution.json").exists()

        # Load the same data and export again (no changes)
        catalog2 = Catalog(app_path=app_dir)
        catalog2.export_db()

        # No changes = no evolution.json created
        assert not (db_dir / "evolution.json").exists()

    def test_track_evolution_with_changes(self, tmp_path: Path):
        """export_db should create evolution.json when changes are detected."""
        app_dir = tmp_path / "app"
        db_dir = app_dir / "data" / "db"

        # First export
        catalog1 = Catalog(app_path=app_dir)
        catalog1.folder.add(Folder(id="test", name="Original"))
        catalog1.export_db()
        assert not (db_dir / "evolution.json").exists()

        # Load, modify, and export again
        catalog2 = Catalog(app_path=app_dir)
        catalog2.folder.update("test", name="Modified")
        catalog2.export_db()

        # Modification should create evolution.json
        assert (db_dir / "evolution.json").exists()
        import json

        with open(db_dir / "evolution.json") as f:
            evolution = json.load(f)
        assert len(evolution) == 1
        assert evolution[0]["type"] == "update"
        assert evolution[0]["entity"] == "folder"
        assert evolution[0]["entity_id"] == "test"
        assert evolution[0]["variable"] == "name"
        assert evolution[0]["old_value"] == "Original"
        assert evolution[0]["new_value"] == "Modified"
