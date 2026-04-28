"""Tests for modality auto-generation."""

import json
from pathlib import Path

from datannurpy import Catalog
from datannurpy.utils import build_modality_name, compute_modality_hash
from datannurpy.utils.ids import build_value_id


class TestModalityHash:
    """Test compute_modality_hash function."""

    def test_hash_deterministic(self):
        """Same values should produce same hash."""
        values = {"H", "F"}
        h1 = compute_modality_hash(values)
        h2 = compute_modality_hash(values)
        assert h1 == h2

    def test_hash_order_independent(self):
        """Order of values should not matter."""
        h1 = compute_modality_hash({"A", "B", "C"})
        h2 = compute_modality_hash({"C", "A", "B"})
        assert h1 == h2

    def test_hash_length(self):
        """Hash should be 10 characters."""
        h = compute_modality_hash({"x", "y"})
        assert len(h) == 10

    def test_hash_different_values(self):
        """Different values should produce different hashes."""
        h1 = compute_modality_hash({"A", "B"})
        h2 = compute_modality_hash({"A", "C"})
        assert h1 != h2

    def test_hash_separator_collision(self):
        """Values with separators should not collide."""
        # These could collide with a naive separator-based approach
        h1 = compute_modality_hash({"A|B", "C"})
        h2 = compute_modality_hash({"A", "B|C"})
        assert h1 != h2


class TestModalityName:
    """Test build_modality_name function."""

    def test_simple_values(self):
        """Simple short values."""
        name = build_modality_name({"H", "F"})
        assert name == "F, H"

    def test_sorted_alphabetically(self):
        """Values should be sorted alphabetically (case-insensitive)."""
        name = build_modality_name({"Zebra", "apple", "Banana"})
        assert name == "apple, Banana, Zebra"

    def test_max_three_values(self):
        """Only first 3 values shown, rest indicated."""
        name = build_modality_name({"A", "B", "C", "D", "E"})
        assert name == "A, B, C... (+2)"

    def test_truncate_long_values(self):
        """Long values should be truncated to 15 chars."""
        name = build_modality_name({"Very long value here"})
        assert "..." in name
        # Value should be truncated: "Very long va..."
        assert name == "Very long va..."

    def test_many_long_values(self):
        """Multiple long values with count."""
        name = build_modality_name(
            {
                "First long value",
                "Second long value",
                "Third long value",
                "Fourth long value",
            }
        )
        # Should show 3 truncated values + count
        assert "... (+1)" in name


class TestModalityGeneration:
    """Test modality auto-generation in Catalog."""

    def test_modality_created_from_frequency(self, tmp_path: Path):
        """Modality should be created for frequency-eligible columns."""
        (tmp_path / "data.csv").write_text("color\nred\nblue\nred\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        assert len(catalog.modality.all()) == 1
        assert catalog.modality.all()[0].folder_id == "_modalities"

    def test_modalities_folder_created(self, tmp_path: Path):
        """_modalities folder should be created when modalities exist."""
        (tmp_path / "data.csv").write_text("color\nred\nblue\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        folder_ids = [f.id for f in catalog.folder.all()]
        assert "_modalities" in folder_ids

    def test_modalities_folder_not_created_when_empty(self, tmp_path: Path):
        """_modalities folder should not be created if no modalities."""
        # Many distinct values = no frequency rows = no modality
        (tmp_path / "data.csv").write_text(
            "id\n" + "\n".join(str(i) for i in range(200))
        )

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        folder_ids = [f.id for f in catalog.folder.all()]
        assert "_modalities" not in folder_ids

    def test_modality_values_created(self, tmp_path: Path):
        """Values should be created for each modality value."""
        (tmp_path / "data.csv").write_text("color\nred\nblue\ngreen\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        assert len(catalog.value.all()) == 3
        values = {v.value for v in catalog.value.all()}
        assert values == {"red", "blue", "green"}

    def test_modality_linked_to_variable(self, tmp_path: Path):
        """Variable should have modality_ids set."""
        (tmp_path / "data.csv").write_text("color\nred\nblue\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        var = catalog.variable.all()[0]
        assert len(var.modality_ids) == 1
        assert var.modality_ids[0] == catalog.modality.all()[0].id

    def test_modality_reused_same_values(self, tmp_path: Path):
        """Same values in different files should reuse same modality."""
        (tmp_path / "file1.csv").write_text("gender\nM\nF\n")
        (tmp_path / "file2.csv").write_text("sex\nM\nF\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        # Should have only 1 modality (reused)
        assert len(catalog.modality.all()) == 1

        # Both variables should reference it
        var1, var2 = catalog.variable.all()
        assert var1.modality_ids == var2.modality_ids

    def test_modality_different_values(self, tmp_path: Path):
        """Different values should create different modalities."""
        (tmp_path / "file1.csv").write_text("status\nactive\ninactive\n")
        (tmp_path / "file2.csv").write_text("color\nred\nblue\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        # Should have 2 different modalities
        assert len(catalog.modality.all()) == 2

    def test_modality_stable_id(self, tmp_path: Path):
        """Same values should produce same modality ID across runs."""
        (tmp_path / "data.csv").write_text("color\nred\nblue\n")

        catalog1 = Catalog()
        catalog1.add_folder(tmp_path)
        id1 = catalog1.modality.all()[0].id

        catalog2 = Catalog()
        catalog2.add_folder(tmp_path)
        id2 = catalog2.modality.all()[0].id

        assert id1 == id2

    def test_hash_based_ids_no_collision(self):
        """Values with similar sanitized forms get distinct hash-based IDs."""
        catalog = Catalog(quiet=False)
        # ".idle" and "_idle" would collide with sanitize_id but not with hashing
        modality_id = catalog.modality_manager.get_or_create({".idle", "_idle"})
        assert modality_id is not None
        # Both values stored (hash-based IDs are unique)
        assert len(catalog.value.all()) == 2


class TestModalityExport:
    """Test modality JSON export."""

    def test_modality_json_exported(self, tmp_path: Path):
        """modality.json should be written."""
        (tmp_path / "data.csv").write_text("color\nred\nblue\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)
        catalog.export_db(tmp_path / "output")

        assert (tmp_path / "output" / "modality.json").exists()
        assert (tmp_path / "output" / "modality.json.js").exists()

    def test_value_json_exported(self, tmp_path: Path):
        """value.json should be written."""
        (tmp_path / "data.csv").write_text("color\nred\nblue\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)
        catalog.export_db(tmp_path / "output")

        assert (tmp_path / "output" / "value.json").exists()
        assert (tmp_path / "output" / "value.json.js").exists()

    def test_table_registry_includes_modality(self, tmp_path: Path):
        """__table__.json should include modality and value."""
        (tmp_path / "data.csv").write_text("color\nred\nblue\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)
        catalog.export_db(tmp_path / "output")

        with open(tmp_path / "output" / "__table__.json") as f:
            registry = json.load(f)

        names = [t["name"] for t in registry]
        assert "modality" in names
        assert "value" in names

    def test_value_json_content(self, tmp_path: Path):
        """value.json should have correct structure."""
        (tmp_path / "data.csv").write_text("color\nred\nblue\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)
        catalog.export_db(tmp_path / "output")

        with open(tmp_path / "output" / "value.json") as f:
            data = json.load(f)

        assert len(data) == 2
        assert all("modality_id" in v for v in data)
        assert all("value" in v for v in data)
        # description should be null when not set
        assert all(v.get("description") is None for v in data)

    def test_variable_json_has_modality_ids(self, tmp_path: Path):
        """variable.json should include modality_ids."""
        (tmp_path / "data.csv").write_text("color\nred\nblue\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)
        catalog.export_db(tmp_path / "output")

        with open(tmp_path / "output" / "variable.json") as f:
            variables = json.load(f)

        # modality_ids should be comma-separated string
        assert "modality_ids" in variables[0]
        assert variables[0]["modality_ids"].startswith("_modalities---mod_")


class TestModalityIncremental:
    """Test modality handling with incremental scan."""

    def test_modality_index_rebuilt_on_load(self, tmp_path: Path):
        """Modality index should be rebuilt when loading from db_path."""
        app_dir = tmp_path
        (tmp_path / "data.csv").write_text("color\nred\nblue\n")

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_folder(tmp_path, include=["data.csv"])
        catalog1.export_db()

        initial_modalities = len(catalog1.modality.all())
        assert initial_modalities == 1

        # Second scan - should reuse existing modality
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_folder(tmp_path, include=["data.csv"])

        # Should not create duplicates
        assert len(catalog2.modality.all()) == initial_modalities

    def test_existing_modality_marked_seen(self, tmp_path: Path):
        """Existing modality should be marked as _seen when reused."""
        app_dir = tmp_path
        (tmp_path / "data.csv").write_text("color\nred\nblue\n")

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_folder(tmp_path, include=["data.csv"])
        catalog1.export_db()

        # Reload and rescan
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_folder(tmp_path, include=["data.csv"])
        catalog2.finalize()

        # Modality should be kept (marked as seen)
        assert len(catalog2.modality.all()) == 1

    def test_rebuild_index_with_none_value(self, tmp_path: Path):
        """rebuild_index should handle None values in Value objects."""
        from datannurpy.schema import Value

        app_dir = tmp_path
        (tmp_path / "data.csv").write_text("color\nred\nblue\n")

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_folder(tmp_path, include=["data.csv"])

        # Manually add a value with None
        catalog1.value.add(
            Value(
                id=build_value_id("test_mod", None), modality_id="test_mod", value=None
            )
        )
        catalog1.export_db()

        # Reload - rebuild_index should not crash
        catalog2 = Catalog(app_path=app_dir, quiet=True)

        # Should have loaded successfully
        assert len(catalog2.modality.all()) >= 1

    def test_get_or_create_modality_not_found_in_list(self):
        """get_or_create should handle case where modality is in index but not in list."""
        catalog = Catalog(quiet=True)

        # Manually set up a broken state (index has id but modalities list doesn't)
        catalog.modality_manager._modality_index[frozenset({"a", "b"})] = "missing_id"

        # get_or_create should still work (won't find modality to mark, but returns id)
        result = catalog.modality_manager.get_or_create({"a", "b"})
        assert result == "missing_id"

    def test_get_or_create_existing_modality_marked_seen(self):
        """get_or_create should mark existing modality as _seen."""
        catalog = Catalog(quiet=True)

        # First call creates the modality
        mod_id = catalog.modality_manager.get_or_create({"x", "y"})
        # Second call should find it in the index and mark _seen
        result = catalog.modality_manager.get_or_create({"x", "y"})
        assert result == mod_id


class TestStoreFrequencyTable:
    """Test frequency table storage."""

    def test_empty_frequency_table(self):
        """Empty frequency table should not add any frequencies."""
        import pyarrow as pa

        catalog = Catalog(quiet=True)
        empty_table = pa.table(
            {
                "variable_id": pa.array([], type=pa.string()),
                "value": pa.array([], type=pa.string()),
                "frequency": pa.array([], type=pa.int64()),
            }
        )
        catalog.modality_manager.store_freq_table(empty_table, {})
        assert len(catalog.frequency.all()) == 0

    def test_frequency_hash_based_ids_no_collision(self):
        """Frequency values with similar sanitized forms get distinct hash-based IDs."""
        import pyarrow as pa

        catalog = Catalog(quiet=False)
        # ".idle" and "_idle" would collide with sanitize_id but not with hashing
        table = pa.table(
            {
                "variable_id": ["var1", "var1"],
                "value": [".idle", "_idle"],
                "frequency": [5, 3],
            }
        )
        catalog.modality_manager.store_freq_table(table, {"var1": "v1"})
        assert len(catalog.frequency.all()) == 2
