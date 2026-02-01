"""Tests for importer/db.py - load_db function."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from datannurpy import Catalog, Folder
from datannurpy.entities import (
    Dataset,
    Doc,
    Institution,
    Modality,
    Tag,
    Value,
    Variable,
)
from datannurpy.importer.db import load_db


DATA_DIR = Path(__file__).parent.parent / "data"
CSV_DIR = DATA_DIR / "csv"


class TestLoadDbEmpty:
    """Test load_db with empty/missing paths."""

    def test_load_nonexistent_path(self, tmp_path: Path):
        """load_db with nonexistent path should leave catalog empty."""
        catalog = Catalog()
        load_db(tmp_path / "nonexistent", catalog)

        assert len(catalog.folders) == 0
        assert len(catalog.datasets) == 0
        assert len(catalog.variables) == 0

    def test_load_empty_directory(self, tmp_path: Path):
        """load_db with empty directory should leave catalog empty."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        catalog = Catalog()
        load_db(empty_dir, catalog)

        assert len(catalog.folders) == 0
        assert len(catalog.datasets) == 0

    def test_load_file_raises_error(self, tmp_path: Path):
        """load_db with file path should raise ValueError."""
        file_path = tmp_path / "file.json"
        file_path.write_text("[]")

        catalog = Catalog()
        with pytest.raises(ValueError, match="must be a directory"):
            load_db(file_path, catalog)


class TestLoadDbEntities:
    """Test load_db loading various entity types."""

    def test_load_folders(self, tmp_path: Path):
        """load_db should load folders from folder.json."""
        (tmp_path / "folder.json").write_text(
            json.dumps([{"id": "f1", "name": "Folder 1", "type": "filesystem"}])
        )

        catalog = Catalog()
        load_db(tmp_path, catalog)

        assert len(catalog.folders) == 1
        assert catalog.folders[0].id == "f1"
        assert catalog.folders[0].name == "Folder 1"
        assert catalog.folders[0].type == "filesystem"

    def test_load_datasets(self, tmp_path: Path):
        """load_db should load datasets from dataset.json."""
        (tmp_path / "dataset.json").write_text(
            json.dumps(
                [
                    {
                        "id": "ds1",
                        "name": "Dataset 1",
                        "folder_id": "f1",
                        "nb_row": 100,
                        "last_update_timestamp": 1706745600,
                        "schema_signature": "abc123",
                    }
                ]
            )
        )

        catalog = Catalog()
        load_db(tmp_path, catalog)

        assert len(catalog.datasets) == 1
        ds = catalog.datasets[0]
        assert ds.id == "ds1"
        assert ds.name == "Dataset 1"
        assert ds.folder_id == "f1"
        assert ds.nb_row == 100
        assert ds.last_update_timestamp == 1706745600
        assert ds.schema_signature == "abc123"

    def test_load_variables(self, tmp_path: Path):
        """load_db should load variables from variable.json."""
        (tmp_path / "variable.json").write_text(
            json.dumps(
                [
                    {
                        "id": "v1",
                        "name": "amount",
                        "dataset_id": "ds1",
                        "type": "int64",
                        "nb_distinct": 50,
                    }
                ]
            )
        )

        catalog = Catalog()
        load_db(tmp_path, catalog)

        assert len(catalog.variables) == 1
        var = catalog.variables[0]
        assert var.id == "v1"
        assert var.name == "amount"
        assert var.dataset_id == "ds1"
        assert var.type == "int64"
        assert var.nb_distinct == 50

    def test_load_modalities(self, tmp_path: Path):
        """load_db should load modalities from modality.json."""
        (tmp_path / "modality.json").write_text(
            json.dumps([{"id": "m1", "name": "Status", "folder_id": "_modalities"}])
        )

        catalog = Catalog()
        load_db(tmp_path, catalog)

        assert len(catalog.modalities) == 1
        assert catalog.modalities[0].id == "m1"
        assert catalog.modalities[0].name == "Status"

    def test_load_values(self, tmp_path: Path):
        """load_db should load values from value.json."""
        (tmp_path / "value.json").write_text(
            json.dumps(
                [
                    {
                        "modality_id": "m1",
                        "value": "active",
                        "description": "Active status",
                    },
                    {"modality_id": "m1", "value": "inactive"},
                ]
            )
        )

        catalog = Catalog()
        load_db(tmp_path, catalog)

        assert len(catalog.values) == 2
        assert catalog.values[0].modality_id == "m1"
        assert catalog.values[0].value == "active"
        assert catalog.values[0].description == "Active status"
        assert catalog.values[1].value == "inactive"
        assert catalog.values[1].description is None

    def test_load_institutions(self, tmp_path: Path):
        """load_db should load institutions from institution.json."""
        (tmp_path / "institution.json").write_text(
            json.dumps([{"id": "inst1", "name": "Org 1", "email": "org@test.com"}])
        )

        catalog = Catalog()
        load_db(tmp_path, catalog)

        assert len(catalog.institutions) == 1
        assert catalog.institutions[0].id == "inst1"
        assert catalog.institutions[0].name == "Org 1"
        assert catalog.institutions[0].email == "org@test.com"

    def test_load_tags(self, tmp_path: Path):
        """load_db should load tags from tag.json."""
        (tmp_path / "tag.json").write_text(
            json.dumps([{"id": "tag1", "name": "Important"}])
        )

        catalog = Catalog()
        load_db(tmp_path, catalog)

        assert len(catalog.tags) == 1
        assert catalog.tags[0].id == "tag1"
        assert catalog.tags[0].name == "Important"

    def test_load_docs(self, tmp_path: Path):
        """load_db should load docs from doc.json."""
        (tmp_path / "doc.json").write_text(
            json.dumps([{"id": "doc1", "name": "README", "path": "/docs/readme.md"}])
        )

        catalog = Catalog()
        load_db(tmp_path, catalog)

        assert len(catalog.docs) == 1
        assert catalog.docs[0].id == "doc1"
        assert catalog.docs[0].name == "README"
        assert catalog.docs[0].path == "/docs/readme.md"

    def test_load_freq_table(self, tmp_path: Path):
        """load_db should load freq.json into _freq_tables."""
        (tmp_path / "freq.json").write_text(
            json.dumps(
                [
                    {"variable_id": "v1", "value": "red", "freq": 10},
                    {"variable_id": "v1", "value": "blue", "freq": 5},
                ]
            )
        )

        catalog = Catalog()
        load_db(tmp_path, catalog)

        assert len(catalog._freq_tables) == 1
        table = catalog._freq_tables[0]
        assert table.num_rows == 2
        assert table.column_names == ["variable_id", "value", "freq"]

    def test_load_freq_table_empty_array(self, tmp_path: Path):
        """load_db should not add freq table when freq.json is empty array."""
        (tmp_path / "freq.json").write_text(json.dumps([]))

        catalog = Catalog()
        load_db(tmp_path, catalog)

        assert len(catalog._freq_tables) == 0


class TestLoadDbListFields:
    """Test load_db handling of comma-separated list fields."""

    def test_load_dataset_with_tag_ids(self, tmp_path: Path):
        """load_db should parse comma-separated tag_ids into list."""
        (tmp_path / "dataset.json").write_text(
            json.dumps([{"id": "ds1", "name": "Test", "tag_ids": "tag1,tag2,tag3"}])
        )

        catalog = Catalog()
        load_db(tmp_path, catalog)

        assert catalog.datasets[0].tag_ids == ["tag1", "tag2", "tag3"]

    def test_load_variable_with_modality_ids(self, tmp_path: Path):
        """load_db should parse comma-separated modality_ids into list."""
        (tmp_path / "variable.json").write_text(
            json.dumps(
                [
                    {
                        "id": "v1",
                        "name": "status",
                        "dataset_id": "ds1",
                        "modality_ids": "m1,m2",
                    }
                ]
            )
        )

        catalog = Catalog()
        load_db(tmp_path, catalog)

        assert catalog.variables[0].modality_ids == ["m1", "m2"]


class TestRoundTrip:
    """Test export → import → export produces identical results."""

    def test_roundtrip_simple_catalog(self, tmp_path: Path):
        """Export → import → export should produce identical JSON."""
        # Create a catalog with various entities
        catalog1 = Catalog()
        catalog1.folders.append(Folder(id="f1", name="Test Folder"))
        catalog1.datasets.append(
            Dataset(
                id="ds1",
                name="Test Dataset",
                folder_id="f1",
                nb_row=100,
                last_update_timestamp=1706745600,
                schema_signature="abc123",
                tag_ids=["tag1", "tag2"],
            )
        )
        catalog1.variables.append(
            Variable(
                id="v1",
                name="amount",
                dataset_id="ds1",
                type="int64",
                nb_distinct=50,
            )
        )
        catalog1.modalities.append(Modality(id="m1", name="Status"))
        catalog1.values.append(Value(modality_id="m1", value="active"))
        catalog1.institutions.append(Institution(id="inst1", name="Org"))
        catalog1.tags.append(Tag(id="tag1", name="Important"))
        catalog1.docs.append(Doc(id="doc1", name="README"))

        # Export first catalog
        export1_dir = tmp_path / "export1"
        catalog1.export_db(export1_dir)

        # Load into new catalog
        catalog2 = Catalog()
        load_db(export1_dir, catalog2)

        # Export second catalog
        export2_dir = tmp_path / "export2"
        catalog2.export_db(export2_dir)

        # Compare JSON files
        for filename in [
            "folder.json",
            "dataset.json",
            "variable.json",
            "modality.json",
            "value.json",
            "institution.json",
            "tag.json",
            "doc.json",
        ]:
            path1 = export1_dir / filename
            path2 = export2_dir / filename

            if path1.exists():
                assert path2.exists(), f"{filename} missing in second export"
                with open(path1) as f1, open(path2) as f2:
                    data1 = json.load(f1)
                    data2 = json.load(f2)
                assert data1 == data2, f"{filename} differs between exports"

    def test_roundtrip_with_freq(self, tmp_path: Path):
        """Export → import → export should preserve freq data."""
        # Create test data
        (tmp_path / "data.csv").write_text("color\nred\nred\nblue\n")

        # Scan and export
        catalog1 = Catalog()
        catalog1.add_folder(tmp_path, include=["data.csv"])

        export1_dir = tmp_path / "export1"
        catalog1.export_db(export1_dir)

        # Load and re-export
        catalog2 = Catalog()
        load_db(export1_dir, catalog2)

        export2_dir = tmp_path / "export2"
        catalog2.export_db(export2_dir)

        # Compare freq.json
        with open(export1_dir / "freq.json") as f1:
            freq1 = json.load(f1)
        with open(export2_dir / "freq.json") as f2:
            freq2 = json.load(f2)

        # Sort for comparison (order may differ)
        freq1_sorted = sorted(freq1, key=lambda x: (x["variable_id"], x["value"]))
        freq2_sorted = sorted(freq2, key=lambda x: (x["variable_id"], x["value"]))
        assert freq1_sorted == freq2_sorted

    def test_roundtrip_real_files(self, tmp_path: Path):
        """Round-trip with real scanned files should preserve data."""
        catalog1 = Catalog()
        catalog1.add_folder(
            CSV_DIR, Folder(id="csv", name="CSV"), include=["employees.csv"]
        )

        export1_dir = tmp_path / "export1"
        catalog1.export_db(export1_dir)

        # Load and re-export
        catalog2 = Catalog()
        load_db(export1_dir, catalog2)

        export2_dir = tmp_path / "export2"
        catalog2.export_db(export2_dir)

        # Compare all JSON files
        for json_file in export1_dir.glob("*.json"):
            if json_file.name == "__table__.json":
                continue  # Skip registry (timestamps differ)

            with open(json_file) as f1:
                data1 = json.load(f1)
            with open(export2_dir / json_file.name) as f2:
                data2 = json.load(f2)

            if json_file.name == "freq.json":
                # Sort freq for comparison (handle None values)
                data1 = sorted(
                    data1, key=lambda x: (x["variable_id"], x["value"] or "")
                )
                data2 = sorted(
                    data2, key=lambda x: (x["variable_id"], x["value"] or "")
                )

            assert data1 == data2, f"{json_file.name} differs"


class TestLoadDbUnknownFields:
    """Test load_db handling of unknown fields."""

    def test_unknown_fields_ignored(self, tmp_path: Path):
        """load_db should ignore unknown fields in JSON."""
        (tmp_path / "dataset.json").write_text(
            json.dumps(
                [
                    {
                        "id": "ds1",
                        "name": "Test",
                        "unknown_field": "should be ignored",
                        "another_unknown": 123,
                    }
                ]
            )
        )

        catalog = Catalog()
        load_db(tmp_path, catalog)

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].id == "ds1"
        assert catalog.datasets[0].name == "Test"
        assert not hasattr(catalog.datasets[0], "unknown_field")
