"""Tests for Catalog class."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from datannurpy import Catalog, Folder


class TestCatalogRepr:
    """Test Catalog __repr__ method."""

    def test_repr(self):
        """Catalog repr should show counts."""
        catalog = Catalog()
        result = repr(catalog)
        assert "Catalog(" in result
        assert "folders=0" in result
        assert "datasets=0" in result
        assert "variables=0" in result
        assert "modalities=0" in result
        assert "values=0" in result
        assert "institutions=0" in result
        assert "tags=0" in result
        assert "docs=0" in result


class TestCatalogAppPath:
    """Test Catalog app_path parameter."""

    def test_app_path_none_by_default(self):
        """Catalog should have app_path=None by default."""
        catalog = Catalog()
        assert catalog.app_path is None
        assert catalog.db_path is None

    def test_app_path_loads_existing_catalog(self, tmp_path: Path):
        """Catalog with app_path should load existing entities from data/db/."""
        # Create a catalog structure (db is in app_path/data/db/)
        app_dir = tmp_path / "app"
        db_dir = app_dir / "data" / "db"
        db_dir.mkdir(parents=True)
        (db_dir / "__table__.json").write_text(
            json.dumps([{"name": "folder"}, {"name": "dataset"}])
        )
        (db_dir / "folder.json").write_text(
            json.dumps([{"id": "f1", "name": "Folder 1"}])
        )
        (db_dir / "dataset.json").write_text(
            json.dumps([{"id": "ds1", "name": "Dataset 1", "folder_id": "f1"}])
        )

        # Load with app_path
        catalog = Catalog(app_path=app_dir)

        assert len(catalog.folder.all()) == 1
        assert catalog.folder.all()[0].id == "f1"
        assert len(catalog.dataset.all()) == 1
        assert catalog.dataset.all()[0].id == "ds1"

    def test_app_path_nonexistent_creates_empty_catalog(self, tmp_path: Path):
        """Catalog with nonexistent app_path should create empty catalog."""
        catalog = Catalog(app_path=tmp_path / "nonexistent")

        assert len(catalog.folder.all()) == 0
        assert len(catalog.dataset.all()) == 0

    def test_app_path_stored_as_path(self, tmp_path: Path):
        """app_path should be stored as Path object."""
        catalog = Catalog(app_path=str(tmp_path))
        assert isinstance(catalog.app_path, Path)
        assert catalog.app_path == tmp_path

    def test_db_path_derived_from_app_path(self, tmp_path: Path):
        """db_path should be app_path/data/db."""
        catalog = Catalog(app_path=tmp_path)
        assert catalog.db_path == tmp_path / "data" / "db"

    def test_db_dir_exists_but_empty_creates_empty_catalog(self, tmp_path: Path):
        """Catalog should be empty if db_dir exists but has no __table__.json."""
        app_dir = tmp_path / "app"
        db_dir = app_dir / "data" / "db"
        db_dir.mkdir(parents=True)
        # Don't create __table__.json - just an empty directory

        catalog = Catalog(app_path=app_dir)

        assert len(catalog.folder.all()) == 0
        assert len(catalog.dataset.all()) == 0


class TestCatalogRefresh:
    """Test Catalog refresh parameter."""

    def test_refresh_false_by_default(self):
        """Catalog should have refresh=False by default."""
        catalog = Catalog()
        assert catalog.refresh is False

    def test_refresh_can_be_set(self):
        """Catalog refresh can be set to True."""
        catalog = Catalog(refresh=True)
        assert catalog.refresh is True


class TestCatalogExportDbDefault:
    """Test Catalog.export_db with app_path default."""

    def test_export_db_uses_db_path_by_default(self, tmp_path: Path):
        """export_db() without args should use db_path (derived from app_path)."""
        app_dir = tmp_path / "app"
        db_dir = app_dir / "data" / "db"

        catalog = Catalog(app_path=app_dir)
        catalog.folder.add(Folder(id="f1", name="Test", _seen=True))
        catalog.export_db()

        assert (db_dir / "folder.json").exists()
        with open(db_dir / "folder.json") as f:
            data = json.load(f)
        assert data[0]["id"] == "f1"

    def test_export_db_without_app_path_raises(self):
        """export_db() without args and no app_path should raise."""
        catalog = Catalog()
        catalog.folder.add(Folder(id="f1", name="Test", _seen=True))

        with pytest.raises(ValueError, match="output_dir is required"):
            catalog.export_db()

    def test_export_db_explicit_path_overrides_db_path(self, tmp_path: Path):
        """export_db(path) should override db_path."""
        app_dir = tmp_path / "app"
        db_dir = app_dir / "data" / "db"
        other_dir = tmp_path / "other"

        catalog = Catalog(app_path=app_dir)
        catalog.folder.add(Folder(id="f1", name="Test", _seen=True))
        catalog.export_db(other_dir)

        assert not (db_dir / "folder.json").exists()
        assert (other_dir / "folder.json").exists()

    def test_full_workflow_with_app_path(self, tmp_path: Path):
        """Full workflow: create, export, reload, modify, export."""
        app_dir = tmp_path / "app"
        db_dir = app_dir / "data" / "db"
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        # First run: scan and export
        catalog1 = Catalog(app_path=app_dir)
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog1.export_db()

        assert (db_dir / "folder.json").exists()
        assert (db_dir / "dataset.json").exists()

        # Second run: load existing catalog
        catalog2 = Catalog(app_path=app_dir)

        # Filter out _modalities folder
        user_folders = [f for f in catalog2.folder.all() if f.id != "_modalities"]
        assert len(user_folders) == 1
        assert user_folders[0].id == "src"
        assert len(catalog2.dataset.all()) == 1


class TestCatalogDepth:
    """Test Catalog depth parameter."""

    def test_depth_structure_clears_variable_tables_on_load(self, tmp_path: Path):
        """Loading with depth='structure' should clear variable/modality/value/freq."""
        app_dir = tmp_path / "app"
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n3,4\n")

        # First run: full scan (creates variables, modalities, etc.)
        catalog1 = Catalog(app_path=app_dir, depth="full")
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog1.export_db()

        # Verify we have data
        assert len(catalog1.variable.all()) > 0
        assert len(catalog1.modality.all()) > 0

        # Second run: load with structure mode
        catalog2 = Catalog(app_path=app_dir, depth="structure")

        # Structure mode should have cleared these tables
        assert len(catalog2.variable.all()) == 0
        assert len(catalog2.modality.all()) == 0
        assert len(catalog2.value.all()) == 0
        assert len(catalog2.freq.all()) == 0

        # But folders and datasets should still be loaded
        assert len(catalog2.folder.all()) > 0
        assert len(catalog2.dataset.all()) == 1

    def test_depth_structure_on_new_catalog(self):
        """depth='structure' on new catalog (no db) should work without error."""
        catalog = Catalog(depth="structure")

        # Should have empty tables
        assert len(catalog.variable.all()) == 0
        assert len(catalog.modality.all()) == 0
        assert len(catalog.value.all()) == 0
        assert len(catalog.freq.all()) == 0
