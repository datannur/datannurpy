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


class TestCatalogDbPath:
    """Test Catalog db_path parameter."""

    def test_db_path_none_by_default(self):
        """Catalog should have db_path=None by default."""
        catalog = Catalog()
        assert catalog.db_path is None

    def test_db_path_loads_existing_catalog(self, tmp_path: Path):
        """Catalog with db_path should load existing entities."""
        # Create a catalog and export it
        db_dir = tmp_path / "db"
        (db_dir).mkdir()
        (db_dir / "folder.json").write_text(
            json.dumps([{"id": "f1", "name": "Folder 1"}])
        )
        (db_dir / "dataset.json").write_text(
            json.dumps([{"id": "ds1", "name": "Dataset 1", "folder_id": "f1"}])
        )

        # Load with db_path
        catalog = Catalog(db_path=db_dir)

        assert len(catalog.folders) == 1
        assert catalog.folders[0].id == "f1"
        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].id == "ds1"

    def test_db_path_nonexistent_creates_empty_catalog(self, tmp_path: Path):
        """Catalog with nonexistent db_path should create empty catalog."""
        catalog = Catalog(db_path=tmp_path / "nonexistent")

        assert len(catalog.folders) == 0
        assert len(catalog.datasets) == 0

    def test_db_path_stored_as_path(self, tmp_path: Path):
        """db_path should be stored as Path object."""
        catalog = Catalog(db_path=str(tmp_path))
        assert isinstance(catalog.db_path, Path)
        assert catalog.db_path == tmp_path


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
    """Test Catalog.export_db with db_path default."""

    def test_export_db_uses_db_path_by_default(self, tmp_path: Path):
        """export_db() without args should use db_path."""
        db_dir = tmp_path / "db"

        catalog = Catalog(db_path=db_dir)
        catalog.folders.append(Folder(id="f1", name="Test", _seen=True))
        catalog.export_db()

        assert (db_dir / "folder.json").exists()
        with open(db_dir / "folder.json") as f:
            data = json.load(f)
        assert data[0]["id"] == "f1"

    def test_export_db_without_db_path_raises(self):
        """export_db() without args and no db_path should raise."""
        catalog = Catalog()
        catalog.folders.append(Folder(id="f1", name="Test", _seen=True))

        with pytest.raises(ValueError, match="output_dir is required"):
            catalog.export_db()

    def test_export_db_explicit_path_overrides_db_path(self, tmp_path: Path):
        """export_db(path) should override db_path."""
        db_dir = tmp_path / "db"
        other_dir = tmp_path / "other"

        catalog = Catalog(db_path=db_dir)
        catalog.folders.append(Folder(id="f1", name="Test", _seen=True))
        catalog.export_db(other_dir)

        assert not (db_dir / "folder.json").exists()
        assert (other_dir / "folder.json").exists()

    def test_full_workflow_with_db_path(self, tmp_path: Path):
        """Full workflow: create, export, reload, modify, export."""
        db_dir = tmp_path / "db"
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        # First run: scan and export
        catalog1 = Catalog(db_path=db_dir)
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog1.export_db()

        assert (db_dir / "folder.json").exists()
        assert (db_dir / "dataset.json").exists()

        # Second run: load existing catalog
        catalog2 = Catalog(db_path=db_dir)

        # Filter out _modalities folder
        user_folders = [f for f in catalog2.folders if f.id != "_modalities"]
        assert len(user_folders) == 1
        assert user_folders[0].id == "src"
        assert len(catalog2.datasets) == 1


class TestCatalogIndexIntegrity:
    """Test that indexes remain consistent with lists."""

    def test_indexes_consistent_after_add_folder(self, tmp_path: Path):
        """Indexes should match lists after add_folder."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        catalog = Catalog(quiet=True)
        catalog.add_folder(data_dir, Folder(id="src", name="Source"))

        # Verify folder index
        for folder in catalog.folders:
            assert folder.id in catalog._folder_index
            assert catalog._folder_index[folder.id] is folder

        # Verify dataset index
        for ds in catalog.datasets:
            if ds.data_path:
                assert ds.data_path in catalog._dataset_index
                assert catalog._dataset_index[ds.data_path] is ds

        # Verify variables index
        for var in catalog.variables:
            assert var.dataset_id in catalog._variables_by_dataset
            assert var in catalog._variables_by_dataset[var.dataset_id]

        # Verify modality index
        for mod in catalog.modalities:
            assert mod.id in catalog._modality_index
            assert catalog._modality_index[mod.id] is mod

    def test_indexes_consistent_after_incremental_scan(self, tmp_path: Path):
        """Indexes should match lists after incremental scan with deletions."""
        db_dir = tmp_path / "db"
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "keep.csv").write_text("a\n1\n")
        (data_dir / "remove.csv").write_text("b\n2\n")

        # First scan
        catalog1 = Catalog(db_path=db_dir, quiet=True)
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog1.export_db()

        # Remove file and rescan
        (data_dir / "remove.csv").unlink()
        catalog2 = Catalog(db_path=db_dir, quiet=True)
        catalog2.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog2.export_db()  # This calls finalize()

        # Verify all indexes are consistent
        assert len(catalog2._folder_index) == len(catalog2.folders)
        assert len(catalog2._dataset_index) == len(
            [ds for ds in catalog2.datasets if ds.data_path]
        )
        assert len(catalog2._modality_index) == len(catalog2.modalities)

        # Verify no stale entries
        for fid in catalog2._folder_index:
            assert any(f.id == fid for f in catalog2.folders)
        for dp in catalog2._dataset_index:
            assert any(ds.data_path == dp for ds in catalog2.datasets)
        for mid in catalog2._modality_index:
            assert any(m.id == mid for m in catalog2.modalities)
