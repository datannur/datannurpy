"""Tests for Catalog class."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from datannurpy import Catalog, Folder
from datannurpy.errors import ConfigError


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


class TestCatalogSampleSize:
    """Test Catalog sample_size default and override."""

    def test_default_sample_size(self):
        """Catalog should have sample_size=100_000 by default."""
        catalog = Catalog()
        assert catalog.sample_size == 100_000

    def test_custom_sample_size(self):
        """Catalog should accept custom sample_size."""
        catalog = Catalog(sample_size=50_000)
        assert catalog.sample_size == 50_000

    def test_disable_sample_size(self):
        """Catalog with sample_size=None should disable sampling."""
        catalog = Catalog(sample_size=None)
        assert catalog.sample_size is None


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

    def test_refresh_skips_loading_existing_db(self, tmp_path: Path):
        """Catalog with refresh=True should not load existing db."""
        app_dir = tmp_path / "app"
        catalog1 = Catalog(app_path=app_dir)
        catalog1.folder.add(Folder(id="f1", name="Test", _seen=True))
        catalog1.export_db()

        catalog2 = Catalog(app_path=app_dir, refresh=True)
        assert len(catalog2.folder.all()) == 0

    def test_corrupted_db_falls_back_gracefully(self, tmp_path: Path):
        """Catalog should warn and start fresh if existing db is corrupted."""
        app_dir = tmp_path / "app"
        db_dir = app_dir / "data" / "db"
        db_dir.mkdir(parents=True)
        (db_dir / "__table__.json").write_text('[{"name":"variable","last_modif":0}]')
        (db_dir / "variable.json").write_text("NOT VALID JSON{{{{")

        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            catalog = Catalog(app_path=app_dir)
            assert len(w) == 1
            assert "Could not load" in str(w[0].message)
        assert len(catalog.variable.all()) == 0

    def test_init_error_without_db_reraised(self, monkeypatch: pytest.MonkeyPatch):
        """Catalog should reraise init errors not caused by loading existing db."""
        monkeypatch.setattr(
            "datannurpy.catalog.DatannurDB.__init__",
            lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        with pytest.raises(RuntimeError, match="boom"):
            Catalog()


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

        with pytest.raises(ConfigError, match="output_dir is required"):
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
        user_folders = catalog2.folder.where("id", "!=", "_modalities")
        assert len(user_folders) == 1
        assert user_folders[0].id == "src"
        assert len(catalog2.dataset.all()) == 1


class TestCatalogDepth:
    """Test Catalog depth parameter."""

    def test_depth_dataset_clears_variable_tables_on_load(self, tmp_path: Path):
        """Loading with depth='dataset' should clear variable/modality/value/freq."""
        app_dir = tmp_path / "app"
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        # Create CSV with repeated values to ensure freq and modalities are created
        (data_dir / "test.csv").write_text("a,b\n1,x\n2,x\n3,x\n")

        # First run: full scan with low freq_threshold to create freq entries
        catalog1 = Catalog(app_path=app_dir, depth="value", freq_threshold=2)
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog1.export_db()

        # Verify we have data in all tables
        assert len(catalog1.variable.all()) > 0
        assert len(catalog1.modality.all()) > 0
        assert len(catalog1.value.all()) > 0
        assert len(catalog1.freq.all()) > 0

        # Second run: load with dataset mode
        catalog2 = Catalog(app_path=app_dir, depth="dataset")

        # Structure mode should have cleared these tables
        assert len(catalog2.variable.all()) == 0
        assert len(catalog2.modality.all()) == 0
        assert len(catalog2.value.all()) == 0
        assert len(catalog2.freq.all()) == 0

        # But folders and datasets should still be loaded
        assert len(catalog2.folder.all()) > 0
        assert len(catalog2.dataset.all()) == 1

    def test_depth_dataset_on_new_catalog(self):
        """depth='dataset' on new catalog (no db) should work without error."""
        catalog = Catalog(depth="dataset")

        # Should have empty tables
        assert len(catalog.variable.all()) == 0
        assert len(catalog.modality.all()) == 0
        assert len(catalog.value.all()) == 0
        assert len(catalog.freq.all()) == 0


class TestAppConfig:
    """Test Catalog app_config parameter."""

    def test_default_app_config(self):
        """Without app_config, config table is empty."""
        catalog = Catalog()
        assert catalog.config.count == 0

    def test_app_config_partial(self):
        """Providing some keys populates only those."""
        catalog = Catalog(app_config={"contact_email": "a@b.com"})
        assert catalog.config.count == 1
        assert catalog.config.all()[0].id == "contact_email"
        assert catalog.config.all()[0].value == "a@b.com"

    def test_app_config_full(self):
        """Providing multiple keys fills them all."""
        cfg = {
            "contact_email": "a@b.com",
            "more_info": "https://example.com",
        }
        catalog = Catalog(app_config=cfg)
        by_id = {c.id: c.value for c in catalog.config.all()}
        assert by_id == cfg

    def test_app_config_extra_keys(self):
        """Any keys provided by user are stored."""
        catalog = Catalog(app_config={"banner": "Welcome", "body": "text"})
        by_id = {c.id: c.value for c in catalog.config.all()}
        assert by_id["banner"] == "Welcome"
        assert by_id["body"] == "text"

    def test_export_db_writes_config_json(self, tmp_path: Path):
        """export_db should write config.json alongside table files."""
        catalog = Catalog(app_config={"contact_email": "test@test.com"})
        catalog.export_db(tmp_path)

        config_path = tmp_path / "config.json"
        assert config_path.exists()
        data = json.loads(config_path.read_text())
        assert data == [{"id": "contact_email", "value": "test@test.com"}]

        # Also writes .json.js
        js_path = tmp_path / "config.json.js"
        assert js_path.exists()
        assert "jsonjs.data['config']" in js_path.read_text()

    def test_export_db_no_config_no_file(self, tmp_path: Path):
        """export_db without app_config writes no config.json."""
        catalog = Catalog()
        catalog.export_db(tmp_path)

        assert not (tmp_path / "config.json").exists()
