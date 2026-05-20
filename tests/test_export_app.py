"""Tests for Catalog.export_app method."""

import json
from pathlib import Path

import pytest

from datannurpy import Catalog, EntityMetadata
from datannurpy.errors import ConfigError
from datannurpy import exporter

DATA_DIR = Path(__file__).parent.parent / "data"


@pytest.fixture(scope="module")
def _employees_catalog() -> Catalog:
    """Scan employees.csv once, reuse across export_app tests."""
    catalog = Catalog()
    catalog.add_folder(
        DATA_DIR,
        metadata=EntityMetadata(id="test", name="Test"),
        include=["**/employees.csv"],
    )
    return catalog


class TestExportApp:
    """Test Catalog.export_app method."""

    def test_export_app_copies_index_html(self, _employees_catalog, tmp_path):
        """export_app should copy app files including index.html."""
        _employees_catalog.export_app(tmp_path)

        assert (tmp_path / "index.html").exists()
        assert (tmp_path / "assets").is_dir()

    def test_export_app_writes_to_data_db(self, _employees_catalog, tmp_path):
        """export_app should write data to data/db/ subdirectory."""
        _employees_catalog.export_app(tmp_path)

        db_dir = tmp_path / "data" / "db"
        assert (db_dir / "folder.json").exists()
        assert (db_dir / "dataset.json").exists()
        assert (db_dir / "variable.json").exists()
        assert (db_dir / "__table__.json").exists()

    def test_export_app_clears_existing_db(self, _employees_catalog, tmp_path):
        """export_app should clear existing data/db/ content."""
        # First export
        _employees_catalog.export_app(tmp_path)

        # Create extra file
        extra_file = tmp_path / "data" / "db" / "old_data.json"
        extra_file.write_text("[]")

        # Second export should remove it
        _employees_catalog.export_app(tmp_path)

        assert not extra_file.exists()

    def test_clean_stale_db_files_handles_missing_path(self):
        """Stale DB cleanup should ignore missing directories."""
        catalog = Catalog(quiet=True)

        exporter._clean_stale_db_files(catalog, Path("/nonexistent/datannur/db"))

    def test_clean_stale_db_files_keeps_unrelated_non_json_files(self, tmp_path):
        """Stale DB cleanup should only remove generated JSON database files."""
        catalog = Catalog(quiet=True)
        note = tmp_path / "note.txt"
        note.write_text("local")

        exporter._clean_stale_db_files(catalog, tmp_path)

        assert note.exists()

    def test_clean_stale_db_files_keeps_directories(self, tmp_path):
        """Stale DB cleanup should ignore directories."""
        catalog = Catalog(quiet=True)
        preview_dir = tmp_path / "preview"
        preview_dir.mkdir()

        exporter._clean_stale_db_files(catalog, tmp_path)

        assert preview_dir.exists()

    def test_export_app_preserves_data_except_db(self, _employees_catalog, tmp_path):
        """Repeated export_app should preserve local data files outside data/db."""
        _employees_catalog.export_app(tmp_path)

        ui_dir = tmp_path / "data" / "db-ui"
        ui_dir.mkdir()
        ui_file = ui_dir / "dataset.json"
        ui_file.write_text('[{"id":"manual","description":"Manual"}]')
        config_file = tmp_path / "data" / "localhost-ports.config.json"
        config_file.write_text('{"editServerPort": 8765}')
        old_db_file = tmp_path / "data" / "db" / "old_data.json"
        old_db_file.write_text("[]")

        _employees_catalog.export_app(tmp_path)

        assert ui_file.exists()
        assert config_file.exists()
        assert not old_db_file.exists()

    def test_export_app_does_not_refresh_existing_app_by_default(
        self, _employees_catalog, tmp_path
    ):
        """Existing app assets are not refreshed unless update_app is true."""
        _employees_catalog.export_app(tmp_path)

        marker = tmp_path / "assets" / "local-only.txt"
        marker.write_text("local")

        _employees_catalog.export_app(tmp_path)

        assert marker.exists()

    def test_export_app_update_app_refreshes_existing_app(
        self, _employees_catalog, tmp_path
    ):
        """update_app=True refreshes bundled app assets while preserving data."""
        _employees_catalog.export_app(tmp_path)

        marker = tmp_path / "assets" / "local-only.txt"
        marker.write_text("local")
        data_file = tmp_path / "data" / "db-ui" / "dataset.json"
        data_file.parent.mkdir(parents=True)
        data_file.write_text("[]")

        _employees_catalog.export_app(tmp_path, update_app=True)

        assert not marker.exists()
        assert data_file.exists()

    def test_export_app_preserves_evolution_tracking(self, tmp_path):
        """export_app should track evolution while regenerating data/db."""
        app_dir = tmp_path / "app"

        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_folder(
            DATA_DIR,
            metadata=EntityMetadata(id="test", name="Original"),
            include=["**/employees.csv"],
        )
        catalog1.export_app()

        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_folder(
            DATA_DIR,
            metadata=EntityMetadata(id="test", name="Modified"),
            include=["**/employees.csv"],
        )
        catalog2.export_app()

        evolution_path = app_dir / "data" / "db" / "evolution.json"
        assert evolution_path.exists()
        evolution = json.loads(evolution_path.read_text())
        assert any(
            item["type"] == "update"
            and item["entity"] == "folder"
            and item["entity_id"] == "test"
            and item["variable"] == "name"
            and item["old_value"] == "Original"
            and item["new_value"] == "Modified"
            for item in evolution
        )

    def test_export_app_quiet(self, _employees_catalog, tmp_path):
        """export_app with quiet=True should not print."""
        _employees_catalog.export_app(tmp_path, quiet=True)

        assert (tmp_path / "index.html").exists()

    def test_export_app_prints_summary_when_not_quiet(
        self, _employees_catalog, tmp_path, capsys: pytest.CaptureFixture[str]
    ):
        """export_app prints its arrow-prefixed summary when quiet is false."""
        _employees_catalog.export_app(tmp_path, quiet=False)

        err = capsys.readouterr().err
        assert "\n  →  exported in " in err
        assert "index.html" in err

    def test_export_app_open_browser(self, _employees_catalog, tmp_path, monkeypatch):
        """export_app with open_browser=True should open browser."""
        opened_urls = []
        monkeypatch.setattr("webbrowser.open", lambda url: opened_urls.append(url))

        _employees_catalog.export_app(tmp_path, open_browser=True, quiet=True)

        assert len(opened_urls) == 1
        assert "index.html" in opened_urls[0]

    def test_export_app_without_app_raises(
        self, _employees_catalog, tmp_path, monkeypatch
    ):
        """export_app should raise FileNotFoundError if app not bundled."""
        # Mock _get_app_path to return nonexistent path
        monkeypatch.setattr(exporter, "_get_app_path", lambda: Path("/nonexistent"))

        with pytest.raises(ConfigError, match="datannur app not found"):
            _employees_catalog.export_app(tmp_path)

    def test_export_app_uses_app_path_by_default(self, tmp_path):
        """export_app() without args should use app_path."""
        app_dir = tmp_path / "output"

        catalog = Catalog(app_path=app_dir, quiet=True)
        catalog.add_folder(
            DATA_DIR,
            metadata=EntityMetadata(id="test", name="Test"),
            include=["**/employees.csv"],
        )
        catalog.export_app()

        assert (app_dir / "index.html").exists()
        assert (app_dir / "data" / "db" / "folder.json").exists()

    def test_export_app_without_app_path_raises(self):
        """export_app() without args and no app_path should raise."""
        catalog = Catalog(quiet=True)
        catalog.add_folder(
            DATA_DIR,
            metadata=EntityMetadata(id="test", name="Test"),
            include=["**/employees.csv"],
        )

        with pytest.raises(ConfigError, match="output_dir is required"):
            catalog.export_app()

    def test_export_app_writes_config_json(self, tmp_path):
        """export_app should write config.json in data/db/."""
        catalog = Catalog(app_config={"contact_email": "x@y.com", "banner": "Hi"})
        catalog.add_folder(
            DATA_DIR,
            metadata=EntityMetadata(id="test", name="Test"),
            include=["**/employees.csv"],
        )
        catalog.export_app(tmp_path, quiet=True)

        config_path = tmp_path / "data" / "db" / "config.json"
        assert config_path.exists()
        data = json.loads(config_path.read_text())
        by_id = {r["id"]: r["value"] for r in data}
        assert by_id["contact_email"] == "x@y.com"
        assert by_id["banner"] == "Hi"

        js_path = tmp_path / "data" / "db" / "config.json.js"
        assert js_path.exists()

    def test_export_app_without_scan_preserves_catalog(self, tmp_path):
        """Reload a catalog from disk then export_app without scanning should not empty it."""
        app_dir = tmp_path / "app"

        # First pass: scan + export
        cat1 = Catalog(app_path=app_dir, quiet=True)
        cat1.add_folder(
            DATA_DIR,
            metadata=EntityMetadata(id="test", name="Test"),
            include=["**/employees.csv"],
        )
        cat1.export_app()

        folder_count = cat1.folder.count
        dataset_count = cat1.dataset.count
        assert folder_count > 0
        assert dataset_count > 0

        # Second pass: reload from disk, no scan, re-export
        cat2 = Catalog(app_path=app_dir, quiet=True)
        cat2.export_app()

        assert cat2.folder.count == folder_count
        assert cat2.dataset.count == dataset_count
