"""Tests for Catalog.export_app method."""

from pathlib import Path

import pytest

from datannurpy import Catalog, Folder

DATA_DIR = Path(__file__).parent.parent / "data"


class TestExportApp:
    """Test Catalog.export_app method."""

    def test_export_app_copies_index_html(self, tmp_path):
        """export_app should copy app files including index.html."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["employees.csv"]
        )
        catalog.export_app(tmp_path)

        assert (tmp_path / "index.html").exists()
        assert (tmp_path / "assets").is_dir()

    def test_export_app_writes_to_data_db(self, tmp_path):
        """export_app should write data to data/db/ subdirectory."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["employees.csv"]
        )
        catalog.export_app(tmp_path)

        db_dir = tmp_path / "data" / "db"
        assert (db_dir / "folder.json").exists()
        assert (db_dir / "dataset.json").exists()
        assert (db_dir / "variable.json").exists()
        assert (db_dir / "__table__.json").exists()

    def test_export_app_clears_existing_db(self, tmp_path):
        """export_app should clear existing data/db/ content."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["employees.csv"]
        )

        # First export
        catalog.export_app(tmp_path)

        # Create extra file
        extra_file = tmp_path / "data" / "db" / "old_data.json"
        extra_file.write_text("[]")

        # Second export should remove it
        catalog.export_app(tmp_path)

        assert not extra_file.exists()

    def test_export_app_without_app_raises(self, tmp_path, monkeypatch):
        """export_app should raise FileNotFoundError if app not bundled."""
        from datannurpy.writers import app

        # Mock get_app_path to return nonexistent path
        monkeypatch.setattr(app, "get_app_path", lambda: Path("/nonexistent"))

        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["employees.csv"]
        )

        with pytest.raises(FileNotFoundError, match="datannur app not found"):
            catalog.export_app(tmp_path)
