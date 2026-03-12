"""Tests for YAML configuration module."""

from __future__ import annotations

from pathlib import Path

from datannurpy import run_config


class TestRunConfig:
    """Test running configurations."""

    def test_run_config_folder(self, tmp_path: Path, data_dir: Path):
        """Run a config that scans a folder."""
        config_file = tmp_path / "catalog.yml"
        config_file.write_text(f"""
app_path: {tmp_path / "output"}
refresh: true
quiet: true

add:
  - type: folder
    path: {data_dir / "csv"}
    folder:
      id: test_csv
      name: Test CSV
""")
        catalog = run_config(config_file)

        assert len(catalog.folder.all()) >= 1
        assert any(f.id == "test_csv" for f in catalog.folder.all())
        assert len(catalog.dataset.all()) > 0

    def test_run_config_database(self, tmp_path: Path):
        """Run a config that scans a database."""
        import sqlite3

        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE users (id INTEGER, name TEXT)")
        conn.execute("INSERT INTO users VALUES (1, 'Alice')")
        conn.commit()
        conn.close()

        config_file = tmp_path / "catalog.yml"
        config_file.write_text(f"""
app_path: {tmp_path / "output"}
refresh: true
quiet: true

add:
  - type: database
    uri: sqlite:///{db_path}
    folder:
      id: test_db
      name: Test Database
""")
        catalog = run_config(config_file)

        assert any(f.id == "test_db" for f in catalog.folder.all())
        assert any(d.name == "users" for d in catalog.dataset.all())

    def test_run_config_with_export_db(self, tmp_path: Path):
        """Config with export_db should create db files."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        output_dir = tmp_path / "output"
        config_file = tmp_path / "catalog.yml"
        config_file.write_text(f"""
app_path: {output_dir}
refresh: true
quiet: true

add:
  - type: folder
    path: {data_dir}

export_db: {{}}
""")
        run_config(config_file)

        db_dir = output_dir / "data" / "db"
        assert db_dir.exists()
        assert (db_dir / "__table__.json").exists()

    def test_run_config_with_export_app(self, tmp_path: Path):
        """Config with export_app should create app files."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        output_dir = tmp_path / "output"
        config_file = tmp_path / "catalog.yml"
        config_file.write_text(f"""
app_path: {output_dir}
refresh: true
quiet: true

add:
  - type: folder
    path: {data_dir}

export_app:
  open_browser: false
""")
        run_config(config_file)

        assert output_dir.exists()
        assert (output_dir / "data" / "db" / "__table__.json").exists()
        assert (output_dir / "index.html").exists()

    def test_run_config_with_metadata(self, tmp_path: Path, data_dir: Path):
        """Config with metadata should load metadata."""
        output_dir = tmp_path / "output"
        config_file = tmp_path / "catalog.yml"
        config_file.write_text(f"""
app_path: {output_dir}
refresh: true
quiet: true

add:
  - type: folder
    path: {data_dir / "csv"}

  - type: metadata
    path: {data_dir / "metadata"}
""")
        catalog = run_config(config_file)

        assert len(catalog.dataset.all()) > 0

    def test_run_config_mixed_sources(self, tmp_path: Path):
        """Config with folder, database and metadata."""
        import sqlite3

        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE mixed_table (id INTEGER)")
        conn.commit()
        conn.close()

        config_file = tmp_path / "catalog.yml"
        config_file.write_text(f"""
app_path: {tmp_path / "output"}
refresh: true
quiet: true

add:
  - type: folder
    path: {data_dir}

  - type: database
    uri: sqlite:///{db_path}
""")
        catalog = run_config(config_file)

        datasets = [d.name for d in catalog.dataset.all()]
        assert "mixed_table" in datasets

    def test_run_config_no_add(self, tmp_path: Path):
        """Config without add should create empty catalog."""
        config_file = tmp_path / "catalog.yml"
        config_file.write_text(f"""
app_path: {tmp_path / "output"}
""")
        catalog = run_config(config_file)

        assert len(catalog.dataset.all()) == 0

    def test_run_config_no_export(self, tmp_path: Path):
        """Config without export should not create output files."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        output_dir = tmp_path / "output"
        config_file = tmp_path / "catalog.yml"
        config_file.write_text(f"""
app_path: {output_dir}
refresh: true
quiet: true

add:
  - type: folder
    path: {data_dir}
""")
        catalog = run_config(config_file)

        assert len(catalog.dataset.all()) == 1
        assert not (output_dir / "data" / "db").exists()

    def test_run_config_unknown_type_ignored(self, tmp_path: Path):
        """Unknown add type should be silently ignored."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        config_file = tmp_path / "catalog.yml"
        config_file.write_text(f"""
app_path: {tmp_path / "output"}
refresh: true
quiet: true

add:
  - type: unknown
    path: {data_dir}
  - type: folder
    path: {data_dir}
""")
        catalog = run_config(config_file)

        # Unknown type ignored, folder still processed
        assert len(catalog.dataset.all()) == 1
