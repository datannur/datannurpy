"""Tests for YAML configuration module."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from datannurpy import run_config
from datannurpy.config.config import _expand_vars


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

    def test_run_config_dataset(self, tmp_path: Path, data_dir: Path):
        """Run a config that scans a single dataset."""
        config_file = tmp_path / "catalog.yml"
        config_file.write_text(f"""
app_path: {tmp_path / "output"}
refresh: true
quiet: true

add:
  - type: dataset
    path: {data_dir / "csv" / "employees.csv"}
    name: Custom Name
    description: Custom description
""")
        catalog = run_config(config_file)

        assert len(catalog.dataset.all()) == 1
        ds = catalog.dataset.all()[0]
        assert ds.name == "Custom Name"
        assert ds.description == "Custom description"

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

    def test_run_config_no_app_path(self, tmp_path: Path):
        """Config without app_path should work."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        config_file = tmp_path / "catalog.yml"
        config_file.write_text(f"""
quiet: true

add:
  - type: folder
    path: {data_dir}
""")
        catalog = run_config(config_file)

        assert len(catalog.dataset.all()) == 1

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

    def test_run_config_unknown_type_raises(self, tmp_path: Path):
        """Unknown add type should raise ValueError."""
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
""")
        with pytest.raises(ValueError, match="Unknown type 'unknown'"):
            run_config(config_file)

    def test_run_config_relative_paths(self, tmp_path: Path):
        """Relative paths should be resolved relative to config file."""
        # Create structure: config_dir/catalog.yml, config_dir/data/test.csv
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        data_dir = config_dir / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        config_file = config_dir / "catalog.yml"
        # Use relative paths in config
        config_file.write_text("""
app_path: ./output
refresh: true
quiet: true

add:
  - type: folder
    path: ./data
""")
        catalog = run_config(config_file)

        assert len(catalog.dataset.all()) == 1
        # Output should be created relative to config file
        assert (config_dir / "output" / "data" / "db").exists() is False  # no export

    def test_run_config_relative_sqlite_path(self, tmp_path: Path):
        """Relative sqlite:/// paths should be resolved relative to config file."""
        import sqlite3

        config_dir = tmp_path / "config"
        config_dir.mkdir()
        db_path = config_dir / "test.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE users (id INTEGER)")
        conn.commit()
        conn.close()

        config_file = config_dir / "catalog.yml"
        config_file.write_text("""
app_path: ./output
refresh: true
quiet: true

add:
  - type: database
    uri: sqlite:///test.db
""")
        catalog = run_config(config_file)

        assert any(d.name == "users" for d in catalog.dataset.all())

    def test_run_config_absolute_paths(self, tmp_path: Path):
        """Absolute paths should be used as-is."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        # Config in different directory
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        config_file = config_dir / "catalog.yml"
        # Use absolute paths
        config_file.write_text(f"""
app_path: {tmp_path / "output"}
refresh: true
quiet: true

add:
  - type: folder
    path: {data_dir}
""")
        catalog = run_config(config_file)

        assert len(catalog.dataset.all()) == 1


class TestExpandVars:
    """Tests for recursive variable expansion."""

    def test_string(self) -> None:
        os.environ["MY_TEST_EXP"] = "world"
        try:
            assert _expand_vars("hello $MY_TEST_EXP") == "hello world"
            assert _expand_vars("${MY_TEST_EXP}!") == "world!"
        finally:
            os.environ.pop("MY_TEST_EXP", None)

    def test_dict(self) -> None:
        os.environ["MY_TEST_EXP2"] = "val"
        try:
            result = _expand_vars({"key": "$MY_TEST_EXP2", "num": 42})
            assert result == {"key": "val", "num": 42}
        finally:
            os.environ.pop("MY_TEST_EXP2", None)

    def test_list(self) -> None:
        os.environ["MY_TEST_EXP3"] = "x"
        try:
            assert _expand_vars(["$MY_TEST_EXP3", 1]) == ["x", 1]
        finally:
            os.environ.pop("MY_TEST_EXP3", None)

    def test_non_string_passthrough(self) -> None:
        assert _expand_vars(42) == 42
        assert _expand_vars(None) is None
        assert _expand_vars(True) is True


class TestRunConfigEnvExpansion:
    """Integration tests for .env + YAML config."""

    def test_env_in_database_uri(self, tmp_path: Path) -> None:
        """Environment variables in database URI are expanded."""
        import sqlite3

        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE env_test (id INTEGER)")
        conn.commit()
        conn.close()

        env_file = tmp_path / ".env"
        env_file.write_text(f"TEST_DB_PATH={db_path}\n")

        config_file = tmp_path / "catalog.yml"
        config_file.write_text("""
quiet: true
refresh: true

add:
  - type: database
    uri: sqlite:///${TEST_DB_PATH}
""")
        try:
            catalog = run_config(config_file)
            assert any(d.name == "env_test" for d in catalog.dataset.all())
        finally:
            os.environ.pop("TEST_DB_PATH", None)

    def test_env_in_folder_path(self, tmp_path: Path) -> None:
        """Environment variables in folder path are expanded."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        env_file = tmp_path / ".env"
        env_file.write_text(f"TEST_DATA_DIR={data_dir}\n")

        config_file = tmp_path / "catalog.yml"
        config_file.write_text("""
quiet: true
refresh: true

add:
  - type: folder
    path: ${TEST_DATA_DIR}
""")
        try:
            catalog = run_config(config_file)
            assert len(catalog.dataset.all()) == 1
        finally:
            os.environ.pop("TEST_DATA_DIR", None)

    def test_env_file_custom_path(self, tmp_path: Path) -> None:
        """env_file in YAML points to a .env in a different directory."""
        import sqlite3

        secrets_dir = tmp_path / "secrets"
        secrets_dir.mkdir()
        env_file = secrets_dir / ".env"

        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE custom_env (id INTEGER)")
        conn.commit()
        conn.close()

        env_file.write_text(f"TEST_CUSTOM_DB={db_path}\n")

        config_file = tmp_path / "catalog.yml"
        config_file.write_text(f"""
quiet: true
refresh: true
env_file: {secrets_dir / ".env"}

add:
  - type: database
    uri: sqlite:///${{TEST_CUSTOM_DB}}
""")
        try:
            catalog = run_config(config_file)
            assert any(d.name == "custom_env" for d in catalog.dataset.all())
        finally:
            os.environ.pop("TEST_CUSTOM_DB", None)
