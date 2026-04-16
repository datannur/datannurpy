"""Tests for Catalog.add_folder method."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from datannurpy import Catalog, Folder
from datannurpy.errors import ConfigError

DATA_DIR = Path(__file__).parent.parent / "data"
CSV_DIR = DATA_DIR / "csv"


class TestAddFolderFormats:
    """Test scanning different file formats."""

    def test_add_folder_scans_csv(self):
        """add_folder should scan CSV files."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["employees.csv"]
        )
        assert len(catalog.variable.all()) == 9

    def test_add_folder_scans_excel(self):
        """add_folder should scan Excel files."""
        catalog = Catalog()
        catalog.add_folder(DATA_DIR, Folder(id="test", name="Test"), include=["*.xlsx"])
        assert len(catalog.variable.all()) > 0

    def test_add_folder_empty_excel(self, tmp_path: Path):
        """add_folder should handle empty Excel files (0 bytes)."""
        (tmp_path / "empty.xlsx").write_bytes(b"")

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 0

    def test_add_folder_corrupted_excel(self, tmp_path: Path, capsys):
        """add_folder should warn on corrupted Excel files."""
        (tmp_path / "corrupted.xlsx").write_bytes(b"not a real excel file")

        catalog = Catalog()
        catalog.add_folder(tmp_path, quiet=False)

        captured = capsys.readouterr()
        assert "✗ corrupted.xlsx" in captured.err

        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 0

    def test_add_folder_empty_sheet_excel(self, tmp_path: Path):
        """add_folder should handle Excel files with empty sheet."""
        pd.DataFrame().to_excel(tmp_path / "empty_sheet.xlsx", index=False)

        catalog = Catalog()
        catalog.add_folder(tmp_path, quiet=True)

        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 0

    def test_add_folder_mixed_types_excel(self, tmp_path: Path):
        """add_folder should handle Excel columns with mixed types."""
        df = pd.DataFrame(
            {
                "COL_A": [1, b"bytes_value", "text", 3.14],
                "COL_B": [10, 20, 30, 40],
            }
        )
        df.to_excel(tmp_path / "mixed.xlsx", index=False)

        catalog = Catalog()
        catalog.add_folder(tmp_path, quiet=True)

        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 2

    def test_add_folder_mixed_types_excel_nan_not_string(self, tmp_path: Path):
        """NaN in mixed-type Excel columns should be counted as missing, not as 'nan' string."""
        df = pd.DataFrame(
            {
                "COL_A": [1, b"bytes_value", None, 3.14],
                "COL_B": ["hello", None, "world", "foo"],
            }
        )
        df.to_excel(tmp_path / "mixed_nan.xlsx", index=False)

        catalog = Catalog()
        catalog.add_folder(tmp_path, quiet=True)

        var_a = catalog.variable.get_by("name", "COL_A")
        assert var_a is not None
        assert var_a.nb_missing == 1

        var_b = catalog.variable.get_by("name", "COL_B")
        assert var_b is not None
        assert var_b.nb_missing == 1

        for val in catalog.value.all():
            assert val.value != "nan"

    def test_add_folder_excel_datetime_with_time(self, tmp_path: Path):
        """Excel datetime columns with non-midnight times stay as datetime."""
        df = pd.DataFrame(
            {
                "event": ["a", "b"],
                "ts": pd.to_datetime(["2024-01-15 10:30:00", "2024-02-20 14:45:00"]),
            }
        )
        df.to_excel(tmp_path / "events.xlsx", index=False)

        catalog = Catalog()
        catalog.add_folder(tmp_path, quiet=True)

        var = catalog.variable.get_by("name", "ts")
        assert var is not None
        assert var.type == "datetime"

    def test_add_folder_scans_parquet(self):
        """add_folder should scan Parquet files (.parquet extension)."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["test.parquet"]
        )
        assert len(catalog.dataset.all()) == 1
        assert catalog.dataset.all()[0].delivery_format == "parquet"
        assert len(catalog.variable.all()) == 3

    def test_add_folder_scans_pq(self):
        """add_folder should scan Parquet files (.pq extension)."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["test.pq"]
        )
        assert len(catalog.dataset.all()) == 1
        assert catalog.dataset.all()[0].delivery_format == "parquet"
        assert len(catalog.variable.all()) == 3

    def test_add_folder_extracts_parquet_metadata(self):
        """add_folder should extract metadata from Parquet files."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR,
            Folder(id="test", name="Test"),
            include=["test_with_metadata.parquet"],
        )
        assert (
            catalog.dataset.all()[0].description == "Table des employes de la societe"
        )
        var_by_name = {v.name: v for v in catalog.variable.all()}
        assert var_by_name["id"].description == "Identifiant unique"
        assert var_by_name["name"].description == "Nom complet de la personne"
        assert var_by_name["age"].description == "Age en annees"


class TestAddFolderIds:
    """Test ID generation in add_folder."""

    def test_add_folder_assigns_folder_id(self, full_catalog):
        """add_folder should assign folder_id to datasets."""
        assert all(
            ds.folder_id is not None
            and (ds.folder_id == "test" or ds.folder_id.startswith("test---"))
            for ds in full_catalog.dataset.all()
        )

    def test_add_folder_prefixes_ids(self):
        """add_folder should prefix IDs with folder ID."""
        catalog = Catalog()
        catalog.add_folder(
            CSV_DIR, Folder(id="src", name="Source"), include=["employees.csv"]
        )
        assert catalog.dataset.all()[0].id == "src---employees_csv"
        assert catalog.variable.all()[0].id.startswith("src---employees_csv---")


class TestAddFolderStats:
    """Test statistics inference."""

    def test_add_folder_infers_stats(self):
        """add_folder should compute stats by default."""
        catalog = Catalog()
        catalog.add_folder(
            CSV_DIR, Folder(id="test", name="Test"), include=["employees.csv"]
        )
        assert all(v.nb_distinct is not None for v in catalog.variable.all())
        assert all(v.nb_missing is not None for v in catalog.variable.all())

    def test_add_folder_without_stats(self):
        """add_folder with depth=variable should skip stats."""
        catalog = Catalog()
        catalog.add_folder(
            CSV_DIR,
            Folder(id="test", name="Test"),
            include=["employees.csv"],
            depth="variable",
        )
        assert all(v.nb_distinct is None for v in catalog.variable.all())
        assert all(v.nb_missing is None for v in catalog.variable.all())


class TestAddFolderOther:
    """Test other add_folder features."""

    def test_add_folder_ignores_unknown_formats(self, tmp_path: Path):
        """add_folder should skip files with unknown extensions when using include."""
        (tmp_path / "data.csv").write_text("x\n1")
        (tmp_path / "unknown.xyz").write_text("some data")
        (tmp_path / "readme.txt").write_text("documentation")

        catalog = Catalog()
        catalog.add_folder(tmp_path, include=["*.*"])

        assert len(catalog.dataset.all()) == 1
        assert catalog.dataset.all()[0].delivery_format == "csv"

    def test_add_folder_handles_empty_csv(self, tmp_path: Path):
        """add_folder should handle empty CSV files (header only)."""
        (tmp_path / "empty.csv").write_text("col1,col2\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        assert len(catalog.dataset.all()) == 1
        assert catalog.dataset.all()[0].nb_row == 0

    def test_add_folder_not_found(self):
        """add_folder should raise FileNotFoundError for missing path."""
        catalog = Catalog()
        with pytest.raises(ConfigError):
            catalog.add_folder("/nonexistent/path")

    def test_add_folder_default_folder(self):
        """add_folder without folder arg should use directory name."""
        catalog = Catalog()
        catalog.add_folder(DATA_DIR, include=["employees.csv"])
        assert catalog.folder.all()[0].id == "data"
        assert catalog.folder.all()[0].name == "data"

    def test_add_folder_sets_type_filesystem(self, full_catalog):
        """add_folder should set type='filesystem' on all folders."""
        for folder in full_catalog.folder.all():
            if folder.id != "_modalities":
                assert folder.type == "filesystem"


class TestSubfolders:
    """Test recursive subfolder scanning."""

    def test_add_folder_scans_subdirs(self, tmp_path: Path):
        """add_folder should scan files in subdirectories."""
        subdir = tmp_path / "2024" / "january"
        subdir.mkdir(parents=True)
        (subdir / "sales.csv").write_text("amount,qty\n100,5\n200,10")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="src", name="Source"))

        assert len(catalog.dataset.all()) == 1
        assert catalog.dataset.all()[0].id == "src---2024---january---sales_csv"

    def test_add_folder_creates_subfolders(self, tmp_path: Path):
        """add_folder should create Folder entities for subdirectories."""
        (tmp_path / "2024").mkdir()
        (tmp_path / "2024" / "data.csv").write_text("x\n1")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="root", name="Root"))

        user_folders = catalog.folder.where("id", "!=", "_modalities")
        assert len(user_folders) == 2  # root + 2024
        subfolder = user_folders[1]
        assert subfolder.id == "root---2024"
        assert subfolder.parent_id == "root"

    def test_add_folder_nested_subfolders(self, tmp_path: Path):
        """add_folder should handle deeply nested folders."""
        deep = tmp_path / "a" / "b" / "c"
        deep.mkdir(parents=True)
        (deep / "file.csv").write_text("col\n1")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="x", name="X"))

        user_folders = catalog.folder.where("id", "!=", "_modalities")
        assert len(user_folders) == 4
        folder_ids = [f.id for f in user_folders]
        assert "x" in folder_ids
        assert "x---a" in folder_ids
        assert "x---a---b" in folder_ids
        assert "x---a---b---c" in folder_ids

    def test_add_folder_subfolder_parent_chain(self, tmp_path: Path):
        """Subfolders should have correct parent_id chain."""
        (tmp_path / "a" / "b").mkdir(parents=True)
        (tmp_path / "a" / "b" / "data.csv").write_text("x\n1")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="root", name="Root"))

        folders_by_id = {f.id: f for f in catalog.folder.all()}
        assert folders_by_id["root---a"].parent_id == "root"
        assert folders_by_id["root---a---b"].parent_id == "root---a"

    def test_add_folder_non_recursive(self, tmp_path: Path):
        """add_folder with recursive=False should not scan subdirs."""
        (tmp_path / "root.csv").write_text("x\n1")
        subdir = tmp_path / "sub"
        subdir.mkdir()
        (subdir / "nested.csv").write_text("y\n2")

        catalog = Catalog()
        catalog.add_folder(tmp_path, recursive=False)

        assert len(catalog.dataset.all()) == 1
        assert catalog.dataset.all()[0].name == "root"

    def test_add_folder_non_recursive_with_include(self, tmp_path: Path):
        """add_folder with recursive=False and include should use glob directly."""
        (tmp_path / "a.csv").write_text("x\n1")
        (tmp_path / "b.csv").write_text("y\n2")

        catalog = Catalog()
        catalog.add_folder(tmp_path, include=["a.csv"], recursive=False)

        assert len(catalog.dataset.all()) == 1
        assert catalog.dataset.all()[0].name == "a"


class TestIdSanitization:
    """Test ID generation with special characters."""

    def test_sanitize_spaces_in_filename(self, tmp_path: Path):
        """Spaces in filenames should be preserved in IDs."""
        (tmp_path / "my file.csv").write_text("x\n1")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="src", name="Source"))

        assert catalog.dataset.all()[0].id == "src---my file_csv"

    def test_sanitize_special_chars(self, tmp_path: Path):
        """Special characters should be replaced with underscore."""
        (tmp_path / "data@2024#v1.csv").write_text("x\n1")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="src", name="Source"))

        assert catalog.dataset.all()[0].id == "src---data_2024_v1_csv"

    def test_sanitize_folder_name_with_spaces(self, tmp_path: Path):
        """Folder names with spaces should be handled."""
        subdir = tmp_path / "Year 2024"
        subdir.mkdir()
        (subdir / "data.csv").write_text("x\n1")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="root", name="Root"))

        assert "root---Year 2024" in [f.id for f in catalog.folder.all()]

    def test_sanitize_variable_name(self, tmp_path: Path):
        """Variable names with special chars should be sanitized."""
        (tmp_path / "data.csv").write_text("col@name,col#2\n1,2")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="src", name="Source"))

        var_ids = [v.id for v in catalog.variable.all()]
        assert "src---data_csv---col_name" in var_ids
        assert "src---data_csv---col_2" in var_ids


class TestIncludeExclude:
    """Test include/exclude glob patterns."""

    def test_include_single_pattern(self, tmp_path: Path):
        """include should filter to matching files only."""
        (tmp_path / "data.csv").write_text("x\n1")
        (tmp_path / "data.xlsx").write_bytes(b"")

        catalog = Catalog()
        catalog.add_folder(tmp_path, include=["*.csv"])

        assert len(catalog.dataset.all()) == 1
        assert catalog.dataset.all()[0].name == "data"

    def test_include_multiple_patterns(self, tmp_path: Path):
        """include should accept multiple patterns."""
        (tmp_path / "a.csv").write_text("x\n1")
        (tmp_path / "b.csv").write_text("y\n2")
        (tmp_path / "c.txt").write_text("ignored")

        catalog = Catalog()
        catalog.add_folder(tmp_path, include=["a.csv", "b.csv"])

        assert len(catalog.dataset.all()) == 2

    def test_exclude_pattern(self, tmp_path: Path):
        """exclude should filter out matching files."""
        (tmp_path / "keep.csv").write_text("x\n1")
        (tmp_path / "skip.csv").write_text("y\n2")

        catalog = Catalog()
        catalog.add_folder(tmp_path, exclude=["skip.csv"])

        assert len(catalog.dataset.all()) == 1
        assert catalog.dataset.all()[0].name == "keep"

    def test_exclude_subdirectory(self, tmp_path: Path):
        """exclude should filter out subdirectories."""
        (tmp_path / "data.csv").write_text("x\n1")
        archive = tmp_path / "archive"
        archive.mkdir()
        (archive / "old.csv").write_text("y\n2")

        catalog = Catalog()
        catalog.add_folder(tmp_path, exclude=["archive"])

        assert len(catalog.dataset.all()) == 1
        assert catalog.dataset.all()[0].name == "data"

    def test_exclude_nested_subdirectory(self, tmp_path: Path):
        """exclude should filter out nested subdirectories."""
        (tmp_path / "data.csv").write_text("x\n1")
        archive = tmp_path / "archive"
        archive.mkdir()
        (archive / "keep.csv").write_text("y\n2")
        tmp = archive / "tmp"
        tmp.mkdir()
        (tmp / "old.csv").write_text("z\n3")

        catalog = Catalog()
        catalog.add_folder(tmp_path, exclude=["archive/tmp"])

        assert len(catalog.dataset.all()) == 2
        names = {d.name for d in catalog.dataset.all()}
        assert names == {"data", "keep"}

    def test_exclude_glob_pattern(self, tmp_path: Path):
        """exclude should support glob patterns."""
        (tmp_path / "data.csv").write_text("x\n1")
        (tmp_path / "backup.csv").write_text("y\n2")
        (tmp_path / "report.txt").write_text("ignored")

        catalog = Catalog()
        catalog.add_folder(tmp_path, exclude=["backup.*"])

        assert len(catalog.dataset.all()) == 1
        assert catalog.dataset.all()[0].name == "data"

    def test_exclude_nonexistent_file(self, tmp_path: Path):
        """exclude with nonexistent file should be ignored."""
        (tmp_path / "data.csv").write_text("x\n1")

        catalog = Catalog()
        catalog.add_folder(tmp_path, exclude=["nonexistent.csv"])

        assert len(catalog.dataset.all()) == 1
        assert catalog.dataset.all()[0].name == "data"


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_folder(self, tmp_path: Path):
        """Empty folder should create no datasets."""
        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="empty", name="Empty"))

        assert len(catalog.folder.all()) == 1
        assert len(catalog.dataset.all()) == 0
        assert len(catalog.variable.all()) == 0

    def test_empty_csv_file(self, tmp_path: Path):
        """Empty CSV should create dataset with no variables."""
        (tmp_path / "empty.csv").write_text("")

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 0

    def test_csv_headers_only(self, tmp_path: Path):
        """CSV with headers but no data should create variables."""
        (tmp_path / "headers.csv").write_text("col_a,col_b,col_c\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 3

    def test_not_a_directory(self, tmp_path: Path):
        """add_folder should raise for file path."""
        file = tmp_path / "file.csv"
        file.write_text("x\n1")

        catalog = Catalog()
        with pytest.raises(ConfigError):
            catalog.add_folder(file)

    def test_add_folder_rejects_dataset_path(self):
        """add_folder should raise for Delta/Hive/Iceberg paths."""
        catalog = Catalog()
        with pytest.raises(ConfigError, match="Use add_dataset"):
            catalog.add_folder(DATA_DIR / "test_delta")

    def test_unsupported_file_extension(self, tmp_path: Path):
        """Unsupported files should be ignored."""
        (tmp_path / "data.csv").write_text("x\n1")
        (tmp_path / "readme.txt").write_text("ignored")
        (tmp_path / "config.json").write_text("{}")

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        assert len(catalog.dataset.all()) == 1


class TestIncrementalScanSubfolders:
    """Test incremental scan with subdirectories."""

    def test_rescan_marks_existing_subfolders_as_seen(self, tmp_path: Path):
        """Rescanning should mark existing subfolders as _seen=True."""
        app_dir = tmp_path / "app"
        data_dir = tmp_path / "data"
        sub_dir = data_dir / "subdir"
        sub_dir.mkdir(parents=True)
        (sub_dir / "file.csv").write_text("a,b\n1,2\n")

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog1.export_db()

        # Reload and rescan
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog2.finalize()

        # All user folders should be kept (excluding _modalities system folder)
        user_folders = catalog2.folder.where("id", "!=", "_modalities")
        assert len(user_folders) == 2
        assert any(f.id == "src" for f in user_folders)
        assert any("subdir" in f.id for f in user_folders)


class TestDepthParameter:
    """Test depth parameter for progressive scanning."""

    def test_depth_at_catalog_level(self, tmp_path: Path):
        """depth can be set at Catalog level and overridden per add_folder."""
        (tmp_path / "data.csv").write_text("a,b\n1,2\n")

        # Set depth at catalog level
        catalog = Catalog(depth="dataset")
        catalog.add_folder(tmp_path)

        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 0  # dataset mode

        # Override at add_folder level
        catalog2 = Catalog(depth="dataset")
        catalog2.add_folder(tmp_path, depth="variable")

        assert len(catalog2.dataset.all()) == 1
        assert len(catalog2.variable.all()) == 2  # schema mode

    def test_depth_dataset_creates_datasets_without_variables(self, tmp_path: Path):
        """depth='dataset' should create datasets but no variables."""
        (tmp_path / "data.csv").write_text("a,b,c\n1,2,3\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path, depth="dataset")

        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 0

        ds = catalog.dataset.all()[0]
        assert ds.delivery_format == "csv"
        assert ds.nb_row is None  # No scanning

    def test_depth_dataset_creates_subfolders(self, tmp_path: Path):
        """depth='dataset' should still create subfolder hierarchy."""
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "data.csv").write_text("a\n1\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path, depth="dataset")

        folders = catalog.folder.all()
        assert len(folders) == 2  # root + sub

    def test_depth_dataset_marks_existing_as_seen(self, tmp_path: Path):
        """depth='dataset' should mark existing datasets as seen on rescan."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)  # Full scan first
        ds = catalog.dataset.all()[0]
        assert ds.nb_row == 1

        # Reset _seen
        catalog.dataset.update(ds.id, _seen=False)

        # Rescan with dataset only
        catalog.add_folder(tmp_path, depth="dataset")

        ds = catalog.dataset.get(ds.id)
        assert ds is not None
        assert ds._seen is True
        assert ds.nb_row == 1  # Preserved from first scan

    def test_depth_dataset_updates_mtime_on_modified(self, tmp_path: Path):
        """depth='dataset' should update mtime when file is modified."""
        import os

        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")
        # Set old mtime (1 day ago)
        old_mtime = int(csv_file.stat().st_mtime) - 86400
        os.utime(csv_file, (old_mtime, old_mtime))

        catalog = Catalog()
        catalog.add_folder(tmp_path, depth="dataset")
        ds = catalog.dataset.all()[0]
        assert ds.last_update_timestamp is not None
        original_mtime = ds.last_update_timestamp

        # Modify file (will have current mtime)
        csv_file.write_text("a,b\n1,2\n3,4\n")

        # Rescan with structure
        catalog.add_folder(tmp_path, depth="dataset")

        ds = catalog.dataset.get(ds.id)
        assert ds is not None
        assert ds.last_update_timestamp is not None
        assert ds.last_update_timestamp > original_mtime

    def test_depth_value_is_default(self, tmp_path: Path):
        """depth='value' should be the default (with variables and stats)."""
        (tmp_path / "data.csv").write_text("a,b\n1,2\n3,4\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)  # Default depth="value"

        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 2
        assert catalog.dataset.all()[0].nb_row == 2

    def test_depth_schema_scans_variables_without_stats(self, tmp_path: Path):
        """depth='variable' should scan variables but no row count or stats."""
        (tmp_path / "data.csv").write_text("a,b,c\n1,2,3\n4,5,6\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path, depth="variable")

        # Should have dataset and variables
        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 3

        # No row count
        ds = catalog.dataset.all()[0]
        assert ds.nb_row is None

        # No stats on variables
        for var in catalog.variable.all():
            assert var.nb_distinct is None
            assert var.nb_missing is None

    def test_depth_schema_parquet(self, tmp_path: Path):
        """depth='variable' should work with parquet files."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        pq.write_table(table, tmp_path / "data.parquet")

        catalog = Catalog()
        catalog.add_folder(tmp_path, depth="variable")

        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 2
        assert catalog.dataset.all()[0].nb_row is None

        # Check types were inferred
        vars_by_name = {v.name: v for v in catalog.variable.all()}
        assert vars_by_name["x"].type == "integer"
        assert vars_by_name["y"].type == "string"

    def test_depth_schema_excel(self, tmp_path: Path):
        """depth='variable' should work with Excel files."""
        import pandas as pd

        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        df.to_excel(tmp_path / "data.xlsx", index=False)

        catalog = Catalog()
        catalog.add_folder(tmp_path, depth="variable")

        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 2
        assert catalog.dataset.all()[0].nb_row is None

    def test_depth_schema_statistical(self):
        """depth='variable' should work with statistical files (SAS)."""
        catalog = Catalog()
        catalog.add_folder(DATA_DIR, include=["cars.sas7bdat"], depth="variable")

        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) > 0
        assert catalog.dataset.all()[0].nb_row is None

    def test_depth_schema_delta(self):
        """depth='variable' should work with Delta tables."""
        delta_path = DATA_DIR / "test_delta"
        if not delta_path.exists():
            pytest.skip("Delta test data not available")

        catalog = Catalog()
        catalog.add_folder(DATA_DIR, include=["test_delta/**"], depth="variable")

        ds = next((d for d in catalog.dataset.all() if "delta" in d.id), None)
        if ds:
            assert ds.nb_row is None
            # Should have variables
            vars_for_ds = [v for v in catalog.variable.all() if v.dataset_id == ds.id]
            assert len(vars_for_ds) > 0

    def test_depth_schema_hive(self):
        """depth='variable' should work with Hive partitioned datasets."""
        hive_path = DATA_DIR / "test_partitioned"
        if not hive_path.exists():
            pytest.skip("Hive test data not available")

        catalog = Catalog()
        catalog.add_folder(DATA_DIR, include=["test_partitioned/**"], depth="variable")

        ds = next((d for d in catalog.dataset.all() if "partitioned" in d.id), None)
        if ds:
            assert ds.nb_row is None

    def test_depth_stat_computes_stats_without_modalities(self, tmp_path: Path):
        """depth='stat' should compute stats but skip modalities and freq."""
        (tmp_path / "data.csv").write_text("a,b\nfoo,1\nbar,2\nbaz,3\n")

        catalog = Catalog(freq_threshold=10)
        catalog.add_folder(tmp_path, depth="stat")

        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 2

        ds = catalog.dataset.all()[0]
        assert ds.nb_row == 3

        # Stats should be computed
        vars_by_name = {v.name: v for v in catalog.variable.all()}
        assert vars_by_name["b"].nb_distinct is not None
        assert vars_by_name["b"].min is not None

        # No modalities or freq tables
        assert len(catalog.modality.all()) == 0
        assert catalog.freq.is_empty


class TestRemoteStorage:
    """Test remote storage URL handling."""

    def test_remote_url_requires_provider_package(self):
        """add_folder should propagate ImportError from missing provider."""
        from unittest.mock import patch

        catalog = Catalog()
        with (
            patch(
                "datannurpy.add_folder.FileSystem",
                side_effect=ImportError("Install s3fs to access S3"),
            ),
            pytest.raises(ImportError, match="s3fs"),
        ):
            catalog.add_folder("s3://bucket/data")

    def test_remote_url_with_connection_error(self):
        """add_folder should propagate connection errors from remote storage."""
        from unittest.mock import patch

        catalog = Catalog()
        with (
            patch(
                "datannurpy.add_folder.FileSystem",
                side_effect=OSError("Connection refused"),
            ),
            pytest.raises(OSError, match="Connection refused"),
        ):
            catalog.add_folder("sftp://host/data", storage_options={"timeout": 1})

    def test_remote_folder_not_found(self, tmp_path: Path):
        """add_folder should raise FileNotFoundError for non-existent remote folder."""
        from unittest.mock import patch, MagicMock

        mock_fs = MagicMock()
        mock_fs.root = "memory://test/data"
        mock_fs.exists.return_value = False

        with patch("datannurpy.add_folder.FileSystem", return_value=mock_fs):
            catalog = Catalog()
            with pytest.raises(ConfigError, match="Folder not found"):
                catalog.add_folder("memory://test/data")

    def test_remote_not_a_directory(self, tmp_path: Path):
        """add_folder should raise NotADirectoryError for remote file."""
        from unittest.mock import patch, MagicMock

        mock_fs = MagicMock()
        mock_fs.root = "memory://test/file.csv"
        mock_fs.exists.return_value = True
        mock_fs.isdir.return_value = False

        with patch("datannurpy.add_folder.FileSystem", return_value=mock_fs):
            catalog = Catalog()
            with pytest.raises(ConfigError, match="Not a directory"):
                catalog.add_folder("memory://test/file.csv")

    def test_remote_folder_is_dataset(self, tmp_path: Path):
        """add_folder should raise ValueError if remote path is a dataset."""
        from unittest.mock import patch, MagicMock

        mock_fs = MagicMock()
        mock_fs.root = "memory://test/data"
        mock_fs.exists.return_value = True
        mock_fs.isdir.return_value = True

        with patch("datannurpy.add_folder.FileSystem", return_value=mock_fs):
            with patch("datannurpy.add_folder.is_delta_table", return_value=True):
                catalog = Catalog()
                with pytest.raises(ConfigError, match="dataset, not a folder"):
                    catalog.add_folder("memory://test/data")


class TestListPath:
    """Test add_folder with a list of paths."""

    def test_add_folder_list_of_paths(self, tmp_path: Path):
        """add_folder with a list scans each path."""
        d1 = tmp_path / "a"
        d1.mkdir()
        (d1 / "f.csv").write_text("x\n1")
        d2 = tmp_path / "b"
        d2.mkdir()
        (d2 / "g.csv").write_text("y\n2")

        catalog = Catalog(quiet=True)
        catalog.add_folder([d1, d2])

        names = {d.id for d in catalog.dataset.all()}
        assert "a---f_csv" in names
        assert "b---g_csv" in names

    def test_add_folder_list_shared_options(self, tmp_path: Path):
        """Options are shared across all paths in the list."""
        d1 = tmp_path / "a"
        d1.mkdir()
        (d1 / "f.csv").write_text("x\n1")
        (d1 / "skip.txt").write_text("no")
        d2 = tmp_path / "b"
        d2.mkdir()
        (d2 / "g.csv").write_text("y\n2")
        (d2 / "skip.txt").write_text("no")

        catalog = Catalog(quiet=True)
        catalog.add_folder([d1, d2], include=["*.csv"])

        assert len(catalog.dataset.all()) == 2


class TestAddFolderKwargs:
    """Test id/name/description kwargs on add_folder."""

    def test_name_only_auto_generates_id(self, tmp_path: Path):
        """Passing name without id auto-generates id from path."""
        d = tmp_path / "sales"
        d.mkdir()
        (d / "f.csv").write_text("x\n1")

        catalog = Catalog(quiet=True)
        catalog.add_folder(d, name="Sales Data")

        folders = catalog.folder.all()
        assert any(f.id == "sales" and f.name == "Sales Data" for f in folders)

    def test_description_only_auto_generates_id(self, tmp_path: Path):
        """Passing description without id auto-generates id from path."""
        d = tmp_path / "hr"
        d.mkdir()
        (d / "f.csv").write_text("x\n1")

        catalog = Catalog(quiet=True)
        catalog.add_folder(d, description="HR data")

        folders = catalog.folder.all()
        f = next(f for f in folders if f.id == "hr")
        assert f.description == "HR data"

    def test_id_name_description(self, tmp_path: Path):
        """Passing all three kwargs works."""
        d = tmp_path / "data"
        d.mkdir()
        (d / "f.csv").write_text("x\n1")

        catalog = Catalog(quiet=True)
        catalog.add_folder(d, id="my_id", name="My Name", description="My Desc")

        folders = catalog.folder.all()
        f = next(f for f in folders if f.id == "my_id")
        assert f.name == "My Name"
        assert f.description == "My Desc"

    def test_folder_and_kwargs_raises(self, tmp_path: Path):
        """Cannot specify both folder and id/name/description."""
        d = tmp_path / "data"
        d.mkdir()

        with pytest.raises(ConfigError, match="Cannot specify both"):
            Catalog(quiet=True).add_folder(d, folder=Folder(id="x"), name="conflict")
