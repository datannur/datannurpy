"""Tests for Catalog.add_folder and Catalog.add_dataset."""

from pathlib import Path

import pytest

from datannurpy import Catalog, Folder

DATA_DIR = Path(__file__).parent.parent / "data"


class TestAddDataset:
    """Test Catalog.add_dataset method."""

    def test_add_dataset_scans_file(self):
        """add_dataset should scan a single file."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "employees.csv")

        assert len(catalog.datasets) == 1
        assert len(catalog.variables) == 9

    def test_add_dataset_with_folder(self):
        """add_dataset with folder should create folder and link."""
        catalog = Catalog()
        catalog.add_dataset(
            DATA_DIR / "employees.csv",
            folder=Folder(id="hr", name="HR Data"),
        )

        # +1 for _modalities folder (auto-created)
        assert len([f for f in catalog.folders if f.id != "_modalities"]) == 1
        assert catalog.folders[0].id == "hr"
        assert catalog.datasets[0].folder_id == "hr"
        assert catalog.datasets[0].id == "hr---employees"

    def test_add_dataset_with_folder_id(self):
        """add_dataset with folder_id should link to existing folder."""
        catalog = Catalog()
        catalog.add_folder(DATA_DIR, Folder(id="data", name="Data"), include=[])
        catalog.add_dataset(DATA_DIR / "employees.csv", folder_id="data")

        assert catalog.datasets[0].folder_id == "data"

    def test_add_dataset_reuses_folder(self):
        """add_dataset should not duplicate folder."""
        catalog = Catalog()
        folder = Folder(id="src", name="Source")
        catalog.add_dataset(DATA_DIR / "employees.csv", folder=folder)
        catalog.add_dataset(DATA_DIR / "regions_france.csv", folder=folder)

        # +1 for _modalities folder (auto-created)
        assert len([f for f in catalog.folders if f.id != "_modalities"]) == 1
        assert len(catalog.datasets) == 2

    def test_add_dataset_with_metadata(self):
        """add_dataset should accept metadata overrides."""
        catalog = Catalog()
        catalog.add_dataset(
            DATA_DIR / "employees.csv",
            name="Employés",
            description="Liste des employés",
            type="référentiel",
            link="https://example.com",
            start_date="2020/01/01",
        )

        ds = catalog.datasets[0]
        assert ds.name == "Employés"
        assert ds.description == "Liste des employés"
        assert ds.type == "référentiel"
        assert ds.link == "https://example.com"
        assert ds.start_date == "2020/01/01"

    def test_add_dataset_custom_id(self):
        """add_dataset with id should use custom ID."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "employees.csv", id="custom-id")

        assert catalog.datasets[0].id == "custom-id"

    def test_add_dataset_standalone_id(self):
        """add_dataset without folder should use filename as ID."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "employees.csv")

        assert catalog.datasets[0].id == "employees"

    def test_add_dataset_not_found(self):
        """add_dataset should raise FileNotFoundError."""
        catalog = Catalog()
        with pytest.raises(FileNotFoundError):
            catalog.add_dataset("/nonexistent/file.csv")

    def test_add_dataset_unsupported_format(self, tmp_path: Path):
        """add_dataset should raise for unsupported formats."""
        (tmp_path / "data.json").write_text("{}")

        catalog = Catalog()
        with pytest.raises(ValueError, match="Unsupported format"):
            catalog.add_dataset(tmp_path / "data.json")

    def test_add_dataset_folder_and_folder_id_error(self):
        """add_dataset should raise if both folder and folder_id given."""
        catalog = Catalog()
        with pytest.raises(ValueError, match="Cannot specify both"):
            catalog.add_dataset(
                DATA_DIR / "employees.csv",
                folder=Folder(id="a", name="A"),
                folder_id="b",
            )


class TestAddFolder:
    """Test Catalog.add_folder method."""

    def test_add_folder_scans_csv(self):
        """add_folder should scan CSV files."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["employees.csv"]
        )
        assert len(catalog.variables) == 9

    def test_add_folder_scans_excel(self):
        """add_folder should scan Excel files."""
        catalog = Catalog()
        catalog.add_folder(DATA_DIR, Folder(id="test", name="Test"), include=["*.xlsx"])
        assert len(catalog.variables) > 0

    def test_add_folder_creates_datasets(self):
        """add_folder should create Dataset entities."""
        catalog = Catalog()
        catalog.add_folder(DATA_DIR, Folder(id="test", name="Test"))
        assert (
            len(catalog.datasets) == 3
        )  # employees.csv, employees.xlsx, regions_france.csv

    def test_add_folder_assigns_folder_id(self):
        """add_folder should assign folder_id to datasets."""
        catalog = Catalog()
        catalog.add_folder(DATA_DIR, Folder(id="mydata", name="My Data"))
        # All datasets should have folder_id starting with root folder ID
        assert all(
            ds.folder_id is not None
            and (ds.folder_id == "mydata" or ds.folder_id.startswith("mydata---"))
            for ds in catalog.datasets
        )

    def test_add_folder_prefixes_ids(self):
        """add_folder should prefix IDs with folder ID."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="src", name="Source"), include=["employees.csv"]
        )
        assert catalog.datasets[0].id == "src---employees_csv"
        assert catalog.variables[0].id.startswith("src---employees_csv---")

    def test_add_folder_infers_stats(self):
        """add_folder should compute stats by default."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["employees.csv"]
        )
        assert all(v.nb_distinct is not None for v in catalog.variables)
        assert all(v.nb_missing is not None for v in catalog.variables)

    def test_add_folder_without_stats(self):
        """add_folder with infer_stats=False should skip stats."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR,
            Folder(id="test", name="Test"),
            include=["employees.csv"],
            infer_stats=False,
        )
        assert all(v.nb_distinct is None for v in catalog.variables)
        assert all(v.nb_missing is None for v in catalog.variables)

    def test_add_folder_not_found(self):
        """add_folder should raise FileNotFoundError for missing path."""
        catalog = Catalog()
        with pytest.raises(FileNotFoundError):
            catalog.add_folder("/nonexistent/path")

    def test_add_folder_default_folder(self):
        """add_folder without folder arg should use directory name."""
        catalog = Catalog()
        catalog.add_folder(DATA_DIR, include=["employees.csv"])
        assert catalog.folders[0].id == "data"
        assert catalog.folders[0].name == "data"


class TestSubfolders:
    """Test recursive subfolder scanning."""

    def test_add_folder_scans_subdirs(self, tmp_path: Path):
        """add_folder should scan files in subdirectories."""
        # Create nested structure
        subdir = tmp_path / "2024" / "january"
        subdir.mkdir(parents=True)
        (subdir / "sales.csv").write_text("amount,qty\n100,5\n200,10")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="src", name="Source"))

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].id == "src---2024---january---sales_csv"

    def test_add_folder_creates_subfolders(self, tmp_path: Path):
        """add_folder should create Folder entities for subdirectories."""
        # Create nested structure
        (tmp_path / "2024").mkdir()
        (tmp_path / "2024" / "data.csv").write_text("x\n1")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="root", name="Root"))

        # +1 for _modalities folder (auto-created)
        user_folders = [f for f in catalog.folders if f.id != "_modalities"]
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

        # Should have: x, x---a, x---a---b, x---a---b---c (+1 for _modalities)
        user_folders = [f for f in catalog.folders if f.id != "_modalities"]
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

        folders_by_id = {f.id: f for f in catalog.folders}
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

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].name == "root"


class TestIdSanitization:
    """Test ID generation with special characters."""

    def test_sanitize_spaces_in_filename(self, tmp_path: Path):
        """Spaces in filenames should be preserved in IDs."""
        (tmp_path / "my file.csv").write_text("x\n1")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="src", name="Source"))

        assert catalog.datasets[0].id == "src---my file_csv"

    def test_sanitize_special_chars(self, tmp_path: Path):
        """Special characters should be replaced with underscore."""
        (tmp_path / "data@2024#v1.csv").write_text("x\n1")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="src", name="Source"))

        assert catalog.datasets[0].id == "src---data_2024_v1_csv"

    def test_sanitize_folder_name_with_spaces(self, tmp_path: Path):
        """Folder names with spaces should be handled."""
        subdir = tmp_path / "Year 2024"
        subdir.mkdir()
        (subdir / "data.csv").write_text("x\n1")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="root", name="Root"))

        assert "root---Year 2024" in [f.id for f in catalog.folders]

    def test_sanitize_variable_name(self, tmp_path: Path):
        """Variable names with special chars should be sanitized."""
        (tmp_path / "data.csv").write_text("col@name,col#2\n1,2")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="src", name="Source"))

        var_ids = [v.id for v in catalog.variables]
        assert "src---data_csv---col_name" in var_ids
        assert "src---data_csv---col_2" in var_ids


class TestIncludeExclude:
    """Test include/exclude glob patterns."""

    def test_include_single_pattern(self, tmp_path: Path):
        """include should filter to matching files only."""
        (tmp_path / "data.csv").write_text("x\n1")
        (tmp_path / "data.xlsx").write_bytes(b"")  # Empty xlsx won't parse

        catalog = Catalog()
        catalog.add_folder(tmp_path, include=["*.csv"])

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].name == "data"

    def test_include_multiple_patterns(self, tmp_path: Path):
        """include should accept multiple patterns."""
        (tmp_path / "a.csv").write_text("x\n1")
        (tmp_path / "b.csv").write_text("y\n2")
        (tmp_path / "c.txt").write_text("ignored")

        catalog = Catalog()
        catalog.add_folder(tmp_path, include=["a.csv", "b.csv"])

        assert len(catalog.datasets) == 2

    def test_exclude_pattern(self, tmp_path: Path):
        """exclude should filter out matching files."""
        (tmp_path / "keep.csv").write_text("x\n1")
        (tmp_path / "skip.csv").write_text("y\n2")

        catalog = Catalog()
        catalog.add_folder(tmp_path, exclude=["skip.csv"])

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].name == "keep"

    def test_exclude_subdirectory(self, tmp_path: Path):
        """exclude should filter out subdirectories."""
        (tmp_path / "data.csv").write_text("x\n1")
        archive = tmp_path / "archive"
        archive.mkdir()
        (archive / "old.csv").write_text("y\n2")

        catalog = Catalog()
        catalog.add_folder(tmp_path, exclude=["archive"])

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].name == "data"

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

        assert len(catalog.datasets) == 2
        names = {d.name for d in catalog.datasets}
        assert names == {"data", "keep"}

    def test_exclude_glob_pattern(self, tmp_path: Path):
        """exclude should support glob patterns."""
        (tmp_path / "data.csv").write_text("x\n1")
        (tmp_path / "backup.csv").write_text("y\n2")
        (tmp_path / "report.txt").write_text("ignored")

        catalog = Catalog()
        catalog.add_folder(tmp_path, exclude=["backup.*"])

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].name == "data"


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_folder(self, tmp_path: Path):
        """Empty folder should create no datasets."""
        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="empty", name="Empty"))

        assert len(catalog.folders) == 1
        assert len(catalog.datasets) == 0
        assert len(catalog.variables) == 0

    def test_empty_csv_file(self, tmp_path: Path):
        """Empty CSV should create dataset with no variables."""
        (tmp_path / "empty.csv").write_text("")

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        assert len(catalog.datasets) == 1
        assert len(catalog.variables) == 0

    def test_csv_headers_only(self, tmp_path: Path):
        """CSV with headers but no data should create variables."""
        (tmp_path / "headers.csv").write_text("col_a,col_b,col_c\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        assert len(catalog.datasets) == 1
        assert len(catalog.variables) == 3

    def test_not_a_directory(self, tmp_path: Path):
        """add_folder should raise for file path."""
        file = tmp_path / "file.csv"
        file.write_text("x\n1")

        catalog = Catalog()
        with pytest.raises(NotADirectoryError):
            catalog.add_folder(file)

    def test_unsupported_file_extension(self, tmp_path: Path):
        """Unsupported files should be ignored."""
        (tmp_path / "data.csv").write_text("x\n1")
        (tmp_path / "readme.txt").write_text("ignored")
        (tmp_path / "config.json").write_text("{}")

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        assert len(catalog.datasets) == 1
