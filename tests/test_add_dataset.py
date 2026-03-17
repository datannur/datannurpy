"""Tests for Catalog.add_dataset method."""

from __future__ import annotations

from pathlib import Path

import pytest

from datannurpy import Catalog, Folder

DATA_DIR = Path(__file__).parent.parent / "data"
CSV_DIR = DATA_DIR / "csv"


class TestAddDataset:
    """Test Catalog.add_dataset method."""

    def test_add_dataset_scans_parquet_file(self):
        """add_dataset should scan a single parquet file via scan_file."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "test.pq")

        assert len(catalog.dataset.all()) == 1
        assert catalog.dataset.all()[0].delivery_format == "parquet"
        assert len(catalog.variable.all()) == 3

    def test_add_dataset_scans_file(self):
        """add_dataset should scan a single file."""
        catalog = Catalog()
        catalog.add_dataset(CSV_DIR / "employees.csv")

        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 9

    def test_add_dataset_with_folder(self):
        """add_dataset with folder should create folder and link."""
        catalog = Catalog()
        catalog.add_dataset(
            CSV_DIR / "employees.csv",
            folder=Folder(id="hr", name="HR Data"),
        )

        assert len(catalog.folder.where("id", "!=", "_modalities")) == 1
        assert catalog.folder.all()[0].id == "hr"
        assert catalog.dataset.all()[0].folder_id == "hr"
        assert catalog.dataset.all()[0].id == "hr---employees"

    def test_add_dataset_with_folder_id(self):
        """add_dataset with folder_id should link to existing folder."""
        catalog = Catalog()
        catalog.add_folder(CSV_DIR, Folder(id="data", name="Data"), include=[])
        catalog.add_dataset(CSV_DIR / "employees.csv", folder_id="data")

        assert catalog.dataset.all()[0].folder_id == "data"

    def test_add_dataset_reuses_folder(self):
        """add_dataset should not duplicate folder."""
        catalog = Catalog()
        folder = Folder(id="src", name="Source")
        catalog.add_dataset(CSV_DIR / "employees.csv", folder=folder)
        catalog.add_dataset(CSV_DIR / "regions_france.csv", folder=folder)

        assert len(catalog.folder.where("id", "!=", "_modalities")) == 1
        assert len(catalog.dataset.all()) == 2

    def test_add_dataset_with_metadata(self):
        """add_dataset should accept metadata overrides."""
        catalog = Catalog()
        catalog.add_dataset(
            CSV_DIR / "employees.csv",
            name="Employés",
            description="Liste des employés",
            type="référentiel",
            link="https://example.com",
            start_date="2020/01/01",
        )

        ds = catalog.dataset.all()[0]
        assert ds.name == "Employés"
        assert ds.description == "Liste des employés"
        assert ds.type == "référentiel"
        assert ds.link == "https://example.com"
        assert ds.start_date == "2020/01/01"

    def test_add_dataset_standalone_id(self):
        """add_dataset without folder should use filename as ID."""
        catalog = Catalog()
        catalog.add_dataset(CSV_DIR / "employees.csv")

        assert catalog.dataset.all()[0].id == "employees"

    def test_add_dataset_inherits_file_description(self):
        """add_dataset should use file metadata description when available."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "cars.sas7bdat")

        assert catalog.dataset.all()[0].description == "Written by SAS"

    def test_add_dataset_explicit_description_not_overwritten(self):
        """add_dataset should keep explicit description over file metadata."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "cars.sas7bdat", description="Custom desc")

        assert catalog.dataset.all()[0].description == "Custom desc"

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
                CSV_DIR / "employees.csv",
                folder=Folder(id="a", name="A"),
                folder_id="b",
            )


class TestAddDatasetDelta:
    """Test add_dataset with Delta Lake directories."""

    def test_add_dataset_delta_directory(self):
        """add_dataset should scan a Delta Lake directory."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "test_delta")

        assert len(catalog.dataset.all()) == 1
        ds = catalog.dataset.all()[0]
        assert ds.id == "test_delta"
        assert ds.delivery_format == "delta"
        assert ds.nb_row == 6
        assert ds.name == "Test Delta Table"
        assert ds.description == "A test Delta Lake table"

    def test_add_dataset_delta_with_overrides(self):
        """add_dataset on Delta should allow metadata overrides."""
        catalog = Catalog()
        catalog.add_dataset(
            DATA_DIR / "test_delta",
            name="Custom Name",
            description="Custom description",
            folder=Folder(id="sales", name="Sales"),
            quiet=True,
        )

        ds = catalog.dataset.all()[0]
        assert ds.id == "sales---test_delta"
        assert ds.name == "Custom Name"
        assert ds.description == "Custom description"
        assert ds.folder_id == "sales"


class TestAddDatasetHive:
    """Test add_dataset with Hive partitioned directories."""

    def test_add_dataset_hive_directory(self):
        """add_dataset should scan a Hive partitioned directory."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "test_partitioned")

        assert len(catalog.dataset.all()) == 1
        ds = catalog.dataset.all()[0]
        assert ds.id == "test_partitioned"
        assert ds.delivery_format == "parquet"
        assert ds.nb_row == 6


class TestAddDatasetIceberg:
    """Test add_dataset with Iceberg table directories."""

    def test_add_dataset_iceberg_directory(self):
        """add_dataset should scan an Iceberg table directory."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "iceberg_warehouse" / "default" / "test_table")

        assert len(catalog.dataset.all()) == 1
        ds = catalog.dataset.all()[0]
        assert ds.id == "test_table"
        assert ds.delivery_format == "iceberg"
        assert ds.description == "Sample Iceberg table for testing"


class TestAddDatasetUnknown:
    """Test add_dataset with unknown directory formats."""

    def test_add_dataset_unknown_directory(self, tmp_path: Path):
        """add_dataset should raise for unknown directory format."""
        (tmp_path / "subdir").mkdir()

        catalog = Catalog()
        with pytest.raises(ValueError, match="not a recognized Parquet format"):
            catalog.add_dataset(tmp_path / "subdir")


class TestAddDatasetDepth:
    """Test add_dataset with depth parameter."""

    def test_add_dataset_structure_file(self):
        """depth=structure should create dataset without scanning."""
        catalog = Catalog()
        catalog.add_dataset(CSV_DIR / "employees.csv", depth="structure")

        assert len(catalog.dataset.all()) == 1
        ds = catalog.dataset.all()[0]
        assert ds.nb_row is None
        assert len(catalog.variable.all()) == 0

    def test_add_dataset_schema_file(self):
        """depth=schema should scan schema but skip stats."""
        catalog = Catalog()
        catalog.add_dataset(CSV_DIR / "employees.csv", depth="schema")

        assert len(catalog.dataset.all()) == 1
        ds = catalog.dataset.all()[0]
        # Schema mode doesn't read data, so nb_row is None
        assert ds.nb_row is None
        assert len(catalog.variable.all()) == 9
        # Schema mode skips modalities
        assert len(catalog.modality.all()) == 0

    def test_add_dataset_structure_delta(self):
        """depth=structure should create dataset without scanning Delta."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "test_delta", depth="structure")

        assert len(catalog.dataset.all()) == 1
        ds = catalog.dataset.all()[0]
        assert ds.delivery_format == "delta"
        assert ds.nb_row is None
        assert len(catalog.variable.all()) == 0

    def test_add_dataset_schema_delta(self):
        """depth=schema should scan Delta schema but skip stats."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "test_delta", depth="schema")

        assert len(catalog.dataset.all()) == 1
        ds = catalog.dataset.all()[0]
        assert ds.nb_row is not None
        assert len(catalog.variable.all()) > 0
        assert len(catalog.modality.all()) == 0

    def test_add_dataset_inherits_catalog_depth(self):
        """add_dataset should use Catalog.depth when not overridden."""
        catalog = Catalog(depth="structure")
        catalog.add_dataset(CSV_DIR / "employees.csv")

        assert len(catalog.variable.all()) == 0

    def test_add_dataset_schema_empty_csv(self, tmp_path: Path):
        """depth=schema should handle empty CSV gracefully."""
        # Create CSV with only header
        (tmp_path / "empty.csv").write_text("a,b,c\n")

        catalog = Catalog()
        catalog.add_dataset(tmp_path / "empty.csv", depth="schema")

        # Should create dataset with variables (schema only, no type inference)
        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 3
        assert catalog.dataset.all()[0].nb_row is None  # No row count in schema mode


class TestRemoteStorage:
    """Test remote storage URL handling."""

    def test_remote_url_requires_provider_package(self):
        """add_dataset should raise ImportError when provider package is missing."""
        catalog = Catalog()
        # S3 URLs require s3fs package
        with pytest.raises(ImportError, match="s3fs"):
            catalog.add_dataset("s3://bucket/data.csv")

    def test_remote_url_with_sftp_connection_error(self):
        """add_dataset should raise connection error for unreachable SFTP."""
        catalog = Catalog()
        # Use a non-routable IP to get a quick timeout
        with pytest.raises((TimeoutError, OSError)):
            catalog.add_dataset(
                "sftp://10.255.255.1/data.csv", storage_options={"timeout": 1}
            )

    def test_remote_file_not_found(self, tmp_path: Path):
        """add_dataset should raise FileNotFoundError for non-existent remote file."""
        from unittest.mock import patch, MagicMock

        mock_fs = MagicMock()
        mock_fs.root = "memory://test/data.csv"
        mock_fs.exists.return_value = False

        with patch("datannurpy.add_dataset.FileSystem", return_value=mock_fs):
            catalog = Catalog()
            with pytest.raises(FileNotFoundError, match="Path not found"):
                catalog.add_dataset("memory://test/data.csv")

    def test_remote_file_path_resolution(self, tmp_path: Path):
        """add_dataset should resolve remote file path correctly."""
        from unittest.mock import patch, MagicMock

        # Create a local CSV for scanning
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("name,age\nAlice,30\n")

        mock_fs = MagicMock()
        mock_fs.root = "memory://bucket/data.csv"
        mock_fs.exists.return_value = True
        mock_fs.isdir.return_value = False  # it's a file
        mock_fs.is_local = False
        mock_fs.ensure_local.return_value.__enter__ = MagicMock(return_value=csv_file)
        mock_fs.ensure_local.return_value.__exit__ = MagicMock(return_value=None)
        mock_fs.info.return_value = {"mtime": 1700000000}

        with patch("datannurpy.add_dataset.FileSystem", return_value=mock_fs):
            catalog = Catalog(quiet=True)
            catalog.add_dataset("memory://bucket/data.csv")

            # Verify dataset was created with correct path
            datasets = catalog.dataset.all()
            assert len(datasets) == 1
            # Path gets normalized to single slash by pathlib
            assert datasets[0].data_path is not None
            assert "memory:" in datasets[0].data_path
            assert "bucket" in datasets[0].data_path
            assert datasets[0].delivery_format == "csv"
