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

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].delivery_format == "parquet"
        assert len(catalog.variables) == 3

    def test_add_dataset_scans_file(self):
        """add_dataset should scan a single file."""
        catalog = Catalog()
        catalog.add_dataset(CSV_DIR / "employees.csv")

        assert len(catalog.datasets) == 1
        assert len(catalog.variables) == 9

    def test_add_dataset_with_folder(self):
        """add_dataset with folder should create folder and link."""
        catalog = Catalog()
        catalog.add_dataset(
            CSV_DIR / "employees.csv",
            folder=Folder(id="hr", name="HR Data"),
        )

        assert len([f for f in catalog.folders if f.id != "_modalities"]) == 1
        assert catalog.folders[0].id == "hr"
        assert catalog.datasets[0].folder_id == "hr"
        assert catalog.datasets[0].id == "hr---employees"

    def test_add_dataset_with_folder_id(self):
        """add_dataset with folder_id should link to existing folder."""
        catalog = Catalog()
        catalog.add_folder(CSV_DIR, Folder(id="data", name="Data"), include=[])
        catalog.add_dataset(CSV_DIR / "employees.csv", folder_id="data")

        assert catalog.datasets[0].folder_id == "data"

    def test_add_dataset_reuses_folder(self):
        """add_dataset should not duplicate folder."""
        catalog = Catalog()
        folder = Folder(id="src", name="Source")
        catalog.add_dataset(CSV_DIR / "employees.csv", folder=folder)
        catalog.add_dataset(CSV_DIR / "regions_france.csv", folder=folder)

        assert len([f for f in catalog.folders if f.id != "_modalities"]) == 1
        assert len(catalog.datasets) == 2

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

        ds = catalog.datasets[0]
        assert ds.name == "Employés"
        assert ds.description == "Liste des employés"
        assert ds.type == "référentiel"
        assert ds.link == "https://example.com"
        assert ds.start_date == "2020/01/01"

    def test_add_dataset_standalone_id(self):
        """add_dataset without folder should use filename as ID."""
        catalog = Catalog()
        catalog.add_dataset(CSV_DIR / "employees.csv")

        assert catalog.datasets[0].id == "employees"

    def test_add_dataset_inherits_file_description(self):
        """add_dataset should use file metadata description when available."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "cars.sas7bdat")

        assert catalog.datasets[0].description == "Written by SAS"

    def test_add_dataset_explicit_description_not_overwritten(self):
        """add_dataset should keep explicit description over file metadata."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "cars.sas7bdat", description="Custom desc")

        assert catalog.datasets[0].description == "Custom desc"

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

        assert len(catalog.datasets) == 1
        ds = catalog.datasets[0]
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

        ds = catalog.datasets[0]
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

        assert len(catalog.datasets) == 1
        ds = catalog.datasets[0]
        assert ds.id == "test_partitioned"
        assert ds.delivery_format == "parquet"
        assert ds.nb_row == 6


class TestAddDatasetIceberg:
    """Test add_dataset with Iceberg table directories."""

    def test_add_dataset_iceberg_directory(self):
        """add_dataset should scan an Iceberg table directory."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "iceberg_warehouse" / "default" / "test_table")

        assert len(catalog.datasets) == 1
        ds = catalog.datasets[0]
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
