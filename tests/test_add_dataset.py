"""Tests for Catalog.add_dataset method."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from datannurpy import Catalog, Folder
from datannurpy.errors import ConfigError

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

    def test_add_dataset_exports_effective_sample_size_for_csv(self, tmp_path: Path):
        """add_dataset should persist effective sample_size for sampled CSV scans."""
        csv_file = tmp_path / "big.csv"
        lines = ["id,value\n"] + [f"{i},{i * 10}\n" for i in range(250)]
        csv_file.write_text("".join(lines))

        catalog = Catalog(quiet=True)
        catalog.add_dataset(csv_file, sample_size=100)

        dataset = catalog.dataset.all()[0]
        assert dataset.nb_row == 250
        assert dataset.sample_size == 100

        out_dir = tmp_path / "out"
        catalog.export_db(out_dir, quiet=True)
        exported = json.loads((out_dir / "dataset.json").read_text())
        assert exported[0]["sample_size"] == 100

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
        with pytest.raises(ConfigError):
            catalog.add_dataset("/nonexistent/file.csv")

    def test_add_dataset_unsupported_format(self, tmp_path: Path):
        """add_dataset should raise for unsupported formats."""
        (tmp_path / "data.json").write_text("{}")

        catalog = Catalog()
        with pytest.raises(ConfigError, match="Unsupported format"):
            catalog.add_dataset(tmp_path / "data.json")

    def test_add_dataset_folder_and_folder_id_error(self):
        """add_dataset should raise if both folder and folder_id given."""
        catalog = Catalog()
        with pytest.raises(ConfigError, match="Cannot specify both"):
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

    def test_add_dataset_hive_directory_persists_sample_size(self, tmp_path: Path):
        """add_dataset should pass sample_size through Hive partitioned scans."""
        hive_dir = tmp_path / "sales"
        (hive_dir / "year=2024").mkdir(parents=True)
        table = pa.table({"id": list(range(250)), "value": list(range(250))})
        pq.write_table(table, hive_dir / "year=2024" / "part-0.parquet")

        catalog = Catalog(quiet=True)
        catalog.add_dataset(hive_dir, sample_size=100)

        dataset = catalog.dataset.all()[0]
        assert dataset.nb_row == 250
        assert dataset.sample_size == 100


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
        with pytest.raises(ConfigError, match="not a recognized Parquet format"):
            catalog.add_dataset(tmp_path / "subdir")


class TestAddDatasetDepth:
    """Test add_dataset with depth parameter."""

    def test_add_dataset_dataset_depth_file(self):
        """depth=dataset should create dataset without scanning."""
        catalog = Catalog()
        catalog.add_dataset(CSV_DIR / "employees.csv", depth="dataset")

        assert len(catalog.dataset.all()) == 1
        ds = catalog.dataset.all()[0]
        assert ds.nb_row is None
        assert len(catalog.variable.all()) == 0

    def test_add_dataset_schema_file(self):
        """depth=variable should scan schema but skip stats."""
        catalog = Catalog()
        catalog.add_dataset(CSV_DIR / "employees.csv", depth="variable")

        assert len(catalog.dataset.all()) == 1
        ds = catalog.dataset.all()[0]
        # Schema mode doesn't read data, so nb_row is None
        assert ds.nb_row is None
        assert len(catalog.variable.all()) == 9
        # Schema mode skips modalities
        assert len(catalog.modality.all()) == 0

    def test_add_dataset_dataset_depth_delta(self):
        """depth=dataset should create dataset without scanning Delta."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "test_delta", depth="dataset")

        assert len(catalog.dataset.all()) == 1
        ds = catalog.dataset.all()[0]
        assert ds.delivery_format == "delta"
        assert ds.nb_row is None
        assert len(catalog.variable.all()) == 0

    def test_add_dataset_schema_delta(self):
        """depth=variable should scan Delta schema but skip stats."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "test_delta", depth="variable")

        assert len(catalog.dataset.all()) == 1
        ds = catalog.dataset.all()[0]
        assert ds.nb_row is not None
        assert len(catalog.variable.all()) > 0
        assert len(catalog.modality.all()) == 0

    def test_add_dataset_stat_file(self):
        """depth=stat should compute stats but skip modalities."""
        catalog = Catalog(freq_threshold=10)
        catalog.add_dataset(CSV_DIR / "employees.csv", depth="stat")

        assert len(catalog.dataset.all()) == 1
        ds = catalog.dataset.all()[0]
        assert ds.nb_row is not None
        assert len(catalog.variable.all()) > 0
        # Stats computed
        assert any(v.nb_distinct is not None for v in catalog.variable.all())
        # No modalities
        assert len(catalog.modality.all()) == 0
        assert catalog.frequency.is_empty

    def test_add_dataset_stat_delta(self):
        """depth=stat should compute Delta stats but skip modalities."""
        catalog = Catalog(freq_threshold=10)
        catalog.add_dataset(DATA_DIR / "test_delta", depth="stat")

        assert len(catalog.dataset.all()) == 1
        ds = catalog.dataset.all()[0]
        assert ds.nb_row is not None
        assert len(catalog.variable.all()) > 0
        assert len(catalog.modality.all()) == 0

    def test_add_dataset_inherits_catalog_depth(self):
        """add_dataset should use Catalog.depth when not overridden."""
        catalog = Catalog(depth="dataset")
        catalog.add_dataset(CSV_DIR / "employees.csv")

        assert len(catalog.variable.all()) == 0

    def test_add_dataset_schema_empty_csv(self, tmp_path: Path):
        """depth=variable should handle empty CSV gracefully."""
        # Create CSV with only header
        (tmp_path / "empty.csv").write_text("a,b,c\n")

        catalog = Catalog()
        catalog.add_dataset(tmp_path / "empty.csv", depth="variable")

        # Should create dataset with variables (schema only, no type inference)
        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 3
        assert catalog.dataset.all()[0].nb_row is None  # No row count in schema mode


class TestRemoteStorage:
    """Test remote storage URL handling."""

    def test_remote_url_requires_provider_package(self):
        """add_dataset should propagate ImportError from missing provider."""
        from unittest.mock import patch

        catalog = Catalog()
        with (
            patch(
                "datannurpy.add_dataset.FileSystem",
                side_effect=ImportError("Install s3fs to access S3"),
            ),
            pytest.raises(ImportError, match="s3fs"),
        ):
            catalog.add_dataset("s3://bucket/data.csv")

    def test_remote_url_with_connection_error(self):
        """add_dataset should propagate connection errors from remote storage."""
        from unittest.mock import patch

        catalog = Catalog()
        with (
            patch(
                "datannurpy.add_dataset.FileSystem",
                side_effect=OSError("Connection refused"),
            ),
            pytest.raises(OSError, match="Connection refused"),
        ):
            catalog.add_dataset("sftp://host/data.csv", storage_options={"timeout": 1})

    def test_remote_file_not_found(self, tmp_path: Path):
        """add_dataset should raise FileNotFoundError for non-existent remote file."""
        from unittest.mock import patch, MagicMock

        mock_fs = MagicMock()
        mock_fs.root = "memory://test/data.csv"
        mock_fs.exists.return_value = False

        with patch("datannurpy.add_dataset.FileSystem", return_value=mock_fs):
            catalog = Catalog()
            with pytest.raises(ConfigError, match="Path not found"):
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


class TestListPath:
    """Test add_dataset with a list of paths."""

    def test_add_dataset_list_of_paths(self, tmp_path: Path):
        """add_dataset with a list scans each file."""
        (tmp_path / "a.csv").write_text("x\n1")
        (tmp_path / "b.csv").write_text("y\n2")

        catalog = Catalog(quiet=True)
        catalog.add_dataset([tmp_path / "a.csv", tmp_path / "b.csv"])

        ids = {d.id for d in catalog.dataset.all()}
        assert "a" in ids
        assert "b" in ids

    def test_add_dataset_list_shared_folder(self, tmp_path: Path):
        """Shared folder is applied to all paths."""
        (tmp_path / "a.csv").write_text("x\n1")
        (tmp_path / "b.csv").write_text("y\n2")

        catalog = Catalog(quiet=True)
        catalog.add_dataset(
            [tmp_path / "a.csv", tmp_path / "b.csv"],
            folder=Folder(id="src", name="Source"),
        )

        ids = {d.id for d in catalog.dataset.all()}
        assert "src---a" in ids
        assert "src---b" in ids
