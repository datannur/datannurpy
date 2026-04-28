"""Tests for incremental scan functionality (files)."""

from __future__ import annotations

import json
import os
from pathlib import Path

from datannurpy import Catalog, Folder
from datannurpy.finalize import remove_dataset_cascade


class TestIncrementalScanFiles:
    """Test incremental scan for files."""

    def test_unchanged_file_is_skipped(self, tmp_path: Path):
        """Unchanged file should be skipped on second scan."""
        app_dir = tmp_path
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        csv_file = data_dir / "test.csv"
        csv_file.write_text("a,b\n1,2\n3,4\n")

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog1.export_db()

        assert len(catalog1.dataset.all()) == 1
        assert len(catalog1.variable.all()) == 2

        # Second scan (same file, same mtime)
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        initial_datasets = len(catalog2.dataset.all())
        catalog2.add_folder(data_dir, Folder(id="src", name="Source"))

        # Should still have same number of datasets (unchanged, skipped)
        assert len(catalog2.dataset.all()) == initial_datasets
        # Dataset should be marked as seen
        ds = catalog2.dataset.all()[0]
        assert getattr(ds, "_seen", False) is True

    def test_unchanged_file_with_dataset_metadata_is_skipped(self, tmp_path: Path):
        """dataset.csv metadata should not break incremental matching on reload."""
        app_dir = tmp_path / "catalog"
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        csv_file = data_dir / "test.csv"
        csv_file.write_text("a,b\n1,2\n3,4\n")

        meta_dir = tmp_path / "metadata"
        meta_dir.mkdir()
        (meta_dir / "dataset.csv").write_text(
            "id,name,folder_id,data_path\nsrc---test_csv,Test,src,../data/test.csv\n"
        )

        catalog1 = Catalog(app_path=app_dir, metadata_path=meta_dir, quiet=True)
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog1.export_db()

        catalog2 = Catalog(app_path=app_dir, metadata_path=meta_dir, quiet=True)
        ds_before = catalog2.dataset.get_by("id", "src---test_csv")
        assert ds_before is not None
        assert ds_before._match_path == str(csv_file)

        catalog2.add_folder(data_dir, Folder(id="src", name="Source"))

        assert len(catalog2.dataset.all()) == 1
        ds_after = catalog2.dataset.get_by("id", "src---test_csv")
        assert ds_after is not None
        assert getattr(ds_after, "_seen", False) is True

    def test_modified_file_is_rescanned(self, tmp_path: Path):
        """Modified file should be rescanned."""
        app_dir = tmp_path
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        csv_file = data_dir / "test.csv"
        csv_file.write_text("a,b\n1,2\n")

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog1.export_db()

        first_timestamp = catalog1.dataset.all()[0].last_update_timestamp
        assert first_timestamp is not None

        # Modify file with a future timestamp to ensure mtime changes
        csv_file.write_text("a,b,c\n1,2,3\n4,5,6\n7,8,9\n")
        # Force mtime to be different (add 10 seconds)
        new_mtime = first_timestamp + 10
        os.utime(csv_file, (new_mtime, new_mtime))

        # Second scan
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_folder(data_dir, Folder(id="src", name="Source"))

        # Should have rescanned with new data
        assert len(catalog2.dataset.all()) == 1
        ds = catalog2.dataset.all()[0]
        assert ds.last_update_timestamp != first_timestamp
        assert ds.nb_row == 3  # new row count
        assert len([v for v in catalog2.variable.all() if v.dataset_id == ds.id]) == 3

    def test_new_file_is_added(self, tmp_path: Path):
        """New file should be added on second scan."""
        app_dir = tmp_path
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "file1.csv").write_text("a,b\n1,2\n")

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog1.export_db()

        assert len(catalog1.dataset.all()) == 1

        # Add new file
        (data_dir / "file2.csv").write_text("x,y,z\n1,2,3\n")

        # Second scan
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_folder(data_dir, Folder(id="src", name="Source"))

        # Should have both datasets
        assert len(catalog2.dataset.all()) == 2

    def test_refresh_true_forces_rescan(self, tmp_path: Path):
        """refresh=True should force rescan even if unchanged."""
        app_dir = tmp_path
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        csv_file = data_dir / "test.csv"
        csv_file.write_text("a,b\n1,2\n")

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog1.export_db()

        # Second scan with refresh=True (global)
        catalog2 = Catalog(app_path=app_dir, refresh=True, quiet=True)
        catalog2.add_folder(data_dir, Folder(id="src", name="Source"))

        # Should have rescanned (dataset recreated)
        assert len(catalog2.dataset.all()) == 1
        ds = catalog2.dataset.all()[0]
        assert getattr(ds, "_seen", False) is True

    def test_refresh_override_per_method(self, tmp_path: Path):
        """refresh parameter can be overridden per method."""
        app_dir = tmp_path
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        csv_file = data_dir / "test.csv"
        csv_file.write_text("a,b\n1,2\n")

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog1.export_db()

        # Second scan with refresh=False global but refresh=True on method
        catalog2 = Catalog(app_path=app_dir, refresh=False, quiet=True)
        catalog2.add_folder(data_dir, Folder(id="src", name="Source"), refresh=True)

        # Should have rescanned
        assert len(catalog2.dataset.all()) == 1


class TestIncrementalScanAddDataset:
    """Test incremental scan for add_dataset."""

    def test_add_dataset_unchanged_skipped(self, tmp_path: Path):
        """add_dataset should skip unchanged file."""
        app_dir = tmp_path
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b\n1,2\n")

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_dataset(csv_file)
        catalog1.export_db()

        # Second scan
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_dataset(csv_file)

        # Should be skipped
        assert len(catalog2.dataset.all()) == 1
        ds = catalog2.dataset.all()[0]
        assert getattr(ds, "_seen", False) is True

    def test_add_dataset_modified_rescanned(self, tmp_path: Path):
        """add_dataset should rescan modified file."""
        app_dir = tmp_path
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b\n1,2\n")

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_dataset(csv_file)
        catalog1.export_db()

        first_timestamp = catalog1.dataset.all()[0].last_update_timestamp
        assert first_timestamp is not None

        # Modify file with a future timestamp
        csv_file.write_text("a,b,c\n1,2,3\n4,5,6\n")
        new_mtime = first_timestamp + 10
        os.utime(csv_file, (new_mtime, new_mtime))

        # Second scan
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_dataset(csv_file)

        # Should have rescanned
        assert len(catalog2.dataset.all()) == 1
        assert catalog2.dataset.all()[0].nb_row == 2
        assert len(catalog2.variable.all()) == 3


class TestRemoveDatasetCascade:
    """Test remove_dataset_cascade functionality."""

    def test_removes_variables(self, tmp_path: Path):
        """remove_dataset_cascade should remove dataset's variables."""
        catalog = Catalog(quiet=True)
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b\n1,2\n")
        catalog.add_dataset(csv_file)

        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 2

        # Remove dataset cascade
        remove_dataset_cascade(catalog, catalog.dataset.all()[0])

        assert len(catalog.dataset.all()) == 0
        assert len(catalog.variable.all()) == 0

    def test_removes_frequencies(self, tmp_path: Path):
        """remove_dataset_cascade should remove dataset's frequencies."""
        catalog = Catalog(quiet=True)
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("color\nred\nblue\nred\n")
        catalog.add_dataset(csv_file)

        assert len(catalog.frequency.all()) > 0

        # Remove dataset cascade
        remove_dataset_cascade(catalog, catalog.dataset.all()[0])

        # Frequency table should be empty
        assert len(catalog.frequency.all()) == 0


class TestLastUpdateTimestamp:
    """Test last_update_timestamp field."""

    def test_timestamp_set_on_scan(self, tmp_path: Path):
        """last_update_timestamp should be set when scanning."""
        catalog = Catalog(quiet=True)
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b\n1,2\n")
        catalog.add_dataset(csv_file)

        ds = catalog.dataset.all()[0]
        assert ds.last_update_timestamp is not None
        assert ds.last_update_timestamp > 0

        # Should match file mtime
        expected_mtime = int(csv_file.stat().st_mtime)
        assert ds.last_update_timestamp == expected_mtime

    def test_timestamp_exported_to_json(self, tmp_path: Path):
        """last_update_timestamp should be exported to JSON."""
        app_dir = tmp_path
        db_dir = app_dir / "data" / "db"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b\n1,2\n")

        catalog = Catalog(app_path=app_dir, quiet=True)
        catalog.add_dataset(csv_file)
        catalog.export_db()

        with open(db_dir / "dataset.json") as f:
            data = json.load(f)

        assert "last_update_timestamp" in data[0]
        assert data[0]["last_update_timestamp"] > 0


class TestLogSkip:
    """Test log_skip output."""

    def test_log_skip_prints_when_not_quiet(self, tmp_path: Path, capsys):
        """log_skip should print when quiet=False."""
        from datannurpy.utils.log import log_skip

        log_skip("test.csv", quiet=False)

        captured = capsys.readouterr()
        assert "test.csv" in captured.err
        assert "unchanged" in captured.err


class TestIncrementalParquetDirectory:
    """Test incremental scan for Parquet directories (Delta/Hive)."""

    def test_parquet_directory_unchanged_skipped(self, tmp_path: Path):
        """Unchanged Parquet directory should be skipped."""
        app_dir = tmp_path
        data_dir = tmp_path / "data"

        # Create a Hive-partitioned dataset
        part_dir = data_dir / "partitioned"
        (part_dir / "year=2024").mkdir(parents=True)
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.table({"a": [1, 2], "b": [3, 4]})
        pq.write_table(table, part_dir / "year=2024" / "data.parquet")

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_dataset(part_dir)
        catalog1.export_db()

        assert len(catalog1.dataset.all()) == 1

        # Second scan (unchanged)
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_dataset(part_dir)

        # Should be skipped
        assert len(catalog2.dataset.all()) == 1
        assert getattr(catalog2.dataset.all()[0], "_seen", False) is True

    def test_parquet_directory_modified_rescanned(self, tmp_path: Path):
        """Modified Parquet directory should be rescanned."""
        app_dir = tmp_path
        data_dir = tmp_path / "data"

        # Create a Hive-partitioned dataset
        part_dir = data_dir / "partitioned"
        (part_dir / "year=2024").mkdir(parents=True)
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.table({"a": [1, 2], "b": [3, 4]})
        pq.write_table(table, part_dir / "year=2024" / "data.parquet")

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_dataset(part_dir)
        catalog1.export_db()

        first_timestamp = catalog1.dataset.all()[0].last_update_timestamp
        assert first_timestamp is not None

        # Modify directory mtime
        new_mtime = first_timestamp + 10
        os.utime(part_dir, (new_mtime, new_mtime))

        # Second scan
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_dataset(part_dir)

        # Should have rescanned
        assert len(catalog2.dataset.all()) == 1
        assert catalog2.dataset.all()[0].last_update_timestamp == new_mtime


class TestRemoveDatasetCascadeWithMultipleDatasets:
    """Test remove_dataset_cascade with multiple datasets."""

    def test_removes_only_target_frequencies(self, tmp_path: Path):
        """remove_dataset_cascade should only remove target dataset's frequencies."""
        catalog = Catalog(quiet=True)

        # Create two CSV files with frequencies
        csv1 = tmp_path / "file1.csv"
        csv1.write_text("color\nred\nblue\nred\n")
        csv2 = tmp_path / "file2.csv"
        csv2.write_text("status\nactive\ninactive\nactive\n")

        catalog.add_dataset(csv1)
        catalog.add_dataset(csv2)

        assert len(catalog.dataset.all()) == 2
        total_before = len(catalog.frequency.all())
        assert total_before > 0

        # Remove first dataset
        remove_dataset_cascade(catalog, catalog.dataset.all()[0])

        # Should still have second dataset's frequencies
        assert len(catalog.dataset.all()) == 1
        total_after = len(catalog.frequency.all())
        assert total_after > 0
        assert total_after < total_before

    def test_removes_all_frequencies_when_single_dataset(self, tmp_path: Path):
        """remove_dataset_cascade drops the frequency table when all rows are removed."""
        catalog = Catalog(quiet=True)

        # Create a single CSV with frequencies
        csv1 = tmp_path / "file1.csv"
        csv1.write_text("color\nred\nblue\n")

        catalog.add_dataset(csv1)

        assert len(catalog.dataset.all()) == 1
        assert len(catalog.frequency.all()) > 0

        # Remove the only dataset
        remove_dataset_cascade(catalog, catalog.dataset.all()[0])

        # All frequencies should be removed
        assert len(catalog.dataset.all()) == 0
        assert len(catalog.frequency.all()) == 0

    def test_removes_dataset_without_frequency_tables(self, tmp_path: Path):
        """remove_dataset_cascade handles dataset without frequencies."""
        from datannurpy.schema import Dataset

        catalog = Catalog(quiet=True)

        # Manually add a dataset without scanning (no frequencies)
        ds = Dataset(id="test", name="Test")
        ds.data_path = str(tmp_path / "nonexistent.csv")
        catalog.dataset.add(ds)

        assert len(catalog.dataset.all()) == 1
        assert len(catalog.frequency.all()) == 0

        # Remove it
        remove_dataset_cascade(catalog, ds)

        assert len(catalog.dataset.all()) == 0

    def test_removes_dataset_without_data_path(self):
        """remove_dataset_cascade handles dataset without data_path."""
        from datannurpy.schema import Dataset

        catalog = Catalog(quiet=True)

        # Manually add a dataset without data_path
        ds = Dataset(id="test", name="Test")
        catalog.dataset.add(ds)

        assert len(catalog.dataset.all()) == 1

        # Remove it (should not crash)
        remove_dataset_cascade(catalog, ds)

        assert len(catalog.dataset.all()) == 0


class TestIncrementalFolderWithParquet:
    """Test incremental scan for folders containing Parquet datasets."""

    def test_parquet_in_folder_unchanged_skipped(self, tmp_path: Path):
        """Unchanged Parquet dataset in folder should be skipped."""
        app_dir = tmp_path
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create a Hive-partitioned dataset inside the folder
        part_dir = data_dir / "partitioned"
        (part_dir / "year=2024").mkdir(parents=True)
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.table({"a": [1, 2], "b": [3, 4]})
        pq.write_table(table, part_dir / "year=2024" / "data.parquet")

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog1.export_db()

        assert len(catalog1.dataset.all()) == 1

        # Second scan (unchanged)
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_folder(data_dir, Folder(id="src", name="Source"))

        # Should be skipped
        assert len(catalog2.dataset.all()) == 1
        assert getattr(catalog2.dataset.all()[0], "_seen", False) is True

    def test_parquet_in_folder_modified_rescanned(self, tmp_path: Path):
        """Modified Parquet dataset in folder should be rescanned."""
        app_dir = tmp_path
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create a Hive-partitioned dataset inside the folder
        part_dir = data_dir / "partitioned"
        (part_dir / "year=2024").mkdir(parents=True)
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.table({"a": [1, 2], "b": [3, 4]})
        pq.write_table(table, part_dir / "year=2024" / "data.parquet")

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog1.export_db()

        first_timestamp = catalog1.dataset.all()[0].last_update_timestamp
        assert first_timestamp is not None

        # Modify directory mtime
        new_mtime = first_timestamp + 10
        os.utime(part_dir, (new_mtime, new_mtime))

        # Second scan
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_folder(data_dir, Folder(id="src", name="Source"))

        # Should have rescanned
        assert len(catalog2.dataset.all()) == 1
        assert catalog2.dataset.all()[0].last_update_timestamp == new_mtime
