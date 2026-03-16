"""Tests for Parquet scanner (simple, Delta, Hive, Iceberg)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from datannurpy import Catalog, Folder
from datannurpy.scanner.filesystem import FileSystem
from datannurpy.scanner.parquet.core import scan_parquet
from datannurpy.scanner.parquet.discovery import (
    is_delta_table,
    is_hive_partitioned,
    is_iceberg_table,
)

DATA_DIR = Path(__file__).parent.parent.parent / "data"


class TestExtractParquetMetadata:
    """Test parquet metadata extraction."""

    def test_metadata_without_description_key(self, tmp_path: Path):
        """Parquet with metadata but no description key should return None."""
        field = pa.field("col", pa.int64(), metadata={b"other_key": b"value"})
        schema = pa.schema([field], metadata={b"other_key": b"value"})
        table = pa.table({"col": [1, 2, 3]}, schema=schema)
        path = tmp_path / "no_desc.parquet"
        pq.write_table(table, path)

        _, _, _, metadata = scan_parquet(path, dataset_id="test")
        assert metadata.description is None
        assert metadata.column_descriptions is None

    def test_partial_column_descriptions(self, tmp_path: Path):
        """Parquet with description for some columns should skip others."""
        field1 = pa.field("col1", pa.int64(), metadata={b"description": b"First col"})
        field2 = pa.field("col2", pa.int64())
        schema = pa.schema([field1, field2])
        table = pa.table({"col1": [1], "col2": [2]}, schema=schema)
        path = tmp_path / "partial.parquet"
        pq.write_table(table, path)

        variables, _, _, _ = scan_parquet(path, dataset_id="test")
        var_by_name = {v.name: v for v in variables}
        assert var_by_name["col1"].description == "First col"
        assert var_by_name["col2"].description is None


class TestHivePartitionDetection:
    """Test Hive partition detection."""

    def test_with_file(self):
        """is_hive_partitioned should return False for a file path."""
        file_path = DATA_DIR / "csv" / "employees.csv"
        assert is_hive_partitioned(file_path) is False

    def test_without_parquet_files(self, tmp_path: Path):
        """is_hive_partitioned should return False if partition dir has no parquet files."""
        partition_dir = tmp_path / "year=2024"
        partition_dir.mkdir()
        (partition_dir / "data.csv").write_text("x\n1")

        assert is_hive_partitioned(tmp_path) is False


class TestScanDeltaExceptions:
    """Test scan_delta exception handling."""

    def test_deltalake_not_installed(self, monkeypatch, capsys):
        """Delta scan should warn if deltalake is not installed."""
        monkeypatch.setitem(sys.modules, "deltalake", None)

        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "test_delta", quiet=False)

        captured = capsys.readouterr()
        assert "deltalake not installed" in captured.err

    def test_deltalake_other_exception(self, monkeypatch, capsys):
        """Delta scan should warn on other deltalake errors."""

        def mock_deltatable(*args, **kwargs):
            raise RuntimeError("Some delta error")

        mock_module = MagicMock()
        mock_module.DeltaTable = mock_deltatable
        monkeypatch.setitem(sys.modules, "deltalake", mock_module)

        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "test_delta", quiet=False)

        captured = capsys.readouterr()
        assert "Failed to extract Delta table metadata" in captured.err


class TestScanIcebergExceptions:
    """Test scan_iceberg exception handling."""

    def test_pyiceberg_not_installed(self, monkeypatch, tmp_path: Path):
        """Iceberg scan should raise ImportError if pyiceberg is not installed."""
        metadata_dir = tmp_path / "metadata"
        metadata_dir.mkdir()
        (metadata_dir / "00000-abc.metadata.json").write_text("{}")

        monkeypatch.setitem(sys.modules, "pyiceberg", None)
        monkeypatch.setitem(sys.modules, "pyiceberg.table", None)

        catalog = Catalog()
        with pytest.raises(ImportError, match="PyIceberg is required"):
            catalog.add_dataset(tmp_path, quiet=True)

    def test_iceberg_no_metadata_files(self, tmp_path: Path):
        """Iceberg scan should raise FileNotFoundError if no metadata files."""
        from datannurpy.scanner.parquet.core import scan_iceberg

        metadata_dir = tmp_path / "metadata"
        metadata_dir.mkdir()
        # Create empty metadata directory (no .metadata.json files)

        with pytest.raises(FileNotFoundError, match="No Iceberg metadata files found"):
            scan_iceberg(tmp_path, "test", infer_stats=True, freq_threshold=None)


class TestParquetFormats:
    """Test scanning different Parquet formats."""

    def test_scan_delta_table(self):
        """add_folder should detect and scan Delta Lake tables."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["test_delta/**"]
        )
        assert len(catalog.dataset.all()) == 1
        assert catalog.dataset.all()[0].delivery_format == "delta"
        assert catalog.dataset.all()[0].name == "Test Delta Table"
        var_names = {v.name for v in catalog.variable.all()}
        assert var_names == {"id", "name", "age"}

    def test_extract_delta_metadata(self):
        """add_folder should extract Delta Lake metadata."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["test_delta/**"]
        )
        ds = catalog.dataset.all()[0]
        assert ds.description == "A test Delta Lake table"

    def test_scan_hive_partitioned(self):
        """add_folder should detect and scan Hive-partitioned Parquet datasets."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["test_partitioned/**"]
        )
        assert len(catalog.dataset.all()) == 1
        assert catalog.dataset.all()[0].delivery_format == "hive"
        var_names = {v.name for v in catalog.variable.all()}
        assert "year" in var_names
        assert "region" in var_names
        assert catalog.dataset.all()[0].nb_row == 6

    def test_scan_iceberg_table(self):
        """add_folder should detect and scan Iceberg tables."""
        iceberg_table_path = DATA_DIR / "iceberg_warehouse" / "default" / "test_table"
        if not iceberg_table_path.exists():
            pytest.skip("iceberg_warehouse table not found")

        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR / "iceberg_warehouse", Folder(id="test", name="Test")
        )

        iceberg_datasets = [
            d for d in catalog.dataset.all() if d.delivery_format == "iceberg"
        ]
        assert len(iceberg_datasets) == 1
        assert iceberg_datasets[0].name == "test_table"

    def test_extract_iceberg_metadata(self):
        """add_folder should extract Iceberg table and column metadata."""
        iceberg_table_path = DATA_DIR / "iceberg_warehouse" / "default" / "test_table"
        if not iceberg_table_path.exists():
            pytest.skip("iceberg_warehouse table not found")

        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR / "iceberg_warehouse", Folder(id="test", name="Test")
        )

        ds = catalog.dataset.all()[0]
        assert ds.description == "Sample Iceberg table for testing"
        var_by_name = {v.name: v for v in catalog.variable.all()}
        assert var_by_name["id"].description == "Unique identifier"


class TestDetectionWithFileSystem:
    """Test format detection functions with FileSystem parameter."""

    def test_is_delta_table_with_fs(self, tmp_path: Path) -> None:
        """is_delta_table should work with FileSystem."""
        delta_log = tmp_path / "_delta_log"
        delta_log.mkdir()
        (delta_log / "00000.json").write_text("{}")

        fs = FileSystem(tmp_path)
        assert is_delta_table(tmp_path, fs=fs) is True

    def test_is_delta_table_with_fs_not_delta(self, tmp_path: Path) -> None:
        """is_delta_table should return False for non-Delta directories."""
        fs = FileSystem(tmp_path)
        assert is_delta_table(tmp_path, fs=fs) is False

    def test_is_iceberg_table_with_fs(self, tmp_path: Path) -> None:
        """is_iceberg_table should work with FileSystem."""
        metadata = tmp_path / "metadata"
        metadata.mkdir()
        (metadata / "00000-abc.metadata.json").write_text("{}")

        fs = FileSystem(tmp_path)
        assert is_iceberg_table(tmp_path, fs=fs) is True

    def test_is_iceberg_table_with_fs_not_iceberg(self, tmp_path: Path) -> None:
        """is_iceberg_table should return False for non-Iceberg directories."""
        fs = FileSystem(tmp_path)
        assert is_iceberg_table(tmp_path, fs=fs) is False

    def test_is_iceberg_table_with_fs_no_metadata_dir(self, tmp_path: Path) -> None:
        """is_iceberg_table should return False when metadata dir doesn't exist."""
        (tmp_path / "some_file.txt").write_text("content")
        fs = FileSystem(tmp_path)
        assert is_iceberg_table(tmp_path, fs=fs) is False

    def test_is_hive_partitioned_with_fs(self, tmp_path: Path) -> None:
        """is_hive_partitioned should work with FileSystem."""
        partition = tmp_path / "year=2024"
        partition.mkdir()
        (partition / "data.parquet").write_bytes(b"PAR1")

        fs = FileSystem(tmp_path)
        assert is_hive_partitioned(tmp_path, fs=fs) is True

    def test_is_hive_partitioned_with_fs_not_hive(self, tmp_path: Path) -> None:
        """is_hive_partitioned should return False for non-Hive directories."""
        fs = FileSystem(tmp_path)
        assert is_hive_partitioned(tmp_path, fs=fs) is False

    def test_is_hive_partitioned_with_fs_not_dir(self, tmp_path: Path) -> None:
        """is_hive_partitioned should return False for non-directories."""
        file_path = tmp_path / "file.txt"
        file_path.write_text("content")
        fs = FileSystem(tmp_path)
        assert is_hive_partitioned(file_path, fs=fs) is False

    def test_is_hive_partitioned_with_fs_no_parquet(self, tmp_path: Path) -> None:
        """is_hive_partitioned should return False if partition has no parquet."""
        partition = tmp_path / "year=2024"
        partition.mkdir()
        (partition / "data.csv").write_text("x\n1")

        fs = FileSystem(tmp_path)
        assert is_hive_partitioned(tmp_path, fs=fs) is False

    def test_is_hive_partitioned_with_fs_non_partition_subdir(
        self, tmp_path: Path
    ) -> None:
        """is_hive_partitioned should skip non-partition subdirectories."""
        # Regular subdirectory (not matching key=value pattern)
        regular_dir = tmp_path / "data"
        regular_dir.mkdir()
        (regular_dir / "file.parquet").write_bytes(b"PAR1")

        # Partition directory with parquet
        partition = tmp_path / "year=2024"
        partition.mkdir()
        (partition / "data.parquet").write_bytes(b"PAR1")

        fs = FileSystem(tmp_path)
        assert is_hive_partitioned(tmp_path, fs=fs) is True

    def test_is_hive_partitioned_with_fs_only_regular_dirs(
        self, tmp_path: Path
    ) -> None:
        """is_hive_partitioned should return False with only regular subdirs."""
        # Multiple regular subdirectories (not matching key=value pattern)
        (tmp_path / "data").mkdir()
        (tmp_path / "data" / "file.parquet").write_bytes(b"PAR1")
        (tmp_path / "archive").mkdir()
        (tmp_path / "archive" / "old.parquet").write_bytes(b"PAR1")

        fs = FileSystem(tmp_path)
        assert is_hive_partitioned(tmp_path, fs=fs) is False
