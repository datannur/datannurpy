"""Integration tests for remote storage using memory:// filesystem.

These tests validate the full remote code path without network I/O.
The memory:// filesystem is built into fsspec and works everywhere.
"""

from __future__ import annotations

import uuid

import fsspec
import pytest

from datannurpy import Catalog, Folder
from datannurpy.errors import ConfigError
from datannurpy.scanner.filesystem import FileSystem


@pytest.fixture
def memory_root() -> str:
    """Create a unique root path for each test to avoid conflicts in parallel."""
    return f"/{uuid.uuid4().hex}"


@pytest.fixture
def memory_fs(memory_root: str) -> fsspec.AbstractFileSystem:
    """Create a memory filesystem with unique root for each test."""
    fs = fsspec.filesystem("memory")
    fs.mkdir(memory_root)
    return fs


class TestMemoryFileSystem:
    """Test FileSystem class with memory:// protocol."""

    def test_init_memory_url(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """FileSystem should recognize memory:// as non-local."""
        fs = FileSystem(f"memory://{memory_root}")
        assert not fs.is_local
        assert fs.root == memory_root

    def test_glob_memory(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """FileSystem.glob should work with memory://."""
        memory_fs.pipe(f"{memory_root}/file1.csv", b"a,b\n1,2")
        memory_fs.pipe(f"{memory_root}/file2.csv", b"a,b\n3,4")

        fs = FileSystem(f"memory://{memory_root}")
        files = fs.glob("*.csv")
        assert len(files) == 2
        assert f"{memory_root}/file1.csv" in files
        assert f"{memory_root}/file2.csv" in files

    def test_isdir_isfile_memory(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """FileSystem.isdir/isfile should work with memory://."""
        memory_fs.mkdir(f"{memory_root}/sub")
        memory_fs.pipe(f"{memory_root}/file.csv", b"a,b\n1,2")

        fs = FileSystem(f"memory://{memory_root}")
        assert fs.isdir(memory_root)
        assert fs.isdir(f"{memory_root}/sub")
        assert fs.isfile(f"{memory_root}/file.csv")
        assert not fs.isfile(f"{memory_root}/sub")
        assert not fs.isdir(f"{memory_root}/file.csv")

    def test_exists_memory(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """FileSystem.exists should work with memory://."""
        memory_fs.pipe(f"{memory_root}/file.csv", b"a,b\n1,2")

        fs = FileSystem(f"memory://{memory_root}")
        assert fs.exists(memory_root)
        assert fs.exists(f"{memory_root}/file.csv")
        assert not fs.exists(f"{memory_root}/nonexistent.csv")

    def test_ensure_local_downloads(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """ensure_local should download remote file to temp location."""
        memory_fs.pipe(f"{memory_root}/file.csv", b"a,b\n1,2\n3,4")

        fs = FileSystem(f"memory://{memory_root}")
        with fs.ensure_local(f"{memory_root}/file.csv") as local_path:
            content = local_path.read_bytes()
            assert content == b"a,b\n1,2\n3,4"

    def test_ensure_local_partial_downloads_n_bytes(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """ensure_local_partial should only download first N bytes from remote."""
        # Create a large file
        content = b"HEADER" + b"X" * 10000
        memory_fs.pipe(f"{memory_root}/large.sas7bdat", content)

        fs = FileSystem(f"memory://{memory_root}")
        with fs.ensure_local_partial(
            f"{memory_root}/large.sas7bdat", max_bytes=100
        ) as local_path:
            assert local_path.suffix == ".sas7bdat"
            partial = local_path.read_bytes()
            assert len(partial) == 100
            assert partial == b"HEADER" + b"X" * 94
        # Temp file cleaned up
        assert not local_path.exists()


class TestCatalogWithMemoryFS:
    """Test Catalog.add_folder and add_dataset with memory:// filesystem."""

    def test_add_folder_memory_csv(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """add_folder should scan CSV files from memory://."""
        memory_fs.pipe(f"{memory_root}/sales.csv", b"id,amount\n1,100\n2,200\n3,300")

        catalog = Catalog(quiet=True)
        catalog.add_folder(f"memory://{memory_root}", Folder(id="test"))

        assert len(catalog.dataset.all()) == 1
        ds = catalog.dataset.all()[0]
        assert ds.id == "test---sales_csv"
        assert ds.delivery_format == "csv"
        assert len(catalog.variable.all()) == 2

    def test_add_folder_memory_multiple_files(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """add_folder should scan multiple files from memory://."""
        memory_fs.pipe(f"{memory_root}/file1.csv", b"a,b\n1,2")
        memory_fs.pipe(f"{memory_root}/file2.csv", b"x,y,z\n1,2,3")

        catalog = Catalog(quiet=True)
        catalog.add_folder(f"memory://{memory_root}", Folder(id="multi"))

        assert len(catalog.dataset.all()) == 2
        # 2 + 3 = 5 variables total
        assert len(catalog.variable.all()) == 5

    def test_add_folder_memory_nested(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """add_folder should scan nested directories from memory://."""
        memory_fs.mkdir(f"{memory_root}/sub1")
        memory_fs.mkdir(f"{memory_root}/sub2")
        memory_fs.pipe(f"{memory_root}/root.csv", b"a\n1")
        memory_fs.pipe(f"{memory_root}/sub1/file1.csv", b"b\n2")
        memory_fs.pipe(f"{memory_root}/sub2/file2.csv", b"c\n3")

        catalog = Catalog(quiet=True)
        catalog.add_folder(
            f"memory://{memory_root}", Folder(id="nested"), recursive=True
        )

        assert len(catalog.dataset.all()) == 3

    def test_add_folder_memory_with_include(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """add_folder should respect include patterns with memory://."""
        memory_fs.pipe(f"{memory_root}/keep.csv", b"a\n1")
        memory_fs.pipe(f"{memory_root}/skip.txt", b"ignored")

        catalog = Catalog(quiet=True)
        catalog.add_folder(
            f"memory://{memory_root}", Folder(id="filtered"), include=["*.csv"]
        )

        assert len(catalog.dataset.all()) == 1
        assert catalog.dataset.all()[0].id == "filtered---keep_csv"

    def test_add_dataset_memory_csv(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """add_dataset should scan a single CSV file from memory://."""
        memory_fs.pipe(f"{memory_root}/single.csv", b"col1,col2\na,1\nb,2")

        catalog = Catalog(quiet=True)
        catalog.add_dataset(f"memory://{memory_root}/single.csv")

        assert len(catalog.dataset.all()) == 1
        assert catalog.dataset.all()[0].delivery_format == "csv"
        assert len(catalog.variable.all()) == 2

    def test_add_dataset_memory_with_folder(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """add_dataset should accept folder with memory:// path."""
        memory_fs.pipe(f"{memory_root}/file.csv", b"x\n1")

        catalog = Catalog(quiet=True)
        catalog.add_dataset(
            f"memory://{memory_root}/file.csv", folder=Folder(id="myfolder")
        )

        assert len(catalog.folder.where("id", "!=", "_modalities")) == 1
        assert catalog.folder.where("id", "!=", "_modalities")[0].id == "myfolder"
        # add_dataset uses name without extension for ID
        assert catalog.dataset.all()[0].id == "myfolder---file"
        assert catalog.dataset.all()[0].folder_id == "myfolder"

    def test_add_folder_memory_depth_structure(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """add_folder with depth='dataset' should skip variable scanning."""
        memory_fs.pipe(f"{memory_root}/file.csv", b"a,b,c\n1,2,3")

        catalog = Catalog(quiet=True, depth="dataset")
        catalog.add_folder(f"memory://{memory_root}", Folder(id="struct"))

        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 0  # No variables at structure depth

    def test_add_folder_memory_depth_schema(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """add_folder with depth='variable' should get variables but no stats."""
        memory_fs.pipe(f"{memory_root}/file.csv", b"a,b\n1,2\n3,4")

        catalog = Catalog(quiet=True, depth="variable")
        catalog.add_folder(f"memory://{memory_root}", Folder(id="schema"))

        assert len(catalog.dataset.all()) == 1
        ds = catalog.dataset.all()[0]
        assert ds.nb_row is None  # No row count at schema depth
        assert len(catalog.variable.all()) == 2  # Variables present

    def test_add_folder_memory_not_found(self) -> None:
        """add_folder should raise FileNotFoundError for missing memory:// path."""
        catalog = Catalog(quiet=True)
        with pytest.raises(ConfigError):
            catalog.add_folder(f"memory:///{uuid.uuid4().hex}")

    def test_add_dataset_memory_not_found(self) -> None:
        """add_dataset should raise ConfigError for missing memory:// path."""
        catalog = Catalog(quiet=True)
        with pytest.raises(ConfigError):
            catalog.add_dataset(f"memory:///{uuid.uuid4().hex}/file.csv")

    def test_add_folder_memory_parquet(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """add_folder should scan parquet files from memory:// via ensure_local."""
        import pyarrow as pa
        import pyarrow.parquet as pq
        from io import BytesIO

        # Create a parquet file in memory
        table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        buffer = BytesIO()
        pq.write_table(table, buffer)
        memory_fs.pipe(f"{memory_root}/data.parquet", buffer.getvalue())

        catalog = Catalog(quiet=True)
        catalog.add_folder(f"memory://{memory_root}", Folder(id="pq"))

        assert len(catalog.dataset.all()) == 1
        ds = catalog.dataset.all()[0]
        assert ds.delivery_format == "parquet"
        assert ds.nb_row == 3
        assert len(catalog.variable.all()) == 2

    def test_add_folder_memory_hive_partitioned(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """add_folder should scan Hive-partitioned datasets from memory://."""
        import pyarrow as pa
        import pyarrow.parquet as pq
        from io import BytesIO

        # Create Hive-partitioned structure: data/year=2024/file.parquet
        memory_fs.mkdir(f"{memory_root}/data")
        memory_fs.mkdir(f"{memory_root}/data/year=2024")
        memory_fs.mkdir(f"{memory_root}/data/year=2025")

        table1 = pa.table({"x": [1, 2]})
        buffer1 = BytesIO()
        pq.write_table(table1, buffer1)
        memory_fs.pipe(f"{memory_root}/data/year=2024/part.parquet", buffer1.getvalue())

        table2 = pa.table({"x": [3]})
        buffer2 = BytesIO()
        pq.write_table(table2, buffer2)
        memory_fs.pipe(f"{memory_root}/data/year=2025/part.parquet", buffer2.getvalue())

        catalog = Catalog(quiet=True)
        catalog.add_folder(f"memory://{memory_root}", Folder(id="hive"))

        # Should detect as single Hive-partitioned dataset
        assert len(catalog.dataset.all()) == 1
        ds = catalog.dataset.all()[0]
        assert ds.delivery_format == "hive"
        assert ds.nb_row == 3  # 2 + 1 rows

    def test_ensure_local_dir_memory(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """ensure_local_dir should download directory from memory://."""
        from datannurpy.scanner.filesystem import FileSystem

        # Create a directory with files
        memory_fs.mkdir(f"{memory_root}/mydir")
        memory_fs.pipe(f"{memory_root}/mydir/file1.txt", b"content1")
        memory_fs.pipe(f"{memory_root}/mydir/file2.txt", b"content2")

        fs = FileSystem(f"memory://{memory_root}")
        with fs.ensure_local_dir("mydir") as local_path:
            # Directory should exist locally
            assert local_path.exists()
            assert local_path.is_dir()
            # Files should be present
            assert (local_path / "file1.txt").read_text() == "content1"
            assert (local_path / "file2.txt").read_text() == "content2"

        # Temp directory should be cleaned up
        assert not local_path.exists()


class TestSchemaOnlyRemoteOptimizations:
    """Test depth='variable' optimizations for remote storage."""

    def test_schema_only_parquet_remote(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """depth='variable' should use PyArrow fs for parquet (no full download)."""
        from io import BytesIO

        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create a parquet file
        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        buffer = BytesIO()
        pq.write_table(table, buffer)
        memory_fs.pipe(f"{memory_root}/data.parquet", buffer.getvalue())

        catalog = Catalog(quiet=True)
        catalog.add_folder(
            f"memory://{memory_root}", Folder(id="test"), depth="variable"
        )

        assert len(catalog.dataset.all()) == 1
        ds = catalog.dataset.all()[0]
        assert ds.nb_row is None  # schema_only doesn't return row count
        assert len(catalog.variable.all()) == 2

    def test_schema_only_hive_remote(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """depth='variable' should use PyArrow fs for hive partition schema."""
        from io import BytesIO

        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create hive partitioned structure
        table = pa.table({"value": [100]})
        buffer = BytesIO()
        pq.write_table(table, buffer)
        memory_fs.pipe(f"{memory_root}/data/year=2024/part.parquet", buffer.getvalue())

        catalog = Catalog(quiet=True)
        catalog.add_folder(
            f"memory://{memory_root}", Folder(id="hive"), depth="variable"
        )

        assert len(catalog.dataset.all()) == 1
        ds = catalog.dataset.all()[0]
        assert ds.delivery_format == "hive"
        assert ds.nb_row is None

    def test_schema_only_excel_remote_full_download(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """depth='variable' for Excel xlsx should stream headers (no full download)."""
        from io import BytesIO

        # Create synthetic Excel by using openpyxl
        try:
            from openpyxl import Workbook

            wb = Workbook()
            ws = wb.active
            assert ws is not None
            ws.append(["col1", "col2"])
            ws.append([1, 2])

            buffer = BytesIO()
            wb.save(buffer)
            memory_fs.pipe(f"{memory_root}/test.xlsx", buffer.getvalue())

            catalog = Catalog(quiet=True)
            catalog.add_folder(
                f"memory://{memory_root}", Folder(id="excel"), depth="variable"
            )

            assert len(catalog.dataset.all()) == 1
            ds = catalog.dataset.all()[0]
            assert ds.delivery_format == "excel"
            assert ds.nb_row is None

            var_names = [v.name for v in catalog.variable.all()]
            assert "col1" in var_names
            assert "col2" in var_names
        except ImportError:
            pytest.skip("openpyxl not installed")

    def test_schema_only_xls_remote_full_download(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """depth='variable' for xls must download full file (xlrd, no streaming)."""
        from pathlib import Path

        from datannurpy.scanner.filesystem import FileSystem
        from datannurpy.scanner.scan import scan_file

        xls_path = (
            Path(__file__).parent.parent / "data" / "subfolder1" / "employees_old.xls"
        )
        if not xls_path.exists():
            pytest.skip("xls test file not found")

        memory_fs.pipe(f"{memory_root}/test.xls", xls_path.read_bytes())

        fs = FileSystem(f"memory://{memory_root}")
        result = scan_file(
            Path(f"{memory_root}/test.xls"),
            "excel",
            dataset_id="test",
            schema_only=True,
            fs=fs,
        )

        assert len(result.variables) > 0
        assert result.nb_row is None

    def test_schema_only_hive_empty(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """depth='variable' with empty hive should return empty schema."""
        from pathlib import Path

        from datannurpy.scanner.filesystem import FileSystem
        from datannurpy.scanner.scan import scan_file

        # Create empty directory structure (no parquet files)
        memory_fs.mkdir(f"{memory_root}/empty/year=2024")

        fs = FileSystem(f"memory://{memory_root}")
        result = scan_file(
            Path(f"{memory_root}/empty"),
            "hive",
            dataset_id="test",
            schema_only=True,
            fs=fs,
        )

        assert result.variables == []
        assert result.nb_row is None

    def test_schema_only_delta_remote(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str, tmp_path
    ) -> None:
        """depth='variable' for Delta should only download _delta_log directory."""
        deltalake = pytest.importorskip("deltalake")
        from io import BytesIO
        from pathlib import Path

        import pyarrow as pa
        import pyarrow.parquet as pq

        from datannurpy.scanner.filesystem import FileSystem
        from datannurpy.scanner.scan import scan_file

        # Create a real Delta table locally first (Delta needs real filesystem)
        delta_path = tmp_path / "delta_table"
        table = pa.table({"id": [1, 2], "value": ["a", "b"]})
        deltalake.write_deltalake(str(delta_path), table)

        # Copy the _delta_log to memory filesystem
        for f in (delta_path / "_delta_log").iterdir():
            content = f.read_bytes()
            memory_fs.pipe(f"{memory_root}/test_delta/_delta_log/{f.name}", content)

        # Also need a parquet file for discovery to detect it as Delta
        buffer = BytesIO()
        pq.write_table(table, buffer)
        memory_fs.pipe(f"{memory_root}/test_delta/part-0.parquet", buffer.getvalue())

        fs = FileSystem(f"memory://{memory_root}")
        result = scan_file(
            Path(f"{memory_root}/test_delta"),
            "delta",
            dataset_id="test",
            schema_only=True,
            fs=fs,
        )

        assert len(result.variables) == 2
        assert result.nb_row is None

    def test_schema_only_iceberg_remote(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """depth='variable' for Iceberg should only download metadata directory."""
        import json
        from pathlib import Path

        from datannurpy.scanner.filesystem import FileSystem
        from datannurpy.scanner.scan import scan_file

        # Create a minimal Iceberg metadata structure
        iceberg_metadata = {
            "format-version": 1,
            "schemas": [
                {
                    "schema-id": 0,
                    "fields": [
                        {"id": 1, "name": "id", "type": "long", "required": True},
                        {"id": 2, "name": "name", "type": "string", "required": False},
                        {"id": 3, "name": "created", "type": "timestamp"},
                    ],
                }
            ],
        }
        memory_fs.pipe(
            f"{memory_root}/test_iceberg/metadata/00001-abc.metadata.json",
            json.dumps(iceberg_metadata).encode(),
        )

        fs = FileSystem(f"memory://{memory_root}")
        result = scan_file(
            Path(f"{memory_root}/test_iceberg"),
            "iceberg",
            dataset_id="test",
            schema_only=True,
            fs=fs,
        )

        assert len(result.variables) == 3
        var_names = [v.name for v in result.variables]
        assert "id" in var_names
        assert "name" in var_names
        assert "created" in var_names
        assert result.nb_row is None

    def test_schema_only_sas_remote(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """depth='variable' for SAS should use pandas streaming (no full download)."""
        from pathlib import Path

        from datannurpy.scanner.filesystem import FileSystem
        from datannurpy.scanner.scan import scan_file

        # Use real SAS file
        sas_path = Path(__file__).parent.parent / "data" / "cars.sas7bdat"
        if not sas_path.exists():
            pytest.skip("SAS test file not found")

        # Upload entire file since we need at least header bytes
        memory_fs.pipe(f"{memory_root}/cars.sas7bdat", sas_path.read_bytes())

        fs = FileSystem(f"memory://{memory_root}")
        result = scan_file(
            Path(f"{memory_root}/cars.sas7bdat"),
            "sas",
            dataset_id="test",
            schema_only=True,
            fs=fs,
        )

        assert len(result.variables) > 0
        assert result.nb_row is None
        # SAS streaming should extract types from header
        assert all(v.type == "float" for v in result.variables)

    def test_schema_only_stata_remote(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """depth='variable' for Stata should use pandas streaming (no full download)."""
        from pathlib import Path

        from datannurpy.scanner.filesystem import FileSystem
        from datannurpy.scanner.scan import scan_file

        stata_path = Path(__file__).parent.parent / "data" / "datatypes_stata.dta"
        if not stata_path.exists():
            pytest.skip("Stata test file not found")

        memory_fs.pipe(f"{memory_root}/datatypes_stata.dta", stata_path.read_bytes())

        fs = FileSystem(f"memory://{memory_root}")
        result = scan_file(
            Path(f"{memory_root}/datatypes_stata.dta"),
            "stata",
            dataset_id="test",
            schema_only=True,
            fs=fs,
        )

        assert len(result.variables) > 0
        assert result.nb_row is None
        # Stata streaming should extract types from header
        var_types = {v.name: v.type for v in result.variables}
        assert all(t in ("float", "string") for t in var_types.values())

    def test_schema_only_stata_remote_no_dtype(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """Stata streaming should handle missing _dtype gracefully."""
        from pathlib import Path
        from unittest.mock import patch

        from datannurpy.scanner.filesystem import FileSystem
        from datannurpy.scanner.scan import scan_file

        stata_path = Path(__file__).parent.parent / "data" / "datatypes_stata.dta"
        if not stata_path.exists():
            pytest.skip("Stata test file not found")

        memory_fs.pipe(f"{memory_root}/datatypes_stata.dta", stata_path.read_bytes())

        fs = FileSystem(f"memory://{memory_root}")
        with patch(
            "pandas.io.stata.StataReader._setup_dtype",
            lambda self: setattr(self, "_dtype", None),
        ):
            result = scan_file(
                Path(f"{memory_root}/datatypes_stata.dta"),
                "stata",
                dataset_id="test",
                schema_only=True,
                fs=fs,
            )

        assert len(result.variables) > 0
        assert all(v.type is None for v in result.variables)

    def test_schema_only_spss_remote(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """depth='variable' for SPSS should download full file (no streaming support)."""
        pytest.importorskip("pyreadstat")
        from pathlib import Path

        from datannurpy.scanner.filesystem import FileSystem
        from datannurpy.scanner.scan import scan_file

        spss_path = Path(__file__).parent.parent / "data" / "datatypes_spss.sav"
        if not spss_path.exists():
            pytest.skip("SPSS test file not found")

        memory_fs.pipe(f"{memory_root}/datatypes_spss.sav", spss_path.read_bytes())

        fs = FileSystem(f"memory://{memory_root}")
        result = scan_file(
            Path(f"{memory_root}/datatypes_spss.sav"),
            "spss",
            dataset_id="test",
            schema_only=True,
            fs=fs,
        )

        assert len(result.variables) > 0
        assert result.nb_row is None

    def test_iceberg_type_conversion(self) -> None:
        """Test _iceberg_type_to_pyarrow covers all type mappings."""
        import pyarrow as pa

        from datannurpy.scanner.scan import _iceberg_type_to_pyarrow

        # Test simple types
        assert _iceberg_type_to_pyarrow("boolean") == pa.bool_()
        assert _iceberg_type_to_pyarrow("int") == pa.int32()
        assert _iceberg_type_to_pyarrow("long") == pa.int64()
        assert _iceberg_type_to_pyarrow("float") == pa.float32()
        assert _iceberg_type_to_pyarrow("double") == pa.float64()
        assert _iceberg_type_to_pyarrow("string") == pa.string()
        assert _iceberg_type_to_pyarrow("binary") == pa.binary()
        assert _iceberg_type_to_pyarrow("date") == pa.date32()
        assert _iceberg_type_to_pyarrow("timestamp") == pa.timestamp("us")
        assert _iceberg_type_to_pyarrow("timestamptz") == pa.timestamp("us", tz="UTC")

        # Test unknown type defaults to string
        assert _iceberg_type_to_pyarrow("unknown") == pa.string()

        # Test dict type (nested type like list, map)
        assert _iceberg_type_to_pyarrow({"type": "long"}) == pa.int64()
        assert _iceberg_type_to_pyarrow({"type": "unknown"}) == pa.string()

    def test_schema_only_iceberg_empty_metadata(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """depth='variable' for Iceberg with empty metadata returns empty schema."""
        from pathlib import Path

        from datannurpy.scanner.filesystem import FileSystem
        from datannurpy.scanner.scan import scan_file

        # Create empty metadata directory (no .metadata.json files)
        memory_fs.mkdir(f"{memory_root}/test_iceberg_empty/metadata")

        fs = FileSystem(f"memory://{memory_root}")
        result = scan_file(
            Path(f"{memory_root}/test_iceberg_empty"),
            "iceberg",
            dataset_id="test",
            schema_only=True,
            fs=fs,
        )

        assert result.variables == []
        assert result.nb_row is None


class TestRemoteDatabase:
    """Test add_database with remote SQLite/GeoPackage files."""

    def test_is_remote_database_file(self) -> None:
        """Test is_remote_database_file correctly identifies remote file URLs."""
        from datannurpy.scanner.database import is_remote_database_file

        # Database URLs are NOT remote files
        assert not is_remote_database_file("sqlite:///test.db")
        assert not is_remote_database_file("postgresql://localhost/db")
        assert not is_remote_database_file("mysql://user:pass@host/db")
        assert not is_remote_database_file("duckdb://md:mydb")

        # Local paths are NOT remote files
        assert not is_remote_database_file("/path/to/file.db")
        assert not is_remote_database_file("file:///path/to/file.db")

        # Remote file URLs ARE remote files
        assert is_remote_database_file("sftp://server/path/file.db")
        assert is_remote_database_file("s3://bucket/path/file.db")
        assert is_remote_database_file("gcs://bucket/path/file.gpkg")
        assert is_remote_database_file("az://container/path/file.db")

    def test_add_database_remote_sqlite(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str, tmp_path
    ) -> None:
        """add_database should download and scan remote SQLite files."""
        import sqlite3

        # Create a real SQLite database
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE users (id INT, name TEXT)")
        cursor.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        conn.commit()
        conn.close()

        # Upload to memory filesystem
        memory_fs.pipe(f"{memory_root}/test.db", db_path.read_bytes())

        # Scan via memory://
        catalog = Catalog(quiet=True)
        catalog.add_database(f"memory://{memory_root}/test.db")

        # Should have 1 dataset (users table)
        assert len(catalog.dataset.all()) == 1
        ds = catalog.dataset.all()[0]
        assert ds.name == "users"
        assert ds.nb_row == 2
        # data_path should be the remote path
        assert ds.data_path == "sqlite://test/users"

    def test_add_database_remote_geopackage(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        """add_database should handle remote GeoPackage files (they are SQLite)."""
        # Use real .gpkg file from test data
        from pathlib import Path

        gpkg_path = Path(__file__).parent.parent / "data" / "photovoltaik.gpkg"
        if not gpkg_path.exists():
            pytest.skip("GeoPackage test file not found")

        # Upload to memory filesystem
        memory_fs.pipe(f"{memory_root}/photo.gpkg", gpkg_path.read_bytes())

        # Scan via memory://
        catalog = Catalog(quiet=True)
        catalog.add_database(f"memory://{memory_root}/photo.gpkg")

        # Should find tables from the GeoPackage
        assert len(catalog.dataset.all()) > 0
        # Folder data_path should be the remote URL
        folders = catalog.folder.all()
        root_folder = next(f for f in folders if f.id == "photo")
        assert root_folder.data_path == f"memory://{memory_root}/photo.gpkg"
