"""Tests for FileSystem abstraction."""

from __future__ import annotations

from pathlib import Path

import pytest

from datannurpy.scanner.filesystem import (
    FileSystem,
    _expand_home_in_options,
    get_filesystem,
    is_remote_url,
)
from datannurpy.scanner.utils import (
    find_files,
    get_mtime_iso,
    get_mtime_timestamp,
)


class TestExpandHomeInOptions:
    """Test _expand_home_in_options function."""

    def test_expands_key_filename(self) -> None:
        """Should expand ~ in key_filename."""
        opts = {"key_filename": "~/.ssh/id_rsa"}
        result = _expand_home_in_options(opts)
        assert result["key_filename"] == str(Path.home() / ".ssh" / "id_rsa")

    def test_expands_keyfile(self) -> None:
        """Should expand ~ in keyfile."""
        opts = {"keyfile": "~/keys/my_key"}
        result = _expand_home_in_options(opts)
        assert result["keyfile"] == str(Path.home() / "keys" / "my_key")

    def test_leaves_absolute_paths(self) -> None:
        """Should not modify absolute paths without ~."""
        opts = {"key_filename": "/home/user/.ssh/id_rsa"}
        result = _expand_home_in_options(opts)
        assert result["key_filename"] == "/home/user/.ssh/id_rsa"

    def test_leaves_other_options(self) -> None:
        """Should not modify non-path options."""
        opts = {"password": "secret", "username": "user"}
        result = _expand_home_in_options(opts)
        assert result == opts


class TestIsRemoteUrl:
    """Test is_remote_url function."""

    def test_local_path(self) -> None:
        """Local paths are not remote URLs."""
        assert not is_remote_url("/path/to/file")
        assert not is_remote_url("relative/path")
        assert not is_remote_url(Path("/path/to/file"))

    def test_file_url(self) -> None:
        """file:// URLs are not treated as remote."""
        assert not is_remote_url("file:///path/to/file")

    def test_remote_urls(self) -> None:
        """Remote URLs are correctly identified."""
        assert is_remote_url("s3://bucket/key")
        assert is_remote_url("sftp://host/path")
        assert is_remote_url("az://container/blob")
        assert is_remote_url("gs://bucket/key")
        assert is_remote_url("https://example.com/file")


class TestFileSystem:
    """Test FileSystem class for local filesystem operations."""

    def test_init_local_path(self, tmp_path: Path) -> None:
        """FileSystem should initialize with a local path."""
        fs = FileSystem(tmp_path)
        assert fs.is_local
        assert fs.root == str(tmp_path)

    def test_init_path_object(self, tmp_path: Path) -> None:
        """FileSystem should accept Path objects."""
        fs = FileSystem(tmp_path)
        assert fs.is_local

    def test_glob_finds_files(self, tmp_path: Path) -> None:
        """glob() should find files matching pattern."""
        (tmp_path / "file1.csv").write_text("a")
        (tmp_path / "file2.csv").write_text("b")
        (tmp_path / "file3.txt").write_text("c")

        fs = FileSystem(tmp_path)
        results = fs.glob("*.csv")
        assert len(results) == 2
        assert all("csv" in r for r in results)

    def test_glob_recursive(self, tmp_path: Path) -> None:
        """glob() should support recursive patterns."""
        subdir = tmp_path / "sub"
        subdir.mkdir()
        (tmp_path / "top.csv").write_text("a")
        (subdir / "nested.csv").write_text("b")

        fs = FileSystem(tmp_path)
        results = fs.glob("**/*.csv")
        assert len(results) == 2

    def test_isdir(self, tmp_path: Path) -> None:
        """isdir() should detect directories."""
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (tmp_path / "file.txt").write_text("content")

        fs = FileSystem(tmp_path)
        assert fs.isdir(str(subdir))
        assert not fs.isdir(str(tmp_path / "file.txt"))
        assert not fs.isdir(str(tmp_path / "nonexistent"))

    def test_isfile(self, tmp_path: Path) -> None:
        """isfile() should detect files."""
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (tmp_path / "file.txt").write_text("content")

        fs = FileSystem(tmp_path)
        assert fs.isfile(str(tmp_path / "file.txt"))
        assert not fs.isfile(str(subdir))
        assert not fs.isfile(str(tmp_path / "nonexistent"))

    def test_exists(self, tmp_path: Path) -> None:
        """exists() should check path existence."""
        (tmp_path / "file.txt").write_text("content")

        fs = FileSystem(tmp_path)
        assert fs.exists(str(tmp_path / "file.txt"))
        assert not fs.exists(str(tmp_path / "nonexistent"))

    def test_info(self, tmp_path: Path) -> None:
        """info() should return file metadata."""
        file_path = tmp_path / "file.txt"
        file_path.write_text("content")

        fs = FileSystem(tmp_path)
        info = fs.info(str(file_path))
        assert "size" in info
        assert info["size"] == 7  # "content" = 7 bytes

    def test_listdir(self, tmp_path: Path) -> None:
        """listdir() should list directory contents."""
        (tmp_path / "file1.txt").write_text("a")
        (tmp_path / "file2.txt").write_text("b")
        subdir = tmp_path / "subdir"
        subdir.mkdir()

        fs = FileSystem(tmp_path)
        contents = fs.listdir(str(tmp_path))
        assert "file1.txt" in contents
        assert "file2.txt" in contents
        assert "subdir" in contents

    def test_iterdir(self, tmp_path: Path) -> None:
        """iterdir() should iterate over directory contents."""
        (tmp_path / "file1.txt").write_text("a")
        (tmp_path / "file2.txt").write_text("b")

        fs = FileSystem(tmp_path)
        contents = list(fs.iterdir(str(tmp_path)))
        assert len(contents) == 2

    def test_open_read(self, tmp_path: Path) -> None:
        """open() should read file contents."""
        file_path = tmp_path / "file.txt"
        file_path.write_text("hello world")

        fs = FileSystem(tmp_path)
        with fs.open(str(file_path), "rb") as f:
            content = f.read()
        assert content == b"hello world"

    def test_ensure_local_local_fs(self, tmp_path: Path) -> None:
        """ensure_local() should return path directly for local filesystem."""
        file_path = tmp_path / "data.csv"
        file_path.write_text("a,b\n1,2")

        fs = FileSystem(tmp_path)
        with fs.ensure_local(str(file_path)) as local_path:
            assert local_path == file_path
            assert local_path.read_text() == "a,b\n1,2"

    def test_ensure_local_remote_fs(self, tmp_path: Path) -> None:
        """ensure_local() should download remote files to temp location."""
        # Use memory filesystem to simulate remote
        import fsspec

        mem_fs = fsspec.filesystem("memory")
        mem_fs.mkdir("/test")
        mem_fs.pipe("/test/data.csv", b"x,y\n3,4")

        fs = FileSystem("memory://test")
        with fs.ensure_local("/test/data.csv") as local_path:
            assert local_path.suffix == ".csv"
            assert local_path.read_text() == "x,y\n3,4"
        # Temp file should be cleaned up
        assert not local_path.exists()

    def test_ensure_local_dir_local_fs(self, tmp_path: Path) -> None:
        """ensure_local_dir() should return path directly for local filesystem."""
        dir_path = tmp_path / "mydir"
        dir_path.mkdir()
        (dir_path / "file.txt").write_text("content")

        fs = FileSystem(tmp_path)
        with fs.ensure_local_dir(str(dir_path)) as local_path:
            assert local_path == dir_path
            assert (local_path / "file.txt").read_text() == "content"

    def test_ensure_local_partial_reads_first_n_bytes(self, tmp_path: Path) -> None:
        """ensure_local_partial() should only read the first N bytes."""
        # Create a larger file
        data_file = tmp_path / "data.sas7bdat"
        content = b"X" * 1000 + b"Y" * 1000  # 2000 bytes
        data_file.write_bytes(content)

        fs = FileSystem(tmp_path)
        with fs.ensure_local_partial(str(data_file), max_bytes=500) as local_path:
            assert local_path.suffix == ".sas7bdat"
            partial_content = local_path.read_bytes()
            assert len(partial_content) == 500
            assert partial_content == b"X" * 500
        # Temp file should be cleaned up
        assert not local_path.exists()

    def test_to_path_local(self, tmp_path: Path) -> None:
        """to_path() should convert to Path for local filesystem."""
        fs = FileSystem(tmp_path)
        path = fs.to_path(str(tmp_path / "file.txt"))
        assert isinstance(path, Path)
        assert str(path) == str(tmp_path / "file.txt")

    def test_relative_to_root(self, tmp_path: Path) -> None:
        """relative_to_root() should return relative path."""
        fs = FileSystem(tmp_path)
        rel = fs.relative_to_root(str(tmp_path / "sub" / "file.txt"))
        assert rel == "sub/file.txt"

    def test_relative_to_root_root_path(self, tmp_path: Path) -> None:
        """relative_to_root() should handle root path."""
        fs = FileSystem(tmp_path)
        rel = fs.relative_to_root(str(tmp_path))
        assert rel == "."

    def test_full_path_already_full(self, tmp_path: Path) -> None:
        """_full_path() should not duplicate root."""
        fs = FileSystem(tmp_path)
        full = fs._full_path(str(tmp_path / "file.txt"))
        assert full == str(tmp_path / "file.txt")


class TestGetFilesystem:
    """Test get_filesystem factory function."""

    def test_creates_filesystem(self, tmp_path: Path) -> None:
        """get_filesystem() should create a FileSystem instance."""
        fs = get_filesystem(tmp_path)
        assert isinstance(fs, FileSystem)
        assert fs.is_local


class TestFindFilesWithFileSystem:
    """Test find_files() with FileSystem parameter."""

    def test_find_files_with_fs(self, tmp_path: Path) -> None:
        """find_files() should work with FileSystem parameter."""
        (tmp_path / "data.csv").write_text("a,b")
        (tmp_path / "readme.txt").write_text("info")

        fs = FileSystem(tmp_path)
        files = find_files(tmp_path, None, None, True, fs=fs)
        assert len(files) == 1
        assert files[0].name == "data.csv"

    def test_find_files_with_fs_include(self, tmp_path: Path) -> None:
        """find_files() with fs should respect include patterns."""
        (tmp_path / "data.csv").write_text("a,b")
        (tmp_path / "other.parquet").write_bytes(b"")

        fs = FileSystem(tmp_path)
        files = find_files(tmp_path, ["*.csv"], None, True, fs=fs)
        assert len(files) == 1
        assert files[0].suffix == ".csv"

    def test_find_files_with_fs_include_folder_pattern(self, tmp_path: Path) -> None:
        """find_files() with fs should handle folder/** pattern."""
        subdir = tmp_path / "sub"
        subdir.mkdir()
        (tmp_path / "root.csv").write_text("a")
        (subdir / "nested.csv").write_text("b")

        fs = FileSystem(tmp_path)
        files = find_files(tmp_path, ["sub/**"], None, True, fs=fs)
        assert len(files) == 1
        assert files[0].name == "nested.csv"

    def test_find_files_with_fs_include_non_recursive(self, tmp_path: Path) -> None:
        """find_files() with fs should handle ** in include pattern."""
        subdir = tmp_path / "sub"
        subdir.mkdir()
        (tmp_path / "root.csv").write_text("a")
        (subdir / "nested.csv").write_text("b")

        fs = FileSystem(tmp_path)
        # Pattern with ** should work directly
        files = find_files(tmp_path, ["**/*.csv"], None, True, fs=fs)
        assert len(files) == 2

    def test_find_files_with_fs_exclude_dir(self, tmp_path: Path) -> None:
        """find_files() with fs should exclude directories."""
        subdir = tmp_path / "excluded"
        subdir.mkdir()
        (tmp_path / "keep.csv").write_text("a")
        (subdir / "skip.csv").write_text("b")

        fs = FileSystem(tmp_path)
        files = find_files(tmp_path, None, ["excluded"], True, fs=fs)
        assert len(files) == 1
        assert files[0].name == "keep.csv"

    def test_find_files_with_fs_exclude_pattern(self, tmp_path: Path) -> None:
        """find_files() with fs should exclude by pattern."""
        (tmp_path / "data.csv").write_text("a")
        (tmp_path / "test.csv").write_text("b")

        fs = FileSystem(tmp_path)
        files = find_files(tmp_path, None, ["test.*"], True, fs=fs)
        assert len(files) == 1
        assert files[0].name == "data.csv"

    def test_find_files_with_fs_exclude_file(self, tmp_path: Path) -> None:
        """find_files() with fs should exclude exact file."""
        (tmp_path / "data.csv").write_text("a")
        (tmp_path / "skip.csv").write_text("b")

        fs = FileSystem(tmp_path)
        files = find_files(tmp_path, None, ["skip.csv"], True, fs=fs)
        assert len(files) == 1
        assert files[0].name == "data.csv"


class TestGetMtimeWithFileSystem:
    """Test get_mtime functions with FileSystem parameter."""

    def test_get_mtime_iso_with_fs(self, tmp_path: Path) -> None:
        """get_mtime_iso() should work with FileSystem."""
        file_path = tmp_path / "file.txt"
        file_path.write_text("content")

        fs = FileSystem(tmp_path)
        result = get_mtime_iso(file_path, fs=fs)
        # Should be a date string in YYYY/MM/DD format
        assert len(result) == 10
        assert result.count("/") == 2

    def test_get_mtime_timestamp_with_fs(self, tmp_path: Path) -> None:
        """get_mtime_timestamp() should work with FileSystem."""
        file_path = tmp_path / "file.txt"
        file_path.write_text("content")

        fs = FileSystem(tmp_path)
        result = get_mtime_timestamp(file_path, fs=fs)
        assert isinstance(result, int)
        assert result > 0

    def test_get_mtime_iso_datetime_from_sftp(self, tmp_path: Path) -> None:
        """get_mtime_iso() should handle datetime mtime (SFTP returns datetime)."""
        from datetime import datetime, timezone
        from unittest.mock import MagicMock

        file_path = tmp_path / "file.txt"
        file_path.write_text("content")
        test_dt = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

        fs = FileSystem(tmp_path)
        fs.info = MagicMock(return_value={"mtime": test_dt})

        result = get_mtime_iso(file_path, fs=fs)
        assert result == "2024/06/15"

    def test_get_mtime_timestamp_datetime_from_sftp(self, tmp_path: Path) -> None:
        """get_mtime_timestamp() should handle datetime mtime (SFTP returns datetime)."""
        from datetime import datetime, timezone
        from unittest.mock import MagicMock

        file_path = tmp_path / "file.txt"
        file_path.write_text("content")
        test_dt = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

        fs = FileSystem(tmp_path)
        fs.info = MagicMock(return_value={"mtime": test_dt})

        result = get_mtime_timestamp(file_path, fs=fs)
        assert result == int(test_dt.timestamp())


class TestFileSystemNonLocal:
    """Test FileSystem edge cases for non-local filesystems."""

    def test_to_path_non_local_raises(self) -> None:
        """to_path() should raise ValueError for non-local filesystems."""
        fs = FileSystem("memory://test")
        with pytest.raises(ValueError, match="only works with local"):
            fs.to_path("/some/path")

    def test_relative_to_root_different_path(self, tmp_path: Path) -> None:
        """relative_to_root() should return path unchanged if not under root."""
        fs = FileSystem(tmp_path)
        result = fs.relative_to_root("/some/other/path")
        assert result == "/some/other/path"


class TestFindFilesEdgeCases:
    """Edge cases for find_files with FileSystem."""

    def test_find_files_exclude_no_matching_candidates(self, tmp_path: Path) -> None:
        """find_files() should handle exclude when no files match."""
        # Only text file, no supported formats
        (tmp_path / "readme.txt").write_text("info")

        fs = FileSystem(tmp_path)
        files = find_files(tmp_path, None, ["*.csv"], True, fs=fs)
        assert files == []

    def test_find_files_multiple_exclude_patterns(self, tmp_path: Path) -> None:
        """find_files() should handle multiple exclude patterns."""
        (tmp_path / "data.csv").write_text("a")
        (tmp_path / "test.csv").write_text("b")
        (tmp_path / "skip.csv").write_text("c")

        fs = FileSystem(tmp_path)
        # Use exact file name (no wildcard) first, then wildcard
        # This ensures line 158 branch (elif fs.isfile) loops back to line 146
        files = find_files(tmp_path, None, ["skip.csv", "test.*"], True, fs=fs)
        assert len(files) == 1
        assert files[0].name == "data.csv"

    def test_find_files_exclude_nonexistent_file(self, tmp_path: Path) -> None:
        """find_files() should handle exclude pattern for nonexistent file."""
        (tmp_path / "data.csv").write_text("a")

        fs = FileSystem(tmp_path)
        # Exclude pattern for file that doesn't exist, followed by another pattern
        # This tests the branch where fs.isfile() is False (line 158 -> back to 146)
        files = find_files(tmp_path, None, ["nonexistent.csv", "data.csv"], True, fs=fs)
        assert files == []

    def test_find_files_excludes_git_directory(self, tmp_path: Path) -> None:
        """find_files() should exclude .git directories by default."""
        git_dir = tmp_path / ".git"
        git_dir.mkdir()
        (git_dir / "config.csv").write_text("a")
        (tmp_path / "data.csv").write_text("b")

        files = find_files(tmp_path, None, None, recursive=True)
        assert len(files) == 1
        assert files[0].name == "data.csv"

    def test_find_files_excludes_office_temp_files(self, tmp_path: Path) -> None:
        """find_files() should exclude Office temp files (~$*) by default."""
        (tmp_path / "~$document.xlsx").write_text("temp")
        (tmp_path / "document.xlsx").write_text("real")

        files = find_files(tmp_path, None, None, recursive=True)
        assert len(files) == 1
        assert files[0].name == "document.xlsx"

    def test_find_files_excludes_libreoffice_lock_files(self, tmp_path: Path) -> None:
        """find_files() should exclude LibreOffice lock files (.~lock.*) by default."""
        (tmp_path / ".~lock.data.csv#").write_text("lock")
        (tmp_path / "data.csv").write_text("real")

        files = find_files(tmp_path, None, None, recursive=True)
        assert len(files) == 1
        assert files[0].name == "data.csv"

    def test_find_files_excludes_default_dirs_with_fs(self, tmp_path: Path) -> None:
        """find_files() with FileSystem should exclude default dirs."""
        venv_dir = tmp_path / ".venv"
        venv_dir.mkdir()
        (venv_dir / "config.csv").write_text("a")
        (tmp_path / "data.csv").write_text("b")

        fs = FileSystem(tmp_path)
        files = find_files(tmp_path, None, None, recursive=True, fs=fs)
        assert len(files) == 1
        assert files[0].name == "data.csv"


class TestRemoteScanWithEnsureLocal:
    """Test scan_file with ensure_local for remote filesystems."""

    def test_scan_csv_with_remote_fs(self, tmp_path: Path) -> None:
        """scan_file should use ensure_local for remote CSV files."""
        from unittest.mock import MagicMock
        from datannurpy.scanner.scan import scan_file

        # Create a test CSV file
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("name,age\nAlice,30\nBob,25\n")

        # Mock a non-local FileSystem
        mock_fs = MagicMock()
        mock_fs.is_local = False
        mock_fs.ensure_local.return_value.__enter__ = MagicMock(return_value=csv_file)
        mock_fs.ensure_local.return_value.__exit__ = MagicMock(return_value=None)

        result = scan_file(
            Path("/remote/test.csv"),
            "csv",
            dataset_id="test",
            fs=mock_fs,
        )

        mock_fs.ensure_local.assert_called_once()
        assert len(result.variables) == 2
        assert result.nb_row == 2

    def test_scan_excel_with_remote_fs(self, tmp_path: Path) -> None:
        """scan_file should use ensure_local for remote Excel files."""
        from unittest.mock import MagicMock
        from datannurpy.scanner.scan import scan_file
        import openpyxl

        # Create a test Excel file
        xlsx_file = tmp_path / "test.xlsx"
        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append(["name", "age"])
        ws.append(["Alice", 30])
        wb.save(xlsx_file)

        # Mock a non-local FileSystem
        mock_fs = MagicMock()
        mock_fs.is_local = False
        mock_fs.ensure_local.return_value.__enter__ = MagicMock(return_value=xlsx_file)
        mock_fs.ensure_local.return_value.__exit__ = MagicMock(return_value=None)

        result = scan_file(
            Path("/remote/test.xlsx"),
            "excel",
            dataset_id="test",
            fs=mock_fs,
        )

        mock_fs.ensure_local.assert_called_once()
        assert len(result.variables) == 2
        assert result.nb_row == 1

    def test_scan_schema_only_with_remote_fs(self, tmp_path: Path) -> None:
        """scan_file schema_only should stream header line for remote CSV."""
        from io import BytesIO
        from unittest.mock import MagicMock

        from datannurpy.scanner.scan import scan_file

        # Mock a non-local FileSystem that returns header bytes via open()
        mock_fs = MagicMock()
        mock_fs.is_local = False
        mock_fs._full_path.return_value = "/remote/test.csv"
        mock_fs.fs.open.return_value.__enter__ = MagicMock(
            return_value=BytesIO(b"name,age\n")
        )
        mock_fs.fs.open.return_value.__exit__ = MagicMock(return_value=None)

        result = scan_file(
            Path("/remote/test.csv"),
            "csv",
            dataset_id="test",
            schema_only=True,
            fs=mock_fs,
        )

        mock_fs.fs.open.assert_called_once()
        assert len(result.variables) == 2
        assert result.nb_row is None


class TestRemoteScanStatistical:
    """Test scanning statistical files (SAS/SPSS/Stata) with remote filesystem."""

    @pytest.fixture
    def sas_file(self, tmp_path: Path) -> Path:
        """Path to real SAS test file."""
        test_data = Path(__file__).parent.parent.parent / "data" / "cars.sas7bdat"
        if not test_data.exists():
            pytest.skip("SAS test file not found")
        return test_data

    def test_scan_sas_with_remote_fs(self, sas_file: Path) -> None:
        """scan_file should use ensure_local for remote SAS files."""
        pytest.importorskip("pyreadstat")
        from unittest.mock import MagicMock
        from datannurpy.scanner.scan import scan_file

        # Mock a non-local FileSystem
        mock_fs = MagicMock()
        mock_fs.is_local = False
        mock_fs.ensure_local.return_value.__enter__ = MagicMock(return_value=sas_file)
        mock_fs.ensure_local.return_value.__exit__ = MagicMock(return_value=None)

        result = scan_file(
            Path("/remote/cars.sas7bdat"),
            "sas",
            dataset_id="test",
            fs=mock_fs,
        )

        mock_fs.ensure_local.assert_called_once()
        assert len(result.variables) > 0
        assert result.nb_row is not None
