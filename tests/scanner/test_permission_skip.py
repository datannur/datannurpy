"""Tests for permission-tolerant traversal helpers."""

from __future__ import annotations

import errno
import os
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from datannurpy.scanner.parquet.discovery import (
    discover_parquet_datasets,
    is_delta_table,
    is_hive_partitioned,
    is_iceberg_table,
)
from datannurpy.scanner.utils import (
    _is_permission_error,
    find_files,
    get_dir_data_size,
    safe_glob_fs,
    safe_glob_local,
    safe_is_dir_fs,
    safe_is_file_fs,
    safe_iterdir_detailed_fs,
    safe_iterdir_fs,
    safe_iterdir_local,
    safe_walk_fs,
    safe_walk_local,
)
import contextlib


def _make_unreadable_dir(parent: Path, name: str = "no_read") -> Path:
    """Create a subdirectory that cannot be listed by the current user."""
    target = parent / name
    target.mkdir()
    # Drop read+exec; keep traversal disabled too
    os.chmod(target, 0o000)
    return target


@pytest.fixture
def restore_perms() -> Any:
    """Ensure 0o000 directories are restored so pytest can clean tmp_path."""
    created: list[Path] = []
    yield created
    for p in created:
        if p.exists():
            with contextlib.suppress(OSError):
                os.chmod(p, 0o700)


class TestIsPermissionError:
    """Test _is_permission_error helper."""

    def test_permission_error_is_detected(self) -> None:
        assert _is_permission_error(PermissionError("nope"))

    def test_oserror_eacces_is_detected(self) -> None:
        exc = OSError(errno.EACCES, "denied")
        assert _is_permission_error(exc)

    def test_oserror_eperm_is_detected(self) -> None:
        exc = OSError(errno.EPERM, "denied")
        assert _is_permission_error(exc)

    def test_plain_oserror_with_eacces_errno_is_detected(self) -> None:
        # Bypass Python's auto-promotion to PermissionError by constructing an
        # OSError and setting errno after the fact.
        exc = OSError("denied")
        exc.errno = errno.EACCES
        assert _is_permission_error(exc)

    def test_other_oserror_is_not(self) -> None:
        exc = OSError(errno.ENOENT, "missing")
        assert not _is_permission_error(exc)

    def test_random_exception_is_not(self) -> None:
        assert not _is_permission_error(ValueError("x"))


class TestSafeIterdirFs:
    """fs.iterdir wrapper."""

    def test_yields_entries_normally(self) -> None:
        fs = MagicMock()
        fs.iterdir.return_value = iter(["/a/x", "/a/y"])
        assert list(safe_iterdir_fs(fs, "/a")) == ["/a/x", "/a/y"]

    def test_swallows_permission_error(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        fs = MagicMock()

        def boom(_path: str) -> Any:
            raise PermissionError("denied")
            yield  # pragma: no cover

        fs.iterdir.side_effect = boom
        assert list(safe_iterdir_fs(fs, "/locked")) == []
        captured = capsys.readouterr()
        assert "permission denied" in captured.err
        assert "/locked" in captured.err

    def test_swallows_oserror_eacces(self) -> None:
        fs = MagicMock()

        def boom(_path: str) -> Any:
            raise OSError(errno.EACCES, "denied")
            yield  # pragma: no cover

        fs.iterdir.side_effect = boom
        assert list(safe_iterdir_fs(fs, "/locked")) == []

    def test_reraises_unrelated_oserror(self) -> None:
        fs = MagicMock()

        def boom(_path: str) -> Any:
            raise OSError(errno.ENOENT, "missing")
            yield  # pragma: no cover

        fs.iterdir.side_effect = boom
        with pytest.raises(OSError, match="missing"):
            list(safe_iterdir_fs(fs, "/missing"))


class TestSafeIterdirDetailedFs:
    """fs.iterdir_detailed wrapper (same EACCES tolerance, carries entry info)."""

    def test_yields_entries_normally(self) -> None:
        fs = MagicMock()
        fs.iterdir_detailed.return_value = iter([("/a/x", {"type": "file"})])
        assert list(safe_iterdir_detailed_fs(fs, "/a")) == [("/a/x", {"type": "file"})]

    def test_swallows_permission_error(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        fs = MagicMock()

        def boom(_path: str) -> Any:
            raise PermissionError("denied")
            yield  # pragma: no cover

        fs.iterdir_detailed.side_effect = boom
        assert list(safe_iterdir_detailed_fs(fs, "/locked")) == []
        assert "permission denied" in capsys.readouterr().err

    def test_reraises_unrelated_oserror(self) -> None:
        fs = MagicMock()

        def boom(_path: str) -> Any:
            raise OSError(errno.ENOENT, "missing")
            yield  # pragma: no cover

        fs.iterdir_detailed.side_effect = boom
        with pytest.raises(OSError, match="missing"):
            list(safe_iterdir_detailed_fs(fs, "/missing"))


class TestSafeIterdirLocal:
    """Path.iterdir wrapper."""

    def test_yields_children(self, tmp_path: Path) -> None:
        (tmp_path / "a.txt").write_text("a")
        (tmp_path / "b.txt").write_text("b")
        names = sorted(p.name for p in safe_iterdir_local(tmp_path))
        assert names == ["a.txt", "b.txt"]

    def test_skips_unreadable_dir(
        self,
        tmp_path: Path,
        restore_perms: list[Path],
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        locked = _make_unreadable_dir(tmp_path)
        restore_perms.append(locked)
        if os.access(locked, os.R_OK):
            pytest.skip("cannot make dir unreadable in this environment")
        assert list(safe_iterdir_local(locked)) == []
        assert "permission denied" in capsys.readouterr().err

    def test_reraises_unrelated_oserror(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fake_iterdir(_self: Path) -> Any:
            raise OSError(errno.ENOENT, "missing")

        monkeypatch.setattr(Path, "iterdir", fake_iterdir)
        with pytest.raises(OSError):
            list(safe_iterdir_local(tmp_path))


class TestSafeFsTypeChecks:
    """fs.isdir/fs.isfile wrappers."""

    def test_safe_is_dir_fs_returns_result(self) -> None:
        fs = MagicMock()
        fs.isdir.return_value = True
        assert safe_is_dir_fs(fs, "/data") is True

    def test_safe_is_file_fs_returns_result(self) -> None:
        fs = MagicMock()
        fs.isfile.return_value = False
        assert safe_is_file_fs(fs, "/data") is False

    def test_safe_is_dir_fs_swallows_permission_error(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        fs = MagicMock()
        fs.isdir.side_effect = PermissionError("denied")
        assert safe_is_dir_fs(fs, "/locked") is False
        assert "permission denied" in capsys.readouterr().err

    def test_safe_is_file_fs_swallows_permission_error(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        fs = MagicMock()
        fs.isfile.side_effect = PermissionError("denied")
        assert safe_is_file_fs(fs, "/locked") is False
        assert "permission denied" in capsys.readouterr().err

    def test_safe_is_dir_fs_reraises_unrelated_error(self) -> None:
        fs = MagicMock()
        fs.isdir.side_effect = OSError(errno.ENOENT, "missing")
        with pytest.raises(OSError):
            safe_is_dir_fs(fs, "/missing")

    def test_safe_is_file_fs_reraises_unrelated_error(self) -> None:
        fs = MagicMock()
        fs.isfile.side_effect = OSError(errno.ENOENT, "missing")
        with pytest.raises(OSError):
            safe_is_file_fs(fs, "/missing")


class TestSafeGlobFs:
    """fs.glob wrapper."""

    def test_returns_results(self) -> None:
        fs = MagicMock()
        fs.glob.return_value = ["/a/x.parquet"]
        assert safe_glob_fs(fs, "/a/*.parquet") == ["/a/x.parquet"]

    def test_swallows_permission_error(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        fs = MagicMock()
        fs.glob.side_effect = PermissionError("denied")
        assert safe_glob_fs(fs, "/x/**/*.pq") == []
        assert "permission denied" in capsys.readouterr().err

    def test_reraises_unrelated_error(self) -> None:
        fs = MagicMock()
        fs.glob.side_effect = OSError(errno.ENOENT, "missing")
        with pytest.raises(OSError):
            safe_glob_fs(fs, "/x")


class TestSafeGlobLocal:
    """Path.glob wrapper."""

    def test_returns_matches(self, tmp_path: Path) -> None:
        (tmp_path / "a.csv").write_text("x")
        (tmp_path / "b.csv").write_text("x")
        results = safe_glob_local(tmp_path, "*.csv")
        assert sorted(p.name for p in results) == ["a.csv", "b.csv"]

    def test_swallows_permission_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        # Pathlib in some Python versions silently ignores per-dir permission
        # errors; patch Path.glob to be sure our wrapper handles a raise.
        def fake_glob(_self: Path, _pattern: str) -> Any:
            raise PermissionError("denied")

        monkeypatch.setattr(Path, "glob", fake_glob)
        assert safe_glob_local(tmp_path, "*") == []
        assert "permission denied" in capsys.readouterr().err

    def test_reraises_unrelated_oserror(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fake_glob(_self: Path, _pattern: str) -> Any:
            raise OSError(errno.ENOENT, "missing")

        monkeypatch.setattr(Path, "glob", fake_glob)
        with pytest.raises(OSError):
            safe_glob_local(tmp_path, "*")


class _FakeScandir:
    """Minimal stand-in for an ``os.scandir`` result: iterable + context manager."""

    def __init__(self, entries: list[Any]) -> None:
        self._entries = entries

    def __iter__(self) -> Any:
        return iter(self._entries)

    def __enter__(self) -> _FakeScandir:
        return self

    def __exit__(self, *_exc: Any) -> bool:
        return False


class _FakeEntry:
    """Minimal ``os.DirEntry`` stand-in whose ``is_dir`` raises a chosen error."""

    def __init__(self, path: str, error: OSError) -> None:
        self.name = path.rsplit("/", 1)[-1]
        self.path = path
        self._error = error

    def is_dir(self) -> bool:
        raise self._error

    def is_symlink(self) -> bool:  # pragma: no cover - not reached before is_dir
        return False


class TestSafeWalkLocal:
    """scandir-based recursive file iterator."""

    def test_walks_all_files(self, tmp_path: Path) -> None:
        (tmp_path / "a.txt").write_text("x")
        (tmp_path / "sub").mkdir()
        (tmp_path / "sub" / "b.txt").write_text("x")
        names = sorted(p.name for p in safe_walk_local(tmp_path))
        assert names == ["a.txt", "b.txt"]

    def test_skips_unreadable_subtree(
        self,
        tmp_path: Path,
        restore_perms: list[Path],
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        (tmp_path / "ok.txt").write_text("x")
        locked = _make_unreadable_dir(tmp_path, "locked")
        restore_perms.append(locked)
        if os.access(locked, os.R_OK):
            pytest.skip("cannot make dir unreadable in this environment")
        names = sorted(p.name for p in safe_walk_local(tmp_path))
        assert "ok.txt" in names
        assert "permission denied" in capsys.readouterr().err

    def test_skips_default_excluded_dirs(self, tmp_path: Path) -> None:
        (tmp_path / "a.txt").write_text("x")
        (tmp_path / "__pycache__").mkdir()
        (tmp_path / "__pycache__" / "skip.txt").write_text("x")
        names = sorted(p.name for p in safe_walk_local(tmp_path))
        assert names == ["a.txt"]

    def test_reraises_unrelated_oserror(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A non-permission scandir failure aborts the walk instead of being skipped.
        def boom(_root: Any) -> Any:
            raise OSError(errno.ENOENT, "missing")

        monkeypatch.setattr(os, "scandir", boom)
        with pytest.raises(OSError, match="missing"):
            list(safe_walk_local(tmp_path))

    def test_skips_entry_whose_stat_denies_permission(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        entry = _FakeEntry(str(tmp_path / "weird"), PermissionError("denied"))
        monkeypatch.setattr(os, "scandir", lambda _p: _FakeScandir([entry]))
        assert list(safe_walk_local(tmp_path)) == []
        assert "permission denied" in capsys.readouterr().err

    def test_reraises_entry_stat_unrelated_oserror(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        entry = _FakeEntry(str(tmp_path / "weird"), OSError(errno.ENOENT, "missing"))
        monkeypatch.setattr(os, "scandir", lambda _p: _FakeScandir([entry]))
        with pytest.raises(OSError, match="missing"):
            list(safe_walk_local(tmp_path))


class TestSafeWalkFs:
    """iterdir_detailed-based recursive file iterator."""

    def test_skips_directories_when_not_recursive(self) -> None:
        fs = MagicMock()
        fs.iterdir_detailed.return_value = [
            ("/data/subdir", {"type": "directory"}),
            ("/data/file.csv", {"type": "file"}),
        ]

        assert list(safe_walk_fs(fs, "/data", recursive=False)) == ["/data/file.csv"]

    def test_classifies_from_listing_without_per_entry_stat(self) -> None:
        """The listing already carries the type, so a file/dir walk must not issue
        an isdir/isfile round-trip per entry (the whole point on SFTP/NAS)."""
        fs = MagicMock()
        fs.iterdir_detailed.side_effect = lambda path: {
            "/data": [
                ("/data/sub", {"type": "directory"}),
                ("/data/a.csv", {"type": "file"}),
            ],
            "/data/sub": [("/data/sub/b.csv", {"type": "file"})],
        }[path]

        assert sorted(safe_walk_fs(fs, "/data", recursive=True)) == [
            "/data/a.csv",
            "/data/sub/b.csv",
        ]
        fs.isdir.assert_not_called()
        fs.isfile.assert_not_called()

    def test_resolves_ambiguous_entry_type_with_stat(self) -> None:
        """A symlink (or any type the listing does not mark dir/file) falls back to
        isdir/isfile so its target is followed as before."""
        fs = MagicMock()
        fs.iterdir_detailed.return_value = [("/data/link", {"type": "link"})]
        fs.isdir.return_value = False
        fs.isfile.return_value = True

        assert list(safe_walk_fs(fs, "/data", recursive=True)) == ["/data/link"]

    def test_resolves_ambiguous_entry_to_dir_and_recurses(self) -> None:
        """A symlink pointing at a directory is descended, as isdir did before."""
        fs = MagicMock()
        fs.iterdir_detailed.side_effect = lambda path: {
            "/data": [("/data/link", {"type": "link"})],
            "/data/link": [("/data/link/f.csv", {"type": "file"})],
        }[path]
        fs.isdir.return_value = True
        fs.isfile.return_value = False

        assert list(safe_walk_fs(fs, "/data", recursive=True)) == ["/data/link/f.csv"]

    def test_skips_entries_that_are_neither_dir_nor_file(self) -> None:
        fs = MagicMock()
        fs.iterdir_detailed.return_value = [("/data/socket", {"type": "other"})]
        fs.isdir.return_value = False
        fs.isfile.return_value = False

        assert list(safe_walk_fs(fs, "/data", recursive=True)) == []


class TestFindFilesWithUnreadableDir:
    """find_files should not abort when a subdir is unreadable."""

    def test_local_no_include(
        self,
        tmp_path: Path,
        restore_perms: list[Path],
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        (tmp_path / "ok.csv").write_text("a,b\n1,2\n")
        locked = _make_unreadable_dir(tmp_path, "locked")
        restore_perms.append(locked)
        if os.access(locked, os.R_OK):
            pytest.skip("cannot make dir unreadable in this environment")
        result = find_files(tmp_path, None, None, recursive=True)
        names = sorted(Path(p).name for p in result)
        assert names == ["ok.csv"]
        assert "permission denied" in capsys.readouterr().err

    def test_local_with_include_pattern(
        self,
        tmp_path: Path,
        restore_perms: list[Path],
    ) -> None:
        (tmp_path / "ok.csv").write_text("a,b\n1,2\n")
        locked = _make_unreadable_dir(tmp_path, "locked")
        restore_perms.append(locked)
        if os.access(locked, os.R_OK):
            pytest.skip("cannot make dir unreadable in this environment")
        # Should not raise even with a glob that descends into locked
        result = find_files(tmp_path, ["*.csv"], None, recursive=True)
        assert any(Path(p).name == "ok.csv" for p in result)


class TestParquetDiscoveryWithUnreadableDir:
    """is_* probes and discover_parquet_datasets should be permission-tolerant."""

    def test_is_hive_partitioned_skips_unreadable(
        self,
        tmp_path: Path,
        restore_perms: list[Path],
    ) -> None:
        locked = _make_unreadable_dir(tmp_path, "locked")
        restore_perms.append(locked)
        if os.access(locked, os.R_OK):
            pytest.skip("cannot make dir unreadable in this environment")
        # Should not raise
        assert is_hive_partitioned(tmp_path) is False

    def test_is_delta_table_handles_unreadable(self, tmp_path: Path) -> None:
        # Plain directory: no _delta_log -> returns False without raising
        assert is_delta_table(tmp_path) is False

    def test_is_iceberg_table_handles_unreadable(self, tmp_path: Path) -> None:
        assert is_iceberg_table(tmp_path) is False

    def test_discover_parquet_skips_unreadable_subtree(
        self,
        tmp_path: Path,
        restore_perms: list[Path],
    ) -> None:
        # A normal parquet sibling should still be found.
        import pyarrow as pa
        import pyarrow.parquet as pq

        pq.write_table(pa.table({"x": [1, 2]}), tmp_path / "ok.parquet")
        locked = _make_unreadable_dir(tmp_path, "locked")
        restore_perms.append(locked)
        if os.access(locked, os.R_OK):
            pytest.skip("cannot make dir unreadable in this environment")
        result = discover_parquet_datasets(tmp_path)
        names = sorted(Path(d.path).name for d in result.datasets)
        assert "ok.parquet" in names


class TestGetDirDataSizeWithUnreadable:
    """get_dir_data_size must tolerate permission errors."""

    def test_local(
        self,
        tmp_path: Path,
        restore_perms: list[Path],
    ) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        pq.write_table(pa.table({"x": [1]}), tmp_path / "ok.parquet")
        locked = _make_unreadable_dir(tmp_path, "locked")
        restore_perms.append(locked)
        if os.access(locked, os.R_OK):
            pytest.skip("cannot make dir unreadable in this environment")
        size = get_dir_data_size(tmp_path)
        assert size > 0
