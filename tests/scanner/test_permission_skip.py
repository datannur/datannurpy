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
    safe_iterdir_fs,
    safe_iterdir_local,
    safe_walk_local,
)


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
            try:
                os.chmod(p, 0o700)
            except OSError:
                pass


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


class TestSafeWalkLocal:
    """os.walk-based recursive file iterator."""

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
        # Force os.walk to invoke onerror with a non-permission OSError.
        def fake_walk(_root: Any, onerror: Any = None, **_kw: Any) -> Any:
            onerror(OSError(errno.ENOENT, "missing"))
            return iter([])

        monkeypatch.setattr(os, "walk", fake_walk)
        with pytest.raises(OSError):
            list(safe_walk_local(tmp_path))


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
