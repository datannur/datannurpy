"""Tests for scan error resilience (continue on error)."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from datannurpy import Catalog, Folder

DATA_DIR = Path(__file__).parent.parent / "data"


def _fail_nth(target: str, n: int = 1):
    """Create a side_effect that raises on the nth call, delegates otherwise."""
    original = getattr(
        __import__(target.rsplit(".", 1)[0], fromlist=[target.rsplit(".", 1)[1]]),
        target.rsplit(".", 1)[1],
    )
    state = {"count": 0}

    def side_effect(*args: Any, **kwargs: Any) -> Any:
        state["count"] += 1
        if state["count"] == n:
            raise RuntimeError("simulated error")
        return original(*args, **kwargs)

    return side_effect


class TestAddFolderScanError:
    """add_folder should continue scanning after a file error."""

    def test_continues_and_reports_errors(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        (tmp_path / "a.csv").write_text("x\n1")
        (tmp_path / "b.csv").write_text("y\n2")

        catalog = Catalog(quiet=False)
        with patch(
            "datannurpy.add_folder.scan_file",
            side_effect=_fail_nth("datannurpy.scanner.scan.scan_file"),
        ):
            catalog.add_folder(tmp_path, Folder(id="t", name="T"))

        assert catalog.dataset.count == 1
        assert "1 error" in capsys.readouterr().err

    def test_preserves_old_dataset_on_rescan_error(self, tmp_path: Path) -> None:
        (tmp_path / "data.csv").write_text("x\n1\n2")
        catalog = Catalog(quiet=True)
        catalog.add_folder(tmp_path, Folder(id="t", name="T"))

        with patch(
            "datannurpy.add_folder.scan_file", side_effect=RuntimeError("corrupt")
        ):
            catalog.add_folder(tmp_path, Folder(id="t", name="T"), refresh=True)

        assert catalog.dataset.count == 1

    def test_time_series_error_continues(self) -> None:
        catalog = Catalog(quiet=True)
        with patch(
            "datannurpy.add_folder._scan_time_series", side_effect=RuntimeError("bad")
        ):
            catalog.add_folder(
                DATA_DIR / "timeseries" / "yearly", Folder(id="ts", name="TS")
            )
        assert catalog.dataset.count == 0


class TestAddDatabaseScanError:
    """add_database should continue scanning after a table error."""

    @pytest.fixture()
    def duckdb_con(self, tmp_path: Path):
        import ibis

        con = ibis.duckdb.connect(str(tmp_path / "test.db"))
        con.raw_sql("CREATE TABLE t1 (a INT); INSERT INTO t1 VALUES (1)")
        con.raw_sql("CREATE TABLE t2 (b INT); INSERT INTO t2 VALUES (2)")
        yield con
        con.disconnect()

    def test_continues_after_scan_table_error(self, duckdb_con) -> None:  # type: ignore[no-untyped-def]
        catalog = Catalog(quiet=True)
        with patch(
            "datannurpy.add_database.scan_table",
            side_effect=_fail_nth("datannurpy.scanner.database.scan_table"),
        ):
            catalog.add_database(duckdb_con)
        assert catalog.dataset.count == 1

    def test_continues_after_scan_table_error_schema_mode(self, duckdb_con) -> None:  # type: ignore[no-untyped-def]
        catalog = Catalog(quiet=True)
        with patch(
            "datannurpy.add_database.scan_table",
            side_effect=_fail_nth("datannurpy.scanner.database.scan_table"),
        ):
            catalog.add_database(duckdb_con, depth="schema")
        assert catalog.dataset.count == 1

    def test_continues_after_signature_error(self, duckdb_con) -> None:  # type: ignore[no-untyped-def]
        catalog = Catalog(quiet=True)
        with patch(
            "datannurpy.add_database.compute_schema_signature",
            side_effect=_fail_nth(
                "datannurpy.scanner.database.compute_schema_signature"
            ),
        ):
            catalog.add_database(duckdb_con)
        assert catalog.dataset.count == 1

    def test_structure_mode_rescan(self, duckdb_con) -> None:  # type: ignore[no-untyped-def]
        catalog = Catalog(quiet=True)
        catalog.add_database(duckdb_con, depth="structure")
        catalog.add_database(duckdb_con, depth="structure", refresh=True)
        assert catalog.dataset.count == 2
