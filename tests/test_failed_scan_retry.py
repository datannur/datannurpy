"""Versioned retry of failed scans (``Dataset.scan_failed_version``).

A dataset whose scan logged errors is stamped with the datannurpy version; the
incremental skip re-scans it once per new release — so scanner fixes reach
previously failed sources without a manual touch — and skips it again while the
version (and the source) stays the same.
"""

from __future__ import annotations

from pathlib import Path, PurePath
from typing import Any
from unittest.mock import patch

import pytest

from datannurpy import Catalog, EntityMetadata
from datannurpy.scanner.discovery import DatasetInfo, compute_scan_plan
from datannurpy.scanner.scan import scan_file as _original_scan_file
from datannurpy.schema import Dataset
from datannurpy.utils import timestamp_to_iso
from datannurpy.utils.log import log_error
from datannurpy.utils.version import scanner_version


def _soft_failing_scan_file(*args: Any, **kwargs: Any) -> Any:
    """Scan normally but log a ✗, like a scanner-internal degraded failure."""
    result = _original_scan_file(*args, **kwargs)
    log_error("member", RuntimeError("simulated internal failure"), True)
    return result


def _write_csv(path: Path, name: str = "data.csv") -> Path:
    p = path / name
    p.write_text("a,b\n1,2\n3,4\n")
    return p


class TestFolderScanRetry:
    def test_soft_failure_stamped_and_retried_on_new_version(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_csv(tmp_path)
        catalog = Catalog(quiet=True)
        with patch(
            "datannurpy.add_folder.scan_file", side_effect=_soft_failing_scan_file
        ):
            catalog.add_folder(tmp_path, metadata=EntityMetadata(id="t", name="T"))
        ds = catalog.dataset.all()[0]
        assert ds.scan_failed_version == scanner_version()

        # Same version, unchanged file: skipped, the stamp is remembered.
        catalog.add_folder(tmp_path, metadata=EntityMetadata(id="t", name="T"))
        assert catalog.dataset.all()[0].scan_failed_version == scanner_version()

        # A new release re-scans it; the clean scan clears the stamp.
        monkeypatch.setattr("datannurpy.utils.version._VERSION", "999.0.0")
        catalog.add_folder(tmp_path, metadata=EntityMetadata(id="t", name="T"))
        assert catalog.dataset.all()[0].scan_failed_version is None

    def test_stamp_survives_the_scan_cache(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        app_dir = tmp_path / "app"
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_csv(data_dir)

        catalog1 = Catalog(app_path=app_dir, quiet=True)
        with patch(
            "datannurpy.add_folder.scan_file", side_effect=_soft_failing_scan_file
        ):
            catalog1.add_folder(data_dir, metadata=EntityMetadata(id="t", name="T"))
        catalog1.export_db()

        # Same version: a fresh run reloads the stamp and still skips.
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_folder(data_dir, metadata=EntityMetadata(id="t", name="T"))
        assert catalog2.dataset.all()[0].scan_failed_version == scanner_version()
        catalog2.export_db()

        # A new release re-scans across processes, not just in-session.
        monkeypatch.setattr("datannurpy.utils.version._VERSION", "999.0.0")
        catalog3 = Catalog(app_path=app_dir, quiet=True)
        catalog3.add_folder(data_dir, metadata=EntityMetadata(id="t", name="T"))
        assert catalog3.dataset.all()[0].scan_failed_version is None

    def test_time_series_soft_failure_stamped(self, tmp_path: Path) -> None:
        (tmp_path / "sales_2023.csv").write_text("a,b\n1,2\n")
        (tmp_path / "sales_2024.csv").write_text("a,b\n3,4\n")
        catalog = Catalog(quiet=True)
        with patch(
            "datannurpy.add_folder.scan_file", side_effect=_soft_failing_scan_file
        ):
            catalog.add_folder(tmp_path, metadata=EntityMetadata(id="t", name="T"))
        ds = catalog.dataset.all()[0]
        assert ds.nb_resources == 2
        assert ds.scan_failed_version == scanner_version()

    def test_compute_scan_plan_retries_stale_failure(self, tmp_path: Path) -> None:
        csv = _write_csv(tmp_path)
        mtime = int(csv.stat().st_mtime)
        info = DatasetInfo(path=PurePath(csv), format="csv", mtime=mtime)

        def _catalog_with(version: str | None) -> Catalog:
            catalog = Catalog(quiet=True)
            catalog.dataset.add(
                Dataset(
                    id="d",
                    last_update_date=timestamp_to_iso(mtime),
                    scan_failed_version=version,
                    _match_path=str(csv),
                )
            )
            return catalog

        plan = compute_scan_plan([info], _catalog_with("0.0.1"), refresh=False)
        assert plan.to_scan == [info]  # stale failure: retried
        plan = compute_scan_plan([info], _catalog_with(scanner_version()), False)
        assert plan.to_skip == [info]  # already failed under this version
        plan = compute_scan_plan([info], _catalog_with(None), refresh=False)
        assert plan.to_skip == [info]  # clean dataset: plain mtime skip


class TestAddDatasetRetry:
    def test_soft_failure_stamped_and_retried_on_new_version(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        csv = _write_csv(tmp_path)
        catalog = Catalog(quiet=True)
        with patch(
            "datannurpy.add_dataset.scan_file", side_effect=_soft_failing_scan_file
        ):
            catalog.add_dataset(csv)
        assert catalog.dataset.all()[0].scan_failed_version == scanner_version()

        catalog.add_dataset(csv)  # same version: skip_unchanged keeps the skip
        assert catalog.dataset.all()[0].scan_failed_version == scanner_version()

        monkeypatch.setattr("datannurpy.utils.version._VERSION", "999.0.0")
        catalog.add_dataset(csv)
        assert catalog.dataset.all()[0].scan_failed_version is None


class TestAddDatabaseRetry:
    def test_degraded_table_retried_on_new_version(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import ibis

        con = ibis.duckdb.connect(str(tmp_path / "test.db"))
        con.raw_sql("CREATE TABLE t1 (a INT); INSERT INTO t1 VALUES (1)")
        state = {"fail": True}

        from datannurpy.scanner.database import scan_table as original

        def flaky(*args: Any, **kwargs: Any) -> Any:
            if state["fail"] and kwargs.get("infer_stats", True):
                raise RuntimeError("simulated aggregate failure")
            return original(*args, **kwargs)

        catalog = Catalog(quiet=True)
        with patch("datannurpy.scanner.database.scan_table", side_effect=flaky):
            catalog.add_database(con)
        ds = catalog.dataset.all()[0]
        assert ds.scan_failed_version == scanner_version()
        assert ds.nb_row == 1  # the row count survives the degrade

        # Same version, unchanged table: stays skipped (and stamped).
        state["fail"] = False
        catalog.add_database(con)
        assert catalog.dataset.all()[0].scan_failed_version == scanner_version()

        # A new release retries: the healthy scan clears the stamp.
        monkeypatch.setattr("datannurpy.utils.version._VERSION", "999.0.0")
        catalog.add_database(con)
        ds = catalog.dataset.all()[0]
        assert ds.scan_failed_version is None
        stats = {v.name: v for v in catalog.variable.all()}
        assert stats["a"].nb_distinct == 1
        con.disconnect()


class TestAddGeodatabaseRetry:
    def test_layer_error_stamped(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from .conftest import empty_geo_scan

        (tmp_path / "store.gdb").mkdir()
        monkeypatch.setattr(
            "datannurpy.scanner.geo_vector.list_geo_layers", lambda p: ["roads"]
        )
        monkeypatch.setattr(
            "datannurpy.scanner.geo_vector.scan_geo_vector", empty_geo_scan
        )
        catalog = Catalog(quiet=True)
        catalog.add_geodatabase(str(tmp_path / "store.gdb"))
        ds = catalog.dataset.all()[0]
        assert ds.scan_failed_version == scanner_version()
        assert catalog.run_errors == 1
