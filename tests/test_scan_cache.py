"""Tests for the ``_scan`` cache: the scan-derived base separated from the export.

The final DB is a disposable materialization of the current scan plus the current
metadata. These tests lock in the semantics that motivated the cache:

- an emptied (or removed) metadata cell falls back to the scanned value instead of
  leaving a stale overlay behind;
- the final DB can be deleted and rebuilt from ``_scan`` + metadata unchanged;
- the cache stays tidy as the scan base shrinks.
"""

from __future__ import annotations

import json
from pathlib import Path

from datannurpy import Catalog
from datannurpy.scan_cache import scan_cache_dir, scan_cache_load_path, write_scan_cache
from datannurpy.schema import Folder


def _dataset_field(db_dir: Path, field: str) -> dict[str, object]:
    rows = json.loads((db_dir / "dataset.json").read_text())
    return {row["id"]: row.get(field) for row in rows}


def _build_source(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Return (data_dir, meta_dir, app_dir) with one scannable CSV."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "sales.csv").write_text("a,b\n1,2\n3,4\n")
    meta_dir = tmp_path / "meta"
    meta_dir.mkdir()
    return data_dir, meta_dir, tmp_path / "app"


class TestEmptyCellSemantics:
    """An emptied metadata contribution reverts to the scanned value."""

    def test_emptied_cell_falls_back_to_scanned_value(self, tmp_path: Path):
        data_dir, meta_dir, app_dir = _build_source(tmp_path)
        db_dir = app_dir / "data" / "db"

        # Run 1: metadata overrides the scanned dataset name.
        (meta_dir / "dataset.csv").write_text("id,name\ndata---sales_csv,OVERRIDDEN\n")
        catalog = Catalog(app_path=app_dir, metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data_dir)
        catalog.export_db(quiet=True)
        assert _dataset_field(db_dir, "name") == {"data---sales_csv": "OVERRIDDEN"}
        # The pristine scanned name is kept in the cache, not the overlay.
        assert _dataset_field(scan_cache_dir(db_dir), "name") == {
            "data---sales_csv": "sales"
        }

        # Run 2: the override is cleared (empty cell) -> scanned value shows through.
        (meta_dir / "dataset.csv").write_text("id,name\ndata---sales_csv,\n")
        catalog2 = Catalog(app_path=app_dir, metadata_path=meta_dir, quiet=True)
        catalog2.add_folder(data_dir)
        catalog2.export_db(quiet=True)
        assert _dataset_field(db_dir, "name") == {"data---sales_csv": "sales"}

    def test_removed_column_falls_back_to_scanned_value(self, tmp_path: Path):
        data_dir, meta_dir, app_dir = _build_source(tmp_path)
        db_dir = app_dir / "data" / "db"

        (meta_dir / "dataset.csv").write_text("id,name\ndata---sales_csv,OVERRIDDEN\n")
        catalog = Catalog(app_path=app_dir, metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data_dir)
        catalog.export_db(quiet=True)

        # Run 2: the name column is gone entirely (missing column == empty cell).
        (meta_dir / "dataset.csv").write_text("id\ndata---sales_csv\n")
        catalog2 = Catalog(app_path=app_dir, metadata_path=meta_dir, quiet=True)
        catalog2.add_folder(data_dir)
        catalog2.export_db(quiet=True)
        assert _dataset_field(db_dir, "name") == {"data---sales_csv": "sales"}

    def test_bang_still_hard_clears_below_the_scan(self, tmp_path: Path):
        data_dir, meta_dir, app_dir = _build_source(tmp_path)
        db_dir = app_dir / "data" / "db"

        # "!" suppresses the final value even though the scan provides one.
        (meta_dir / "dataset.csv").write_text("id,name\ndata---sales_csv,!\n")
        catalog = Catalog(app_path=app_dir, metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data_dir)
        catalog.export_db(quiet=True)
        assert _dataset_field(db_dir, "name") == {"data---sales_csv": None}
        # The scan base still holds the scanned value underneath.
        assert _dataset_field(scan_cache_dir(db_dir), "name") == {
            "data---sales_csv": "sales"
        }


class TestDisposability:
    """The final DB is a rebuildable artifact; only _scan is the source of truth."""

    def test_final_db_rebuilds_from_scan_plus_metadata(self, tmp_path: Path):
        data_dir, meta_dir, app_dir = _build_source(tmp_path)
        db_dir = app_dir / "data" / "db"
        (meta_dir / "dataset.csv").write_text(
            "id,name\ndata---sales_csv,Sales report\n"
        )

        catalog = Catalog(app_path=app_dir, metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data_dir)
        catalog.export_db(quiet=True)
        before = (db_dir / "dataset.json").read_text()

        # Delete every final table but keep _scan; a metadata-only rebuild (no scan)
        # must reproduce the same app-facing dataset table.
        for json_file in db_dir.glob("*.json"):
            json_file.unlink()
        rebuilt = Catalog(app_path=app_dir, metadata_path=meta_dir, quiet=True)
        rebuilt.export_db(quiet=True)

        assert _dataset_field(db_dir, "name") == {"data---sales_csv": "Sales report"}
        assert (db_dir / "dataset.json").read_text() == before


class TestScanCacheIO:
    """Unit-level behavior of the cache reader/writer."""

    def test_load_path_none_without_db_path(self):
        assert scan_cache_load_path(None) is None

    def test_load_path_none_without_manifest(self, tmp_path: Path):
        assert scan_cache_load_path(tmp_path) is None

    def test_shrinking_base_removes_stale_cache_files(self, tmp_path: Path):
        app_dir = tmp_path / "app"
        db_dir = app_dir / "data" / "db"
        catalog = Catalog(app_path=app_dir, quiet=True)
        catalog.folder.add(Folder(id="f1", name="Folder", _seen=True))
        write_scan_cache(catalog, db_dir, timestamp=1000)
        assert (scan_cache_dir(db_dir) / "folder.json").exists()

        # The folder table is now empty: its cached file must be pruned.
        catalog.folder.remove("f1")
        write_scan_cache(catalog, db_dir, timestamp=1001)
        assert not (scan_cache_dir(db_dir) / "folder.json").exists()
        assert (scan_cache_dir(db_dir) / "__table__.json").exists()
