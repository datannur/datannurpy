"""SQLite backend tests."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from datannurpy import Catalog, Folder
from datannurpy.scanner.database import connect, list_tables, scan_table

from .base import BaseDatabaseTests

if TYPE_CHECKING:
    import ibis


class TestSQLite(BaseDatabaseTests):
    """SQLite backend tests."""

    @pytest.fixture
    def db(
        self, sample_sqlite_db: Path
    ) -> Generator[tuple[ibis.BaseBackend, str, str], None, None]:
        con, _ = connect(f"sqlite:////{sample_sqlite_db}")
        yield con, "sqlite", "sqlite"
        con.disconnect()

    @pytest.fixture
    def db_with_employees(
        self, sample_sqlite_db: Path
    ) -> Generator[tuple[ibis.BaseBackend, str, str], None, None]:
        con, _ = connect(f"sqlite:////{sample_sqlite_db}")
        yield con, "sqlite", "sqlite"
        con.disconnect()

    def test_scan_table_sampling_large_table(self) -> None:
        """Sampling activates on tables with >= MIN_ROWS_FOR_SAMPLING rows."""
        import ibis

        con = ibis.sqlite.connect(":memory:")
        n = 200
        data = pa.table({"id": list(range(n)), "value": [float(i) for i in range(n)]})
        con.create_table("big", data)
        variables, row_count, sample_size, _ = scan_table(
            con, "big", dataset_id="test", sample_size=100
        )
        assert row_count == n
        assert sample_size is not None
        assert sample_size <= row_count
        var_by_name = {v.name: v for v in variables}
        # min/max/mean are streaming stats from the full table
        assert var_by_name["value"].min == pytest.approx(0.0)
        assert var_by_name["value"].max == pytest.approx(199.0)
        assert var_by_name["value"].mean == pytest.approx(99.5)
        con.disconnect()


class TestSQLiteIncrementalScan:
    """Test incremental scan for SQLite with prefix folders."""

    def test_rescan_marks_existing_prefix_folders_as_seen(
        self, sample_sqlite_db: Path, tmp_path: Path
    ) -> None:
        """Rescanning database should mark existing prefix folders as _seen=True."""
        app_dir = tmp_path
        conn_str = f"sqlite:////{sample_sqlite_db}"

        # First scan with prefix grouping
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_database(
            conn_str,
            Folder(id="db", name="Database"),
            group_by_prefix=True,
            prefix_min_tables=2,
        )
        catalog1.export_db()

        # Should have prefix folders (dim, dim_product, dim_time)
        prefix_folders = [f for f in catalog1.folder.all() if f.type == "table_prefix"]
        assert len(prefix_folders) > 0

        # Reload and rescan
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_database(
            conn_str,
            Folder(id="db", name="Database"),
            group_by_prefix=True,
            prefix_min_tables=2,
        )
        catalog2.finalize()

        # All prefix folders should be kept (marked as seen)
        prefix_folders2 = [f for f in catalog2.folder.all() if f.type == "table_prefix"]
        assert len(prefix_folders2) == len(prefix_folders)


class TestGeoPackage:
    """GeoPackage (SQLite) tests."""

    @pytest.fixture
    def gpkg_path(self) -> Path:
        return Path(__file__).parent.parent.parent / "data" / "photovoltaik.gpkg"

    def test_geopackage_connect(self, gpkg_path: Path) -> None:
        """Test connecting to a GeoPackage file."""
        if not gpkg_path.exists():
            pytest.skip("GeoPackage test file not available")
        con, backend = connect(f"sqlite:////{gpkg_path}")
        try:
            assert backend == "sqlite"
            assert con is not None
        finally:
            con.disconnect()

    def test_geopackage_list_tables(self, gpkg_path: Path) -> None:
        """Test listing tables in a GeoPackage (excludes system tables)."""
        if not gpkg_path.exists():
            pytest.skip("GeoPackage test file not available")
        con, _ = connect(f"sqlite:////{gpkg_path}")
        try:
            tables = list_tables(con)
            # Should have data tables
            assert len(tables) > 0
            assert "Project" in tables
            assert "ProjectStatus" in tables
            # Should not include GeoPackage system tables
            assert not any(t.startswith("gpkg_") for t in tables)
            # Should not include rtree index tables
            assert not any(t.startswith("rtree_") for t in tables)
        finally:
            con.disconnect()

    def test_geopackage_scan_table_with_geometry(self, gpkg_path: Path) -> None:
        """Test scanning a table with geometry columns (POINT → geometry type)."""
        if not gpkg_path.exists():
            pytest.skip("GeoPackage test file not available")
        con, _ = connect(f"sqlite:////{gpkg_path}")
        try:
            # Project has: id, geom (POINT → geometry), ProjectName, etc.
            variables, row_count, _, freq_table = scan_table(
                con, "Project", dataset_id="test", infer_stats=True, freq_threshold=100
            )
            assert row_count == 29  # 29 photovoltaic projects
            var_dict = {v.id: v for v in variables}
            # geom should be detected as geometry with no stats
            assert var_dict["geom"].type == "geometry"
            assert var_dict["geom"].nb_distinct is None
            # Other columns should have stats
            assert var_dict["id"].nb_distinct is not None
            assert var_dict["ProjectName"].nb_distinct is not None
        finally:
            con.disconnect()
