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


class TestDatabaseTimeSeries:
    """Tests for time series detection in database tables."""

    @pytest.fixture
    def ts_sqlite_db(self, tmp_path: Path) -> Path:
        """Create SQLite with temporal tables."""
        import sqlite3

        db_path = tmp_path / "ts.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE stats_2022 (id INT, value REAL, old_col TEXT)")
        conn.execute("INSERT INTO stats_2022 VALUES (1, 10.0, 'x')")
        conn.execute("CREATE TABLE stats_2023 (id INT, value REAL)")
        conn.execute("INSERT INTO stats_2023 VALUES (2, 20.0)")
        conn.execute("CREATE TABLE stats_2024 (id INT, value REAL, new_col TEXT)")
        conn.execute("INSERT INTO stats_2024 VALUES (3, 30.0, 'y')")
        conn.execute("CREATE TABLE users (name TEXT)")
        conn.execute("INSERT INTO users VALUES ('alice')")
        conn.commit()
        conn.close()
        return db_path

    def test_time_series_groups_tables(self, ts_sqlite_db: Path) -> None:
        """Tables with temporal pattern are grouped into a single dataset."""
        catalog = Catalog(quiet=True)
        catalog.add_database(
            f"sqlite:////{ts_sqlite_db}",
            Folder(id="db", name="DB"),
            group_by_prefix=False,
        )
        datasets = {d.name: d for d in catalog.dataset.all()}
        # stats_2022, stats_2023, stats_2024 → 1 series + users → 2 datasets
        assert len(datasets) == 2
        assert "users" in datasets
        series = [d for d in datasets.values() if d.nb_resources is not None]
        assert len(series) == 1
        ds = series[0]
        assert ds.nb_resources == 3
        assert ds.start_date == "2022"
        assert ds.end_date == "2024"

    def test_time_series_variable_periods(self, ts_sqlite_db: Path) -> None:
        """Variable start_date/end_date reflect schema evolution."""
        catalog = Catalog(quiet=True)
        catalog.add_database(
            f"sqlite:////{ts_sqlite_db}",
            Folder(id="db", name="DB"),
            group_by_prefix=False,
        )
        series = [d for d in catalog.dataset.all() if d.nb_resources is not None]
        assert len(series) == 1
        vars_by_name = {
            v.name: v for v in catalog.variable.all() if v.dataset_id == series[0].id
        }
        # old_col only in 2022
        assert vars_by_name["old_col"].end_date == "2022"
        # new_col only in 2024
        assert vars_by_name["new_col"].start_date == "2024"
        # id and value present in all periods → no start/end
        assert vars_by_name["id"].start_date is None
        assert vars_by_name["id"].end_date is None

    def test_time_series_disabled(self, ts_sqlite_db: Path) -> None:
        """time_series=False keeps tables separate."""
        catalog = Catalog(quiet=True)
        catalog.add_database(
            f"sqlite:////{ts_sqlite_db}",
            Folder(id="db", name="DB"),
            time_series=False,
            group_by_prefix=False,
        )
        datasets = catalog.dataset.all()
        assert len(datasets) == 4  # 3 stats + 1 users

    def test_time_series_structure_mode(self, ts_sqlite_db: Path) -> None:
        """Structure mode creates series dataset without scanning."""
        catalog = Catalog(quiet=True)
        catalog.add_database(
            f"sqlite:////{ts_sqlite_db}",
            Folder(id="db", name="DB"),
            depth="structure",
            group_by_prefix=False,
        )
        series = [d for d in catalog.dataset.all() if d.nb_resources is not None]
        assert len(series) == 1
        assert series[0].nb_resources == 3
        assert series[0].nb_row is None  # Not scanned
        assert catalog.variable.count == 0

    def test_time_series_schema_mode(self, ts_sqlite_db: Path) -> None:
        """Schema mode scans columns but not stats."""
        catalog = Catalog(quiet=True)
        catalog.add_database(
            f"sqlite:////{ts_sqlite_db}",
            Folder(id="db", name="DB"),
            depth="schema",
            group_by_prefix=False,
        )
        series = [d for d in catalog.dataset.all() if d.nb_resources is not None]
        assert len(series) == 1
        assert series[0].nb_resources == 3
        # Variables should be present (from schema scan)
        series_vars = [
            v for v in catalog.variable.all() if v.dataset_id == series[0].id
        ]
        assert len(series_vars) >= 3  # id, value, old_col, new_col

    def test_time_series_with_prefix(self, tmp_path: Path) -> None:
        """Time series dataset placed in correct prefix folder, not year folder."""
        import sqlite3

        db_path = tmp_path / "pfx.db"
        conn = sqlite3.connect(db_path)
        for name in [
            "wp_archive_blob_2023",
            "wp_archive_blob_2024",
            "wp_archive_num_2023",
            "wp_archive_num_2024",
            "wp_users",
            "wp_options",
            "other_table",
        ]:
            conn.execute(f"CREATE TABLE {name} (id INT)")
            conn.execute(f"INSERT INTO {name} VALUES (1)")
        conn.commit()
        conn.close()

        catalog = Catalog(quiet=True)
        catalog.add_database(
            f"sqlite:////{db_path}",
            Folder(id="db", name="DB"),
            group_by_prefix=True,
            prefix_min_tables=2,
        )
        datasets = {d.name: d for d in catalog.dataset.all()}
        # 5 datasets: 2 series + 2 singles + other_table
        assert len(datasets) == 5
        series = {d.name: d for d in datasets.values() if d.nb_resources is not None}
        assert len(series) == 2

        # Both series should be in the "wp_archive" prefix folder
        folder_names = {f.name for f in catalog.folder.all()}
        assert "wp_archive" in folder_names
        archive_folder = next(f for f in catalog.folder.all() if f.name == "wp_archive")
        for s in series.values():
            assert s.folder_id == archive_folder.id

        # No year-specific prefix folders should exist
        assert "wp_archive_blob_2023" not in folder_names
        assert "wp_archive_blob_2024" not in folder_names

    def test_time_series_rescan(self, ts_sqlite_db: Path) -> None:
        """Rescanning removes old series dataset and creates new one."""
        conn_str = f"sqlite:////{ts_sqlite_db}"
        catalog = Catalog(quiet=True)
        catalog.add_database(
            conn_str,
            Folder(id="db", name="DB"),
            group_by_prefix=False,
        )
        catalog.add_database(
            conn_str,
            Folder(id="db", name="DB"),
            group_by_prefix=False,
            refresh=True,
        )
        series = [d for d in catalog.dataset.all() if d.nb_resources is not None]
        assert len(series) == 1

    def test_time_series_full_scan_error(self, tmp_path: Path) -> None:
        """Error scanning latest table in full mode returns error count."""
        import sqlite3
        from unittest.mock import patch

        db_path = tmp_path / "err.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE data_2023 (a INT)")
        conn.execute("CREATE TABLE data_2024 (a INT)")
        conn.commit()
        conn.close()

        catalog = Catalog(quiet=True)
        original = __import__(
            "datannurpy.scanner.database", fromlist=["scan_table"]
        ).scan_table
        call_count = 0

        def fail_on_full(con, name, **kw):
            nonlocal call_count
            call_count += 1
            # Fail on the full scan (infer_stats=True) but allow schema scans
            if kw.get("infer_stats", False):
                raise RuntimeError("boom")
            return original(con, name, **kw)

        with patch("datannurpy.add_database.scan_table", side_effect=fail_on_full):
            catalog.add_database(
                f"sqlite:////{db_path}",
                Folder(id="db", name="DB"),
                group_by_prefix=False,
            )
        # Series dataset should not be created (error during full scan)
        series = [d for d in catalog.dataset.all() if d.nb_resources is not None]
        assert len(series) == 0

    def test_time_series_schema_scan_error(self, tmp_path: Path) -> None:
        """Error in schema mode scan returns error count."""
        import sqlite3
        from unittest.mock import patch

        db_path = tmp_path / "err.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE data_2023 (a INT)")
        conn.execute("CREATE TABLE data_2024 (a INT)")
        conn.commit()
        conn.close()

        catalog = Catalog(quiet=True)

        def fail_all(*a, **kw):
            raise RuntimeError("boom")

        with patch("datannurpy.add_database.scan_table", side_effect=fail_all):
            catalog.add_database(
                f"sqlite:////{db_path}",
                Folder(id="db", name="DB"),
                depth="schema",
                group_by_prefix=False,
            )
        series = [d for d in catalog.dataset.all() if d.nb_resources is not None]
        assert len(series) == 0

    def test_time_series_schema_scan_partial_error(self, tmp_path: Path) -> None:
        """Error scanning one table during columns_by_period continues."""
        import sqlite3
        from unittest.mock import patch

        db_path = tmp_path / "err.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE data_2023 (a INT)")
        conn.execute("INSERT INTO data_2023 VALUES (1)")
        conn.execute("CREATE TABLE data_2024 (a INT)")
        conn.execute("INSERT INTO data_2024 VALUES (2)")
        conn.commit()
        conn.close()

        catalog = Catalog(quiet=True)
        call_count = [0]

        def fail_first(con, name, **kw):
            call_count[0] += 1
            # Fail on the first schema scan (data_2023), succeed for rest
            if call_count[0] == 1:
                raise RuntimeError("boom")
            from datannurpy.scanner.database import scan_table as real

            return real(con, name, **kw)

        with patch("datannurpy.add_database.scan_table", side_effect=fail_first):
            catalog.add_database(
                f"sqlite:////{db_path}",
                Folder(id="db", name="DB"),
                group_by_prefix=False,
            )
        # Series should still be created (partial schema data)
        series = [d for d in catalog.dataset.all() if d.nb_resources is not None]
        assert len(series) == 1


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
