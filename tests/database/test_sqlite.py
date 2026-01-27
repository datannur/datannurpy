"""SQLite backend tests."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

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

    @pytest.fixture
    def db_with_employees(
        self, sample_sqlite_db: Path
    ) -> Generator[tuple[ibis.BaseBackend, str, str], None, None]:
        con, _ = connect(f"sqlite:////{sample_sqlite_db}")
        yield con, "sqlite", "sqlite"


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
        assert backend == "sqlite"
        assert con is not None

    def test_geopackage_list_tables(self, gpkg_path: Path) -> None:
        """Test listing tables in a GeoPackage (excludes system tables)."""
        if not gpkg_path.exists():
            pytest.skip("GeoPackage test file not available")
        con, _ = connect(f"sqlite:////{gpkg_path}")
        tables = list_tables(con)
        # Should have data tables
        assert len(tables) > 0
        assert "Project" in tables
        assert "ProjectStatus" in tables
        # Should not include GeoPackage system tables
        assert not any(t.startswith("gpkg_") for t in tables)
        # Should not include rtree index tables
        assert not any(t.startswith("rtree_") for t in tables)

    def test_geopackage_scan_table_with_geometry(self, gpkg_path: Path) -> None:
        """Test scanning a table with geometry columns (Unknown type for POINT)."""
        if not gpkg_path.exists():
            pytest.skip("GeoPackage test file not available")
        con, _ = connect(f"sqlite:////{gpkg_path}")
        # Project has: id, geom (POINT/Unknown), ProjectName, etc.
        variables, row_count, freq_table = scan_table(
            con, "Project", infer_stats=True, freq_threshold=100
        )
        assert row_count == 29  # 29 photovoltaic projects
        var_dict = {v.id: v for v in variables}
        # geom (Unknown) should have None stats
        assert var_dict["geom"].nb_distinct is None
        # Other columns should have stats
        assert var_dict["id"].nb_distinct is not None
        assert var_dict["ProjectName"].nb_distinct is not None
