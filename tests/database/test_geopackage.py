"""GeoPackage geo-metadata extraction (CRS + geometry type)."""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

from datannurpy import Catalog, EntityMetadata
from datannurpy.scanner.geopackage import (
    _build_crs,
    apply_geopackage_geo,
    extract_geopackage_geo,
)

if TYPE_CHECKING:
    import ibis


_LV95_BOUNDS = (2600000.0, 1100000.0, 2620000.0, 1120000.0)
# WGS84 reprojection of _LV95_BOUNDS (west, south, east, north).
_WGS84_BOUNDS = (7.43864, 46.05124, 7.69789, 46.23144)


def _parse_bbox(bbox: list[float] | None) -> list[float]:
    assert bbox is not None
    return bbox


def _make_geopackage(
    path: Path,
    *,
    geometry_type_name: str = "POLYGON",
    srs_id: int = 2056,
    organization: str = "EPSG",
    org_coordsys_id: int = 2056,
    include_srs_table: bool = True,
    bounds: tuple[float, float, float, float] = _LV95_BOUNDS,
) -> None:
    """Create a minimal GeoPackage (SQLite + standard gpkg_* metadata tables)."""
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    if include_srs_table:
        cur.execute(
            "CREATE TABLE gpkg_spatial_ref_sys ("
            "srs_name TEXT, srs_id INTEGER PRIMARY KEY, organization TEXT, "
            "organization_coordsys_id INTEGER, definition TEXT, description TEXT)"
        )
        cur.execute(
            "INSERT INTO gpkg_spatial_ref_sys VALUES (?,?,?,?,?,?)",
            ("CH1903+ / LV95", srs_id, organization, org_coordsys_id, "", ""),
        )
    cur.execute(
        "CREATE TABLE gpkg_contents ("
        "table_name TEXT PRIMARY KEY, data_type TEXT, identifier TEXT, "
        "description TEXT, last_change TEXT, min_x REAL, min_y REAL, "
        "max_x REAL, max_y REAL, srs_id INTEGER)"
    )
    cur.execute(
        "INSERT INTO gpkg_contents VALUES (?,?,?,?,?,?,?,?,?,?)",
        ("parcels", "features", "parcels", "", "", *bounds, srs_id),
    )
    cur.execute(
        "CREATE TABLE gpkg_geometry_columns ("
        "table_name TEXT, column_name TEXT, geometry_type_name TEXT, "
        "srs_id INTEGER, z INTEGER, m INTEGER)"
    )
    cur.execute(
        "INSERT INTO gpkg_geometry_columns VALUES (?,?,?,?,?,?)",
        ("parcels", "geom", geometry_type_name, srs_id, 0, 0),
    )
    cur.execute("CREATE TABLE parcels (id INTEGER, name TEXT, geom BLOB)")
    cur.execute("INSERT INTO parcels VALUES (1, 'A', X'00')")
    cur.execute("INSERT INTO parcels VALUES (2, 'B', X'00')")
    conn.commit()
    conn.close()


def _connect(path: Path) -> ibis.BaseBackend:
    import ibis

    return ibis.sqlite.connect(str(path))


class TestExtractGeopackageGeo:
    """Unit tests for the gpkg_* metadata reader."""

    def test_extracts_crs_and_geometry_type(self, tmp_path: Path) -> None:
        gpkg = tmp_path / "data.gpkg"
        _make_geopackage(gpkg)
        con = _connect(gpkg)
        try:
            geo = extract_geopackage_geo(con)
        finally:
            con.disconnect()
        assert geo["parcels"]["crs"] == "EPSG:2056"
        assert geo["parcels"]["geometry_type"] == "polygon"
        assert _parse_bbox(geo["parcels"]["bbox"]) == pytest.approx(
            _WGS84_BOUNDS, abs=1e-4
        )

    def test_wgs84_layer_passes_bbox_through(self, tmp_path: Path) -> None:
        gpkg = tmp_path / "wgs84.gpkg"
        bounds = (6.0, 46.0, 7.0, 47.0)
        _make_geopackage(gpkg, srs_id=4326, org_coordsys_id=4326, bounds=bounds)
        con = _connect(gpkg)
        try:
            geo = extract_geopackage_geo(con)
        finally:
            con.disconnect()
        assert geo["parcels"]["crs"] == "EPSG:4326"
        assert geo["parcels"]["bbox"] == [6.0, 46.0, 7.0, 47.0]

    def test_bbox_null_when_pyproj_unavailable(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        gpkg = tmp_path / "data.gpkg"
        _make_geopackage(gpkg)  # EPSG:2056, needs reprojection
        monkeypatch.setitem(sys.modules, "pyproj", None)
        con = _connect(gpkg)
        try:
            geo = extract_geopackage_geo(con)
        finally:
            con.disconnect()
        assert geo["parcels"]["crs"] == "EPSG:2056"
        assert geo["parcels"]["bbox"] is None

    def test_plain_sqlite_returns_empty(self, tmp_path: Path) -> None:
        db = tmp_path / "plain.db"
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.commit()
        conn.close()
        con = _connect(db)
        try:
            assert extract_geopackage_geo(con) == {}
        finally:
            con.disconnect()

    def test_abstract_geometry_type_is_dropped(self, tmp_path: Path) -> None:
        gpkg = tmp_path / "g.gpkg"
        _make_geopackage(gpkg, geometry_type_name="GEOMETRY")
        con = _connect(gpkg)
        try:
            geo = extract_geopackage_geo(con)
        finally:
            con.disconnect()
        assert geo["parcels"]["geometry_type"] is None
        assert geo["parcels"]["crs"] == "EPSG:2056"

    def test_missing_srs_table_yields_null_crs(self, tmp_path: Path) -> None:
        gpkg = tmp_path / "nosrs.gpkg"
        _make_geopackage(gpkg, include_srs_table=False)
        con = _connect(gpkg)
        try:
            geo = extract_geopackage_geo(con)
        finally:
            con.disconnect()
        assert geo["parcels"] == {
            "crs": None,
            "geometry_type": "polygon",
            "bbox": None,
        }


class TestApplyGeopackageGeoViaCatalog:
    """End-to-end: a scanned GeoPackage layer carries CRS + geometry type + bbox."""

    def test_dataset_is_enriched(self, tmp_path: Path) -> None:
        gpkg = tmp_path / "data.gpkg"
        _make_geopackage(gpkg, geometry_type_name="MULTIPOLYGON")
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_database(
            f"sqlite:////{gpkg}",
            metadata=EntityMetadata(id="db", name="DB"),
        )
        parcels = catalog.dataset.get_by("name", "parcels")
        assert parcels is not None
        assert parcels.crs == "EPSG:2056"
        assert parcels.geometry_type == "multipolygon"
        assert _parse_bbox(parcels.bbox) == pytest.approx(_WGS84_BOUNDS, abs=1e-4)

    def test_bbox_exports_as_json_array(self, tmp_path: Path) -> None:
        gpkg = tmp_path / "data.gpkg"
        _make_geopackage(gpkg)
        app_dir = tmp_path / "app"
        catalog = Catalog(app_path=app_dir, quiet=True)
        catalog.add_database(
            f"sqlite:////{gpkg}",
            metadata=EntityMetadata(id="db", name="DB"),
        )
        catalog.export_db()
        datasets = json.loads(
            (app_dir / "data" / "db" / "dataset.json").read_text(encoding="utf-8")
        )
        parcels = next(d for d in datasets if d["name"] == "parcels")
        assert isinstance(parcels["bbox"], list)
        assert parcels["bbox"] == pytest.approx(_WGS84_BOUNDS, abs=1e-4)

    def test_plain_sqlite_leaves_geo_fields_null(self, tmp_path: Path) -> None:
        db = tmp_path / "plain.db"
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE things (id INTEGER, label TEXT)")
        conn.execute("INSERT INTO things VALUES (1, 'x')")
        conn.commit()
        conn.close()
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_database(
            f"sqlite:////{db}",
            metadata=EntityMetadata(id="db", name="DB"),
        )
        things = catalog.dataset.get_by("name", "things")
        assert things is not None
        assert things.crs is None
        assert things.geometry_type is None
        assert things.bbox is None


class TestUnitHelpers:
    def test_build_crs(self) -> None:
        assert _build_crs("EPSG", 2056) == "EPSG:2056"
        assert _build_crs("epsg", 4326) == "EPSG:4326"
        assert _build_crs(None, 2056) is None
        assert _build_crs("EPSG", None) is None
        assert _build_crs("NONE", 0) is None


class _FakeResult:
    def __init__(self, rows: list) -> None:
        self._rows = rows

    def fetchall(self) -> list:
        return self._rows


class _FakeCon:
    """Minimal stand-in for an ibis backend with a scripted ``raw_sql``."""

    def __init__(self, responses: list) -> None:
        self._responses = responses
        self._i = 0

    def raw_sql(self, _query: str) -> _FakeResult:
        response = self._responses[self._i]
        self._i += 1
        if isinstance(response, Exception):
            raise response
        return _FakeResult(response)


_GPKG_TABLE_ROWS = [
    ("gpkg_contents",),
    ("gpkg_geometry_columns",),
    ("gpkg_spatial_ref_sys",),
]


class TestExtractDefensive:
    """Defensive branches: missing raw_sql, query failures, malformed rows."""

    def test_connection_without_raw_sql(self) -> None:
        assert extract_geopackage_geo(cast("ibis.BaseBackend", object())) == {}

    def test_table_probe_failure_returns_empty(self) -> None:
        con = cast("ibis.BaseBackend", _FakeCon([RuntimeError("boom")]))
        assert extract_geopackage_geo(con) == {}

    def test_geo_query_failure_returns_empty(self) -> None:
        con = cast("ibis.BaseBackend", _FakeCon([_GPKG_TABLE_ROWS, RuntimeError("x")]))
        assert extract_geopackage_geo(con) == {}

    def test_row_with_null_table_name_is_skipped(self) -> None:
        con = cast(
            "ibis.BaseBackend",
            _FakeCon([_GPKG_TABLE_ROWS, [(None, "POLYGON", "EPSG", 2056, 0, 0, 1, 1)]]),
        )
        assert extract_geopackage_geo(con) == {}


class TestApplyEdgeCases:
    """apply_geopackage_geo with no usable metadata or no matching dataset."""

    def test_all_null_metadata_is_skipped(self, tmp_path: Path) -> None:
        gpkg = tmp_path / "n.gpkg"
        _make_geopackage(gpkg, geometry_type_name="GEOMETRY", include_srs_table=False)
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        con = _connect(gpkg)
        try:
            assert apply_geopackage_geo(catalog, con, "sqlite", "n") == 0
        finally:
            con.disconnect()

    def test_no_matching_dataset(self, tmp_path: Path) -> None:
        gpkg = tmp_path / "data.gpkg"
        _make_geopackage(gpkg)
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        con = _connect(gpkg)
        try:
            # Fresh catalog: no scanned dataset matches the layer's data_path.
            assert apply_geopackage_geo(catalog, con, "sqlite", "data") == 0
        finally:
            con.disconnect()


class TestGeopackageFolderDiscovery:
    """folder: scans delegate discovered .gpkg files to the database machinery —
    one dataset per layer/table, nested as a container folder in the scan tree."""

    def test_gpkg_discovered_with_layers_and_geo(self, tmp_path: Path) -> None:
        (tmp_path / "sub").mkdir()
        _make_geopackage(tmp_path / "sub" / "cadastre.gpkg")
        (tmp_path / "plain.csv").write_text("a,b\n1,2\n")
        catalog = Catalog(quiet=True)
        catalog.add_folder(tmp_path)
        root_name = tmp_path.name
        container = catalog.folder.get(f"{root_name}---sub---cadastre_gpkg")
        assert container is not None
        assert container.name == "cadastre"
        assert container.parent_id == f"{root_name}---sub"
        parcels = catalog.dataset.get_by("name", "parcels")
        assert parcels is not None
        assert parcels.folder_id == container.id
        assert parcels.nb_row == 2
        assert parcels.crs == "EPSG:2056"
        assert parcels.geometry_type == "polygon"
        assert _parse_bbox(parcels.bbox) == pytest.approx(_WGS84_BOUNDS, abs=1e-3)
        assert catalog.dataset.get_by("name", "plain") is not None
        assert catalog.run_errors == 0

    def test_unchanged_gpkg_table_skipped_on_rescan(self, tmp_path: Path) -> None:
        _make_geopackage(tmp_path / "data.gpkg")
        catalog = Catalog(quiet=True)
        catalog.add_folder(tmp_path)
        catalog.add_folder(tmp_path)
        assert catalog.dataset.count == 1  # still one parcels dataset
        assert catalog.run_errors == 0

    def test_gpkg_dataset_depth_lists_tables_without_scan(self, tmp_path: Path) -> None:
        _make_geopackage(tmp_path / "data.gpkg")
        catalog = Catalog(quiet=True)
        catalog.add_folder(tmp_path, depth="dataset")
        parcels = catalog.dataset.get_by("name", "parcels")
        assert parcels is not None
        assert parcels.nb_row is None
        assert catalog.variable.count == 0

    def test_misnamed_gpkg_skipped_with_warning(self, tmp_path: Path, capsys) -> None:
        (tmp_path / "fake.gpkg").write_text("not a sqlite database")
        catalog = Catalog()
        catalog.add_folder(tmp_path, quiet=False)
        assert "not a SQLite/GeoPackage file" in capsys.readouterr().err
        assert catalog.dataset.count == 0
        assert catalog.run_errors == 0

    def test_metadata_first_mode_skips_gpkg(self, tmp_path: Path, capsys) -> None:
        _make_geopackage(tmp_path / "data.gpkg")
        catalog = Catalog()
        catalog.add_folder(tmp_path, create_folders=False, quiet=False)
        assert "GeoPackage skipped (create_folders=False)" in capsys.readouterr().err
        assert catalog.dataset.count == 0

    def test_delegation_failure_is_a_scan_error(
        self, tmp_path: Path, capsys, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _make_geopackage(tmp_path / "data.gpkg")

        def _boom(*args: object, **kwargs: object) -> None:
            raise RuntimeError("connection exploded")

        monkeypatch.setattr("datannurpy.add_database.add_database", _boom)
        catalog = Catalog()
        catalog.add_folder(tmp_path, quiet=False)
        assert "connection exploded" in capsys.readouterr().err
        assert catalog.run_errors == 1

    def test_is_sqlite_file_declines_unreadable_path(self, tmp_path: Path) -> None:
        from datannurpy.add_folder import _is_sqlite_file

        assert not _is_sqlite_file(tmp_path, None)  # a directory, not a file

    def test_remote_folder_gpkg(self, tmp_path: Path) -> None:
        import uuid

        import fsspec

        _make_geopackage(tmp_path / "cadastre.gpkg")
        root = f"/gpkg_{uuid.uuid4().hex}"
        mem = fsspec.filesystem("memory")
        mem.pipe(f"{root}/cadastre.gpkg", (tmp_path / "cadastre.gpkg").read_bytes())
        mem.pipe(f"{root}/plain.csv", b"a,b\n1,2\n")
        catalog = Catalog(quiet=True)
        catalog.add_folder(f"memory://{root}")
        parcels = catalog.dataset.get_by("name", "parcels")
        assert parcels is not None
        assert parcels.nb_row == 2
        assert parcels.crs == "EPSG:2056"
        assert catalog.dataset.get_by("name", "plain") is not None

    def test_explicit_database_entry_wins_over_discovery(self, tmp_path: Path) -> None:
        # The pre-discovery pattern: an explicit database entry (often with
        # curated metadata) already catalogs the file — discovery steps aside.
        _make_geopackage(tmp_path / "photovoltaik.gpkg")
        catalog = Catalog(quiet=True)
        catalog.add_database(
            f"sqlite:///{tmp_path / 'photovoltaik.gpkg'}",
            metadata=EntityMetadata(id="photovoltaik", name="Photovoltaïque"),
        )
        catalog.add_folder(tmp_path)
        containers = [
            f for f in catalog.folder.all() if f.data_path == "sqlite://photovoltaik"
        ]
        assert [f.id for f in containers] == ["photovoltaik"]  # no duplicate
        parcels = catalog.dataset.get_by("name", "parcels")
        assert parcels is not None
        assert parcels.folder_id == "photovoltaik"

    def test_explicit_database_entry_takes_over_discovered_container(
        self, tmp_path: Path
    ) -> None:
        # Reverse order: discovery ran first — the explicit entry still wins,
        # replacing the discovered container (order independence).
        _make_geopackage(tmp_path / "photovoltaik.gpkg")
        catalog = Catalog(quiet=True)
        catalog.add_folder(tmp_path)
        discovered = catalog.folder.get_by("data_path", "sqlite://photovoltaik")
        assert discovered is not None
        assert discovered._discovered
        catalog.add_database(
            f"sqlite:///{tmp_path / 'photovoltaik.gpkg'}",
            metadata=EntityMetadata(id="photovoltaik", name="Photovoltaïque"),
        )
        containers = [
            f for f in catalog.folder.all() if f.data_path == "sqlite://photovoltaik"
        ]
        assert [(f.id, f.name) for f in containers] == [
            ("photovoltaik", "Photovoltaïque")
        ]
        parcels = catalog.dataset.get_by("name", "parcels")
        assert parcels is not None
        assert parcels.folder_id == "photovoltaik"
        assert catalog.dataset.count == 1  # no duplicated table dataset

    def test_delegation_tolerates_containerless_database_run(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # An add_database run that errors out internally (logged, not raised)
        # creates no container — the discovery marking must not blow up.
        _make_geopackage(tmp_path / "data.gpkg")
        monkeypatch.setattr(
            "datannurpy.add_database.add_database", lambda *a, **k: None
        )
        catalog = Catalog(quiet=True)
        catalog.add_folder(tmp_path)
        assert catalog.folder.get_by("data_path", "sqlite://data") is None
