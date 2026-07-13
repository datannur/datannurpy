"""Vector geo-format reader (GeoJSON, Shapefile) via pyogrio."""

from __future__ import annotations

import json
import sys
import warnings
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

pytest.importorskip("pyogrio", reason="pyogrio (geo extra) not installed")

from pyogrio.raw import read_arrow, write_arrow

from datannurpy import Catalog
from datannurpy.scanner.geo_vector import _extension_types_to_storage, scan_geo_vector

_SQUARE = [[[7.4, 46.0], [7.7, 46.0], [7.7, 46.2], [7.4, 46.2], [7.4, 46.0]]]
# A square in Swiss LV95 (EPSG:2056) metres and its WGS84 reprojection.
_LV95_SQUARE = [
    [
        [2600000, 1100000],
        [2620000, 1100000],
        [2620000, 1120000],
        [2600000, 1120000],
        [2600000, 1100000],
    ]
]
_WGS84_BOUNDS = (7.43864, 46.05124, 7.69789, 46.23144)


def _write_geojson(path: Path, features: list[dict[str, Any]]) -> None:
    path.write_text(
        json.dumps({"type": "FeatureCollection", "features": features}),
        encoding="utf-8",
    )


def _polygon(name: str, coords: list) -> dict[str, Any]:
    return {
        "type": "Feature",
        "properties": {"name": name},
        "geometry": {"type": "Polygon", "coordinates": coords},
    }


def _write_ogr(path: Path, coords: list, *, driver: str, crs: str) -> None:
    """Write a one-feature vector file by round-tripping a GeoJSON through Arrow."""
    src = path.with_suffix(".geojson")
    _write_geojson(src, [_polygon("a", coords)])
    _, table = read_arrow(src)
    src.unlink()
    write_arrow(
        table,
        path,
        driver=driver,
        geometry_name="wkb_geometry",
        geometry_type="Polygon",
        crs=crs,
    )


class TestScanGeoVectorViaCatalog:
    def test_dataset_is_enriched(self, tmp_path: Path) -> None:
        src = tmp_path / "parcels.geojson"
        _write_geojson(src, [_polygon("a", _SQUARE)])
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_dataset(str(src))
        dataset = catalog.dataset.get_by("name", "parcels")
        assert dataset is not None
        assert dataset.crs == "EPSG:4326"
        assert dataset.geometry_type == "polygon"
        assert dataset.bbox == [7.4, 46.0, 7.7, 46.2]  # WGS84 passthrough
        # Attribute column plus the geometry, kept as an un-profiled binary variable.
        types = {v.name: v.type for v in catalog.variable.all()}
        assert types["name"] == "string"
        assert types["wkb_geometry"] == "binary"

    def test_schema_only_depth_still_extracts_geo(self, tmp_path: Path) -> None:
        # Geo formats bypass the tabular schema-only path; depth="variable" must not
        # crash and still yields the geo fields.
        src = tmp_path / "parcels.geojson"
        _write_geojson(src, [_polygon("a", _SQUARE)])
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_dataset(str(src), depth="variable")
        dataset = catalog.dataset.get_by("name", "parcels")
        assert dataset is not None
        assert dataset.crs == "EPSG:4326"
        assert {v.name for v in catalog.variable.all()} == {"name", "wkb_geometry"}


class TestShapefileViaCatalog:
    def test_reprojected_from_native_crs(self, tmp_path: Path) -> None:
        _write_ogr(
            tmp_path / "parcels.shp",
            _LV95_SQUARE,
            driver="ESRI Shapefile",
            crs="EPSG:2056",
        )
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_dataset(str(tmp_path / "parcels.shp"))
        dataset = catalog.dataset.get_by("name", "parcels")
        assert dataset is not None
        assert dataset.crs == "EPSG:2056"
        assert dataset.geometry_type == "polygon"
        assert dataset.bbox is not None
        assert dataset.bbox == pytest.approx(_WGS84_BOUNDS, abs=1e-3)

    def test_folder_walk_ignores_sidecars(self, tmp_path: Path) -> None:
        _write_ogr(
            tmp_path / "parcels.shp", _SQUARE, driver="ESRI Shapefile", crs="EPSG:4326"
        )
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_folder(str(tmp_path))
        # Only the .shp is a dataset; .shx/.dbf/.prj/.cpg are not scanned.
        assert [d.name for d in catalog.dataset.all()] == ["parcels"]

    def test_folder_scan_carries_geo_fields(self, tmp_path: Path) -> None:
        # A folder-discovered geo file keeps its spatial enrichment, like the
        # same file added via add_dataset.
        _write_ogr(
            tmp_path / "parcels.shp", _SQUARE, driver="ESRI Shapefile", crs="EPSG:4326"
        )
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_folder(str(tmp_path))
        dataset = catalog.dataset.get_by("name", "parcels")
        assert dataset is not None
        assert dataset.crs == "EPSG:4326"
        assert dataset.geometry_type == "polygon"
        assert dataset.bbox == [7.4, 46.0, 7.7, 46.2]


class TestRemoteVector:
    def test_remote_shapefile_fetches_sidecars(self, tmp_path: Path) -> None:
        import uuid

        import fsspec

        _write_ogr(
            tmp_path / "parcels.shp", _SQUARE, driver="ESRI Shapefile", crs="EPSG:4326"
        )
        root = f"rmt_shp_{uuid.uuid4().hex}"  # unique: the memory fs is a singleton
        mem_fs = fsspec.filesystem("memory")
        mem_fs.mkdir(f"/{root}")
        for f in tmp_path.iterdir():
            if f.stem == "parcels":
                mem_fs.upload(str(f), f"/{root}/{f.name}")
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_dataset(f"memory:///{root}/parcels.shp")
        dataset = catalog.dataset.get_by("name", "parcels")
        assert dataset is not None
        assert dataset.crs == "EPSG:4326"
        assert dataset.nb_row == 1  # sidecars present → fully readable


class TestGmlKmlViaCatalog:
    def test_gml_is_enriched(self, tmp_path: Path) -> None:
        _write_ogr(tmp_path / "zones.gml", _SQUARE, driver="GML", crs="EPSG:4326")
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_dataset(str(tmp_path / "zones.gml"))
        dataset = catalog.dataset.get_by("name", "zones")
        assert dataset is not None
        assert dataset.crs == "EPSG:4326"
        assert dataset.geometry_type == "polygon"
        assert dataset.bbox == [7.4, 46.0, 7.7, 46.2]

    def test_kml_is_enriched(self, tmp_path: Path) -> None:
        _write_ogr(tmp_path / "points.kml", _SQUARE, driver="KML", crs="EPSG:4326")
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_dataset(str(tmp_path / "points.kml"))
        dataset = catalog.dataset.get_by("name", "points")
        assert dataset is not None
        # KML carries the CRS reliably; geometry type and extent are best-effort
        # (the bundled KML driver reports them inconsistently across GDAL versions).
        assert dataset.crs == "EPSG:4326"
        assert dataset.geometry_type in (None, "polygon")
        assert dataset.bbox in (None, [7.4, 46.0, 7.7, 46.2])

    def test_gpx_is_enriched(self, tmp_path: Path) -> None:
        # GPX is a fixed multi-layer container; the first non-empty layer
        # (waypoints here) is scanned, with its extent force-computed (the GPX
        # driver has no cheap one).
        (tmp_path / "peaks.gpx").write_text(
            '<?xml version="1.0"?>'
            '<gpx version="1.1" creator="test" '
            'xmlns="http://www.topografix.com/GPX/1/1">'
            '<wpt lat="46.0" lon="7.4"><name>A</name><ele>500</ele></wpt>'
            '<wpt lat="46.2" lon="7.7"><name>B</name><ele>600</ele></wpt>'
            "</gpx>"
        )
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_dataset(str(tmp_path / "peaks.gpx"))
        dataset = catalog.dataset.get_by("name", "peaks")
        assert dataset is not None
        assert dataset.delivery_format == "gpx"
        assert dataset.nb_row == 2
        assert dataset.crs == "EPSG:4326"
        assert dataset.geometry_type == "point"
        assert dataset.bbox == [7.4, 46.0, 7.7, 46.2]
        assert "name" in [v.name for v in catalog.variable.all()]

    def test_gpx_track_recording_scans_tracks_layer(self, tmp_path: Path) -> None:
        # The dominant real-world GPX (Strava/Garmin recordings) has no waypoints:
        # the empty leading layers are skipped and the tracks layer is scanned.
        (tmp_path / "ride.gpx").write_text(
            '<?xml version="1.0"?>'
            '<gpx version="1.1" creator="test" '
            'xmlns="http://www.topografix.com/GPX/1/1">'
            "<trk><name>morning ride</name><trkseg>"
            '<trkpt lat="46.0" lon="7.4"><ele>500</ele></trkpt>'
            '<trkpt lat="46.2" lon="7.7"><ele>600</ele></trkpt>'
            "</trkseg></trk></gpx>"
        )
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_dataset(str(tmp_path / "ride.gpx"))
        dataset = catalog.dataset.get_by("name", "ride")
        assert dataset is not None
        assert dataset.nb_row == 1  # the track feature, not an empty waypoints layer
        assert dataset.geometry_type == "multilinestring"

    def test_gpx_without_features_scans_empty(self, tmp_path: Path) -> None:
        # All layers empty: the scan completes with an empty dataset, no crash.
        (tmp_path / "empty.gpx").write_text(
            '<?xml version="1.0"?>'
            '<gpx version="1.1" creator="test" '
            'xmlns="http://www.topografix.com/GPX/1/1"></gpx>'
        )
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_dataset(str(tmp_path / "empty.gpx"))
        dataset = catalog.dataset.get_by("name", "empty")
        assert dataset is not None
        assert dataset.nb_row == 0
        assert dataset.bbox is None


class TestScanGeoVector:
    def test_returns_geo_and_variables(self, tmp_path: Path) -> None:
        src = tmp_path / "x.geojson"
        _write_geojson(src, [_polygon("a", _SQUARE), _polygon("b", _SQUARE)])
        variables, nb_row, _freq, geo, preview = scan_geo_vector(src, dataset_id="ds")
        assert nb_row == 2
        assert {v.name for v in variables} == {"name", "wkb_geometry"}
        assert geo == {
            "crs": "EPSG:4326",
            "geometry_type": "polygon",
            "bbox": [7.4, 46.0, 7.7, 46.2],
        }
        assert preview is None  # return_preview defaults to False

    def test_preview_returned_when_requested(self, tmp_path: Path) -> None:
        src = tmp_path / "x.geojson"
        _write_geojson(src, [_polygon("a", _SQUARE)])
        *_, preview = scan_geo_vector(
            src, dataset_id="ds", preview_rows=5, return_preview=True
        )
        assert preview is not None

    def test_mixed_geometry_type_is_null(self, tmp_path: Path) -> None:
        src = tmp_path / "mixed.geojson"
        _write_geojson(
            src,
            [
                _polygon("a", _SQUARE),
                {
                    "type": "Feature",
                    "properties": {"name": "p"},
                    "geometry": {"type": "Point", "coordinates": [7.5, 46.1]},
                },
            ],
        )
        _vars, _nb, _freq, geo, _preview = scan_geo_vector(src, dataset_id="ds")
        assert geo is not None
        assert geo["geometry_type"] is None  # heterogeneous layer

    def test_read_failure_returns_empty(self, tmp_path: Path) -> None:
        bad = tmp_path / "broken.geojson"
        bad.write_text("{ not valid geojson", encoding="utf-8")
        assert scan_geo_vector(bad, dataset_id="ds", quiet=True) == (
            [],
            None,  # unknown, not zero: the failure is already reported as an error
            None,
            None,
            None,
        )

    def test_crs_none_yields_null_crs_and_bbox(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import pyogrio

        src = tmp_path / "x.geojson"
        _write_geojson(src, [_polygon("a", _SQUARE)])
        real_info = pyogrio.read_info
        monkeypatch.setattr(
            pyogrio,
            "read_info",
            lambda p, **kw: {
                **real_info(p, **kw),
                "crs": None,
                "total_bounds": (0, 0, 0, 0),
            },
        )
        _vars, _nb, _freq, geo, _preview = scan_geo_vector(src, dataset_id="ds")
        assert geo is not None
        assert geo["crs"] is None
        assert geo["bbox"] is None  # no CRS → no reprojection

    def test_missing_pyogrio_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(sys.modules, "pyogrio", None)
        with pytest.raises(ImportError, match="datannurpy\\[geo\\]"):
            scan_geo_vector("any.geojson", dataset_id="ds")


class TestGeoArrowExtensionType:
    """The ``geoarrow.wkb`` annotation pyogrio puts on the geometry column must not
    reach polars: it warns on the unknown extension type (polars ≥ 1.40) and will
    materialize it as an extension dtype in polars 2.0, breaking the WKB pipeline."""

    def test_scan_emits_no_extension_warning(self, tmp_path: Path) -> None:
        src = tmp_path / "x.geojson"
        _write_geojson(src, [_polygon("a", _SQUARE)])
        with warnings.catch_warnings():
            warnings.filterwarnings("error", message=".*[Ee]xtension type.*")
            *_, preview = scan_geo_vector(
                src, dataset_id="ds", preview_rows=5, return_preview=True
            )
        assert preview is not None

    def test_geometry_extension_annotation_is_stripped(self, tmp_path: Path) -> None:
        src = tmp_path / "x.geojson"
        _write_geojson(src, [_polygon("a", _SQUARE)])
        _, table = read_arrow(src)
        geom_field = table.schema.field("wkb_geometry")
        assert b"ARROW:extension:name" in (geom_field.metadata or {})  # precondition
        stripped = _extension_types_to_storage(table)
        for field in stripped.schema:
            assert b"ARROW:extension:name" not in (field.metadata or {})
        # Values are untouched — the WKB bytes are the storage the pipeline expects.
        assert stripped["wkb_geometry"].to_pylist() == table["wkb_geometry"].to_pylist()

    def test_plain_table_is_returned_unchanged(self) -> None:
        table = pa.table({"name": ["a"], "geom": [b"\x01"]})
        assert _extension_types_to_storage(table) is table

    def test_registered_extension_type_is_cast_to_storage(self) -> None:
        class _WkbLike(pa.ExtensionType):
            def __init__(self) -> None:
                super().__init__(pa.binary(), "test.wkb_like")

            def __arrow_ext_serialize__(self) -> bytes:
                return b""

            @classmethod
            def __arrow_ext_deserialize__(
                cls, storage_type: pa.DataType, serialized: bytes
            ) -> _WkbLike:
                return cls()

        storage = pa.array([b"\x01\x02", None], pa.binary())
        ext_array = pa.ExtensionArray.from_storage(_WkbLike(), storage)
        table = pa.table({"geom": ext_array, "name": ["a", "b"]})
        stripped = _extension_types_to_storage(table)
        assert stripped.schema.field("geom").type == pa.binary()
        assert stripped["geom"].to_pylist() == [b"\x01\x02", None]
        assert stripped["name"].to_pylist() == ["a", "b"]


class TestEmptyVectorContainer:
    _EMPTY_KML = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<kml xmlns="http://www.opengis.net/kml/2.2">'
        "<Document><name>empty</name></Document></kml>"
    )

    def test_kml_without_layers_scans_empty(self, tmp_path: Path) -> None:
        # A KML without a single Folder/Placemark exposes no layer at all; the
        # scan reports an empty dataset instead of crashing on the layer list.
        (tmp_path / "empty.kml").write_text(self._EMPTY_KML)
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_dataset(str(tmp_path / "empty.kml"))
        dataset = catalog.dataset.get_by("name", "empty")
        assert dataset is not None
        assert dataset.nb_row == 0
        assert dataset.bbox is None
        assert catalog.run_errors == 0

    def test_no_layers_logs_no_error(self, tmp_path: Path, capsys) -> None:
        (tmp_path / "empty.kml").write_text(self._EMPTY_KML)
        variables, nb_row, freq, geo, preview = scan_geo_vector(
            tmp_path / "empty.kml", dataset_id="ds"
        )
        assert (variables, nb_row, freq, geo, preview) == ([], 0, None, None, None)
        err = capsys.readouterr().err
        assert "IndexError" not in err
        assert "✗" not in err


class TestJsonExtensionProperties:
    """pyogrio maps a nested GeoJSON property to the canonical ``arrow.json``
    extension type, which ibis' memtable schema mapping rejects — the values
    must reach the catalog as text instead of crashing the scan."""

    def test_json_extension_type_is_cast_to_storage(self) -> None:
        import ibis

        table = pa.table({"meta": pa.array(['{"x": 1}'], type=pa.json_()), "n": [1]})
        stripped = _extension_types_to_storage(table)
        assert stripped.schema.field("meta").type == pa.string()
        assert stripped["meta"].to_pylist() == ['{"x": 1}']
        ibis.memtable(stripped)  # the previously crashing mapping

    def test_nested_properties_scan_as_text(self, tmp_path: Path) -> None:
        src = tmp_path / "nested.geojson"
        src.write_text(
            json.dumps(
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {"label": "a", "meta": {"x": 1, "y": [2]}},
                            "geometry": {"type": "Point", "coordinates": [7.4, 46.0]},
                        }
                    ],
                }
            )
        )
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_dataset(str(src))
        dataset = catalog.dataset.get_by("name", "nested")
        assert dataset is not None
        assert dataset.nb_row == 1
        assert "meta" in [v.name for v in catalog.variable.all()]
        assert catalog.run_errors == 0


class TestMalformedXmlRepair:
    """Systematic exporter bugs (bare '&' in URLs, bare '<' in values) get one
    conservative repair-and-retry on a temp copy; the original stays untouched."""

    _BAD_GPX = (
        '<?xml version="1.0"?>'
        '<gpx version="1.1" creator="test" '
        'xmlns="http://www.topografix.com/GPX/1/1">'
        '<wpt lat="46.0" lon="7.4"><name>A</name>'
        "<desc>https://x.ch?a=1&b=2 deckung: <25%</desc></wpt></gpx>"
    )

    def test_bare_amp_and_lt_repaired(self, tmp_path: Path, capsys) -> None:
        bad = tmp_path / "bad.gpx"
        bad.write_text(self._BAD_GPX)
        original = bad.read_bytes()
        variables, nb_row, *_ = scan_geo_vector(bad, dataset_id="ds")
        err = capsys.readouterr().err
        assert nb_row == 1
        assert "desc" in [v.name for v in variables]
        assert "repaired malformed XML (2 substitutions)" in err
        assert "✗" not in err
        assert bad.read_bytes() == original  # the source file stays untouched

    def test_repaired_values_keep_their_text(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.kml"
        bad.write_text(
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<kml xmlns="http://www.opengis.net/kml/2.2"><Document><Folder>'
            "<Placemark><name>a & b</name>"
            "<Point><coordinates>7.4,46.0</coordinates></Point></Placemark>"
            "</Folder></Document></kml>"
        )
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_dataset(str(bad))
        dataset = catalog.dataset.get_by("name", "bad")
        assert dataset is not None
        assert dataset.nb_row == 1
        assert catalog.run_errors == 0

    def test_unrepairable_file_fails_with_one_line(
        self, tmp_path: Path, capsys
    ) -> None:
        # No bare '&'/'<' to substitute: the retry declines and the original
        # error is logged once.
        trunc = tmp_path / "trunc.gpx"
        trunc.write_text('<?xml version="1.0"?><gpx version="1.1"')
        variables, nb_row, *_ = scan_geo_vector(trunc, dataset_id="ds")
        err = capsys.readouterr().err
        assert (variables, nb_row) == ([], None)
        assert err.count("✗") == 1
        assert "repaired" not in err

    def test_non_xml_format_is_not_repaired(self, tmp_path: Path, capsys) -> None:
        broken = tmp_path / "broken.geojson"
        broken.write_text('{"type": "FeatureCollection", "features": [')
        variables, nb_row, *_ = scan_geo_vector(broken, dataset_id="ds")
        err = capsys.readouterr().err
        assert (variables, nb_row) == ([], None)
        assert "✗" in err
        assert "repaired" not in err

    def test_retry_declines_missing_and_binary_files(self, tmp_path: Path) -> None:
        from datannurpy.scanner.geo_vector import _retry_with_repaired_xml

        # Unreadable source: nothing to repair.
        assert _retry_with_repaired_xml(tmp_path / "gone.gpx", None, "x", True) is None
        # NUL bytes near the start (UTF-16 …): a byte-level ASCII repair would
        # corrupt the file, so the retry declines.
        utf16 = tmp_path / "wide.gpx"
        utf16.write_bytes('<?xml version="1.0"?><gpx>&'.encode("utf-16"))
        assert _retry_with_repaired_xml(utf16, None, "x", True) is None

    def test_retry_declines_when_repair_is_not_enough(self, tmp_path: Path) -> None:
        from datannurpy.scanner.geo_vector import _retry_with_repaired_xml

        # A bare '&' triggers the retry, but the XML stays broken after repair.
        still_bad = tmp_path / "still.gpx"
        still_bad.write_text('<?xml version="1.0"?><gpx>&<wpt')
        assert _retry_with_repaired_xml(still_bad, None, "x", True) is None

    def test_retry_declines_featureless_repaired_file(self, tmp_path: Path) -> None:
        from datannurpy.scanner.geo_vector import _retry_with_repaired_xml

        # Repaired copy parses but exposes no layer: nothing scannable, decline.
        empty = tmp_path / "empty.kml"
        empty.write_text(
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<kml xmlns="http://www.opengis.net/kml/2.2">'
            "<Document><name>a & b</name></Document></kml>"
        )
        assert _retry_with_repaired_xml(empty, None, "x", True) is None


class TestMultiLayerVisibility:
    def test_multilayer_kml_warns_with_skipped_layers(
        self, tmp_path: Path, capsys
    ) -> None:
        (tmp_path / "multi.kml").write_text(
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<kml xmlns="http://www.opengis.net/kml/2.2"><Document>'
            "<Folder><name>zones</name><Placemark><name>a</name>"
            "<Point><coordinates>7.4,46.0</coordinates></Point></Placemark></Folder>"
            "<Folder><name>points</name><Placemark><name>b</name>"
            "<Point><coordinates>7.5,46.1</coordinates></Point></Placemark></Folder>"
            "</Document></kml>"
        )
        variables, nb_row, *_ = scan_geo_vector(tmp_path / "multi.kml", dataset_id="ds")
        err = capsys.readouterr().err
        assert nb_row == 1  # only the first populated layer is scanned
        assert "⚠" in err
        assert "skipped: 'points'" in err

    def test_multilayer_gpx_stays_quiet(self, tmp_path: Path, capsys) -> None:
        # A GPX's layers are fixed facets of one recording — no warning noise.
        (tmp_path / "ride.gpx").write_text(
            '<?xml version="1.0"?>'
            '<gpx version="1.1" creator="test" '
            'xmlns="http://www.topografix.com/GPX/1/1">'
            "<trk><trkseg>"
            '<trkpt lat="46.0" lon="7.4"><ele>500</ele></trkpt>'
            "</trkseg></trk></gpx>"
        )
        variables, nb_row, *_ = scan_geo_vector(tmp_path / "ride.gpx", dataset_id="ds")
        err = capsys.readouterr().err
        assert nb_row == 1
        assert "skipped:" not in err
        assert "⚠" not in err
