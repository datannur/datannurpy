"""Vector geo-format reader (GeoJSON via pyogrio)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("pyogrio", reason="pyogrio (geo extra) not installed")

from datannurpy import Catalog
from datannurpy.scanner.geo_vector import scan_geo_vector

_SQUARE = [[[7.4, 46.0], [7.7, 46.0], [7.7, 46.2], [7.4, 46.2], [7.4, 46.0]]]


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
        assert dataset.bbox == "7.4,46.0,7.7,46.2"  # WGS84 passthrough
        # Attribute column plus the geometry, kept as an un-profiled binary variable.
        types = {v.name: v.type for v in catalog.variable.all()}
        assert types["name"] == "string"
        assert types["wkb_geometry"] == "binary"


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
            "bbox": "7.4,46.0,7.7,46.2",
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
            0,
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
            lambda p: {**real_info(p), "crs": None, "total_bounds": (0, 0, 0, 0)},
        )
        _vars, _nb, _freq, geo, _preview = scan_geo_vector(src, dataset_id="ds")
        assert geo is not None
        assert geo["crs"] is None
        assert geo["bbox"] is None  # no CRS → no reprojection

    def test_missing_pyogrio_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(sys.modules, "pyogrio", None)
        with pytest.raises(ImportError, match="datannurpy\\[geo\\]"):
            scan_geo_vector("any.geojson", dataset_id="ds")
