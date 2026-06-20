"""Shared geo primitives and the GeoParquet reader."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from datannurpy import Catalog
from datannurpy.scanner import geo as geo_mod
from datannurpy.scanner.geo import (
    extract_geoparquet_geo,
    normalize_geometry_type,
    wgs84_bbox,
)

_LV95_BOUNDS = (2600000.0, 1100000.0, 2620000.0, 1120000.0)
# WGS84 reprojection of _LV95_BOUNDS (west, south, east, north).
_WGS84_BOUNDS = (7.43864, 46.05124, 7.69789, 46.23144)
_EPSG_2056 = {"id": {"authority": "EPSG", "code": 2056}}


def _parse_bbox(bbox: str | None) -> list[float]:
    assert bbox is not None
    return [float(part) for part in bbox.split(",")]


def _write_geoparquet(
    path: Path,
    *,
    column: dict[str, Any] | None,
    primary_column: str | None = "geometry",
    omit_geo: bool = False,
) -> None:
    """Write a one-row Parquet file, optionally carrying GeoParquet metadata."""
    table = pa.table({"geometry": pa.array([b"\x00"], type=pa.binary()), "n": ["a"]})
    if not omit_geo:
        geo: dict[str, Any] = {"version": "1.1.0", "columns": {}}
        if primary_column is not None:
            geo["primary_column"] = primary_column
        if column is not None:
            geo["columns"] = {(primary_column or "geometry"): column}
        table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    pq.write_table(table, path)


class TestNormalizeGeometryType:
    def test_known_values_lower_cased(self) -> None:
        assert normalize_geometry_type("POLYGON") == "polygon"
        assert normalize_geometry_type("MultiPolygon") == "multipolygon"

    def test_unknown_or_null_is_none(self) -> None:
        assert normalize_geometry_type("GEOMETRY") is None
        assert normalize_geometry_type("CIRCULARSTRING") is None
        assert normalize_geometry_type(None) is None


class _RaisingTransformer:
    def transform_bounds(self, *_args: float) -> tuple[float, ...]:
        raise RuntimeError("boom")


class _NonFiniteTransformer:
    def transform_bounds(self, *_args: float) -> tuple[float, float, float, float]:
        return (float("inf"), 0.0, 1.0, 1.0)


class TestWgs84Bbox:
    def test_wgs84_passes_through(self) -> None:
        assert (
            wgs84_bbox("EPSG:4326", 6.0, 46.0, 7.0, 47.0, cache={})
            == "6.0,46.0,7.0,47.0"
        )

    def test_reprojects_from_native_crs(self) -> None:
        assert _parse_bbox(
            wgs84_bbox("EPSG:2056", *_LV95_BOUNDS, cache={})
        ) == pytest.approx(_WGS84_BOUNDS, abs=1e-4)

    def test_incomplete_bounds_is_none(self) -> None:
        assert wgs84_bbox("EPSG:4326", None, 1.0, 2.0, 3.0, cache={}) is None

    def test_non_numeric_bounds_is_none(self) -> None:
        assert wgs84_bbox("EPSG:4326", "x", 1.0, 2.0, 3.0, cache={}) is None

    def test_non_finite_bounds_is_none(self) -> None:
        # Empty layer: an infinite WGS84 extent must not pass through.
        assert wgs84_bbox("EPSG:4326", float("inf"), 0.0, 1.0, 1.0, cache={}) is None

    def test_null_crs_is_none(self) -> None:
        assert wgs84_bbox(None, *_LV95_BOUNDS, cache={}) is None

    def test_unknown_crs_is_none(self) -> None:
        assert wgs84_bbox("EPSG:99999999", *_LV95_BOUNDS, cache={}) is None

    def test_pyproj_unavailable_is_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(sys.modules, "pyproj", None)
        assert wgs84_bbox("EPSG:2056", *_LV95_BOUNDS, cache={}) is None

    def test_transformer_reused_via_cache(self) -> None:
        cache: dict[str, Any] = {}
        first = wgs84_bbox("EPSG:2056", *_LV95_BOUNDS, cache=cache)
        second = wgs84_bbox("EPSG:2056", *_LV95_BOUNDS, cache=cache)
        assert first == second
        assert list(cache) == ["EPSG:2056"]  # transformer built once, then reused

    def test_transform_failure_is_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            geo_mod, "_wgs84_transformer", lambda _crs: _RaisingTransformer()
        )
        assert wgs84_bbox("EPSG:2056", *_LV95_BOUNDS, cache={}) is None

    def test_non_finite_reprojection_is_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            geo_mod, "_wgs84_transformer", lambda _crs: _NonFiniteTransformer()
        )
        assert wgs84_bbox("EPSG:2056", *_LV95_BOUNDS, cache={}) is None


class TestExtractGeoparquetGeo:
    def test_native_crs_reprojected(self, tmp_path: Path) -> None:
        path = tmp_path / "lv95.parquet"
        _write_geoparquet(
            path,
            column={
                "geometry_types": ["Polygon"],
                "crs": _EPSG_2056,
                "bbox": list(_LV95_BOUNDS),
            },
        )
        geo = extract_geoparquet_geo(path)
        assert geo is not None
        assert geo["crs"] == "EPSG:2056"
        assert geo["geometry_type"] == "polygon"
        assert _parse_bbox(geo["bbox"]) == pytest.approx(_WGS84_BOUNDS, abs=1e-4)

    def test_null_crs_defaults_to_wgs84(self, tmp_path: Path) -> None:
        path = tmp_path / "wgs84.parquet"
        _write_geoparquet(
            path,
            column={
                "geometry_types": ["Point"],
                "crs": None,
                "bbox": [7.4, 46.0, 7.7, 46.2],
            },
        )
        assert extract_geoparquet_geo(path) == {
            "crs": "EPSG:4326",
            "geometry_type": "point",
            "bbox": "7.4,46.0,7.7,46.2",
        }

    def test_mixed_geometry_types_is_null(self, tmp_path: Path) -> None:
        path = tmp_path / "mixed.parquet"
        _write_geoparquet(
            path, column={"geometry_types": ["Point", "MultiPoint"], "crs": None}
        )
        geo = extract_geoparquet_geo(path)
        assert geo is not None
        assert geo["geometry_type"] is None
        assert geo["bbox"] is None  # no bbox in metadata

    def test_primary_column_falls_back_to_first(self, tmp_path: Path) -> None:
        path = tmp_path / "noprimary.parquet"
        _write_geoparquet(
            path,
            column={"geometry_types": ["Polygon"], "crs": None},
            primary_column=None,
        )
        geo = extract_geoparquet_geo(path)
        assert geo is not None
        assert geo["geometry_type"] == "polygon"

    def test_crs_without_id_member_is_null(self, tmp_path: Path) -> None:
        path = tmp_path / "noid.parquet"
        _write_geoparquet(
            path,
            column={"geometry_types": ["Polygon"], "crs": {"type": "GeographicCRS"}},
        )
        geo = extract_geoparquet_geo(path)
        assert geo is not None
        assert geo["crs"] is None

    def test_non_dict_crs_is_null(self, tmp_path: Path) -> None:
        path = tmp_path / "strcrs.parquet"
        _write_geoparquet(
            path, column={"geometry_types": ["Polygon"], "crs": "EPSG:2056"}
        )
        geo = extract_geoparquet_geo(path)
        assert geo is not None
        assert geo["crs"] is None

    def test_crs_id_without_authority_is_null(self, tmp_path: Path) -> None:
        path = tmp_path / "partialid.parquet"
        _write_geoparquet(
            path, column={"geometry_types": ["Polygon"], "crs": {"id": {"code": 2056}}}
        )
        geo = extract_geoparquet_geo(path)
        assert geo is not None
        assert geo["crs"] is None

    def test_plain_parquet_returns_none(self, tmp_path: Path) -> None:
        path = tmp_path / "plain.parquet"
        _write_geoparquet(path, column=None, omit_geo=True)
        assert extract_geoparquet_geo(path) is None

    def test_malformed_geo_metadata_returns_none(self, tmp_path: Path) -> None:
        path = tmp_path / "bad.parquet"
        table = pa.table({"a": [1]}).replace_schema_metadata({b"geo": b"not-json"})
        pq.write_table(table, path)
        assert extract_geoparquet_geo(path) is None

    def test_non_dict_column_returns_none(self, tmp_path: Path) -> None:
        path = tmp_path / "weird.parquet"
        _write_geoparquet(path, column=["not", "a", "dict"])  # type: ignore[arg-type]
        assert extract_geoparquet_geo(path) is None


class TestScanGeoparquetViaCatalog:
    """End-to-end: a scanned GeoParquet file carries crs + geometry type + bbox."""

    def test_dataset_is_enriched(self, tmp_path: Path) -> None:
        src = tmp_path / "parcels.parquet"
        _write_geoparquet(
            src,
            column={
                "geometry_types": ["MultiPolygon"],
                "crs": _EPSG_2056,
                "bbox": list(_LV95_BOUNDS),
            },
        )
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_dataset(str(src))
        dataset = catalog.dataset.get_by("name", "parcels")
        assert dataset is not None
        assert dataset.crs == "EPSG:2056"
        assert dataset.geometry_type == "multipolygon"
        assert _parse_bbox(dataset.bbox) == pytest.approx(_WGS84_BOUNDS, abs=1e-4)

    def test_plain_parquet_leaves_geo_fields_null(self, tmp_path: Path) -> None:
        src = tmp_path / "plain.parquet"
        _write_geoparquet(src, column=None, omit_geo=True)
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_dataset(str(src))
        dataset = catalog.dataset.get_by("name", "plain")
        assert dataset is not None
        assert dataset.crs is None
        assert dataset.geometry_type is None
        assert dataset.bbox is None
