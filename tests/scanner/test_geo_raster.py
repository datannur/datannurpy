"""Raster geo-format reader (GeoTIFF via rasterio)."""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

pytest.importorskip("rasterio", reason="rasterio (geo extra) not installed")

import rasterio
from rasterio.transform import from_origin

from datannurpy import Catalog
from datannurpy.scanner.geo_raster import scan_geo_raster


def _write_raster(
    path: Path,
    *,
    crs: str | None,
    origin: tuple[float, float],
    res: float,
    nodata: float | None = None,
    descriptions: tuple[str, ...] | None = None,
) -> None:
    """Write a 2x2, two-band float raster."""
    data = np.array([[[100, 200], [300, 400]], [[1, 2], [3, 4]]], dtype="float32")
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=2,
        width=2,
        count=2,
        dtype="float32",
        crs=crs,
        transform=from_origin(origin[0], origin[1], res, res),
        nodata=nodata,
    ) as dst:
        dst.write(data)
        if descriptions is not None:
            for i, desc in enumerate(descriptions, start=1):
                dst.set_band_description(i, desc)


class TestGeotiffViaCatalog:
    def test_bands_and_geo(self, tmp_path: Path) -> None:
        path = tmp_path / "dem.tif"
        _write_raster(
            path, crs="EPSG:2056", origin=(2600000, 1120000), res=10, nodata=400
        )
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_dataset(str(path))
        dataset = catalog.dataset.get_by("name", "dem")
        assert dataset is not None
        assert dataset.crs == "EPSG:2056"
        assert dataset.geometry_type is None  # raster has no vector geometry
        assert dataset.spatial_resolution == 10.0  # metres, projected CRS
        assert dataset.nb_row == 4  # pixel count (2x2)
        bands = {v.name: v for v in catalog.variable.all()}
        assert set(bands) == {"band_1", "band_2"}
        assert all(v.type == "band" for v in bands.values())
        # nodata (400) is excluded from band_1's statistics.
        assert (bands["band_1"].min, bands["band_1"].max) == (100.0, 300.0)
        assert bands["band_1"].mean == 200.0


class TestScanGeoRaster:
    def test_geographic_crs_has_no_resolution(self, tmp_path: Path) -> None:
        path = tmp_path / "geo.tif"
        _write_raster(path, crs="EPSG:4326", origin=(7.4, 46.2), res=0.1)
        _vars, _nb, geo, spatial_resolution = scan_geo_raster(path, dataset_id="ds")
        assert geo is not None
        assert geo["crs"] == "EPSG:4326"
        assert geo["bbox"] == [7.4, 46.0, 7.6, 46.2]  # WGS84 passthrough
        assert spatial_resolution is None  # not a projected CRS → no metre resolution

    def test_no_crs_yields_null_geo(self, tmp_path: Path) -> None:
        path = tmp_path / "raw.tif"
        _write_raster(path, crs=None, origin=(0, 2), res=1)
        variables, nb_row, geo, spatial_resolution = scan_geo_raster(
            path, dataset_id="ds"
        )
        assert nb_row == 4
        assert len(variables) == 2  # bands still extracted without georeferencing
        assert geo == {"crs": None, "geometry_type": None, "bbox": None}
        assert spatial_resolution is None

    def test_band_description_used_as_name(self, tmp_path: Path) -> None:
        path = tmp_path / "named.tif"
        _write_raster(
            path,
            crs="EPSG:2056",
            origin=(2600000, 1120000),
            res=10,
            descriptions=("elevation", "slope"),
        )
        variables, *_ = scan_geo_raster(path, dataset_id="ds")
        assert [v.name for v in variables] == ["elevation", "slope"]

    def test_read_failure_returns_empty(self, tmp_path: Path) -> None:
        bad = tmp_path / "broken.tif"
        bad.write_bytes(b"not a tiff")
        assert scan_geo_raster(bad, dataset_id="ds", quiet=True) == (
            [],
            None,  # unknown, not zero: the failure is already reported as an error
            None,
            None,
        )

    def test_missing_rasterio_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(sys.modules, "rasterio", None)
        with pytest.raises(ImportError, match="datannurpy\\[geo\\]"):
            scan_geo_raster("any.tif", dataset_id="ds")
