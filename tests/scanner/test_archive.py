"""Tests for zipped-Shapefile handling (the standard open-data distribution form)."""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

import fsspec
import pytest

from datannurpy.errors import ConfigError
from datannurpy.scanner.archive import (
    is_zip,
    unsupported_zip_error,
    zip_shapefile_member,
)

pytest.importorskip("pyogrio", reason="pyogrio (geo extra) not installed")

from pyogrio.raw import read_arrow, write_arrow  # noqa: E402

from datannurpy import Catalog  # noqa: E402

_SQUARE = [[[7.4, 46.0], [7.7, 46.0], [7.7, 46.2], [7.4, 46.2], [7.4, 46.0]]]
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


def _write_shapefile(dirpath: Path, name: str, coords: list, crs: str) -> list[Path]:
    """Write a one-feature Shapefile and return its parts (.shp/.shx/.dbf/.prj …)."""
    src = dirpath / f"{name}.geojson"
    src.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"label": "a"},
                        "geometry": {"type": "Polygon", "coordinates": coords},
                    }
                ],
            }
        )
    )
    _, table = read_arrow(src)
    src.unlink()
    write_arrow(
        table,
        dirpath / f"{name}.shp",
        driver="ESRI Shapefile",
        geometry_name="wkb_geometry",
        geometry_type="Polygon",
        crs=crs,
    )
    return sorted(dirpath.glob(f"{name}.*"))


def _zip_shapefile(
    tmp_path: Path,
    zip_name: str = "parcels.zip",
    *,
    coords: list | None = None,
    crs: str = "EPSG:4326",
    arc_prefix: str = "",
    extras: dict[str, str] | None = None,
) -> Path:
    """Build a .zip containing a single Shapefile (plus optional extra members)."""
    build = tmp_path / "_build"
    build.mkdir(exist_ok=True)
    parts = _write_shapefile(build, "parcels", coords or _SQUARE, crs)
    zpath = tmp_path / zip_name
    with zipfile.ZipFile(zpath, "w") as z:
        for part in parts:
            z.write(part, arcname=f"{arc_prefix}{part.name}")
        for name, content in (extras or {}).items():
            z.writestr(name, content)
    for part in parts:
        part.unlink()
    return zpath


# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #
class TestZipHelpers:
    def test_is_zip(self) -> None:
        assert is_zip("ADMIN-EXPRESS_FRA.zip")
        assert is_zip("data.ZIP")
        assert not is_zip("data.shp")

    def test_zip_shapefile_member(self) -> None:
        assert zip_shapefile_member(["a.shp", "a.dbf", "a.shx"]) == "a.shp"
        assert zip_shapefile_member(["a.dbf", "readme.txt"]) is None  # no .shp
        assert zip_shapefile_member(["a.shp", "b.shp"]) is None  # ambiguous
        assert zip_shapefile_member(["sub/", "sub/a.shp"]) == "sub/a.shp"

    def test_unsupported_zip_error_message(self) -> None:
        assert "expected exactly one" in str(unsupported_zip_error("x.zip", ["a.txt"]))
        assert "(empty)" in str(unsupported_zip_error("x.zip", []))


# --------------------------------------------------------------------------- #
# Integration — a zipped Shapefile behaves like a plain Shapefile
# --------------------------------------------------------------------------- #
class TestZippedShapefile:
    def test_value_depth(self, tmp_path: Path) -> None:
        catalog = Catalog(quiet=True)
        catalog.add_dataset(str(_zip_shapefile(tmp_path)), depth="value")
        ds = catalog.dataset.all()[0]
        assert ds.id == "parcels"
        assert ds.delivery_format == "shapefile"
        assert ds.geometry_type == "polygon"
        assert ds.crs == "EPSG:4326"
        assert "label" in [v.name for v in catalog.variable.all()]

    def test_variable_depth(self, tmp_path: Path) -> None:
        catalog = Catalog(quiet=True)
        catalog.add_dataset(str(_zip_shapefile(tmp_path)), depth="variable")
        assert catalog.dataset.all()[0].delivery_format == "shapefile"
        assert "label" in [v.name for v in catalog.variable.all()]

    def test_dataset_depth_no_scan(self, tmp_path: Path) -> None:
        zpath = _zip_shapefile(tmp_path)
        catalog = Catalog(quiet=True)
        catalog.add_dataset(str(zpath), depth="dataset")
        ds = catalog.dataset.all()[0]
        assert ds.delivery_format == "shapefile"
        assert ds.geometry_type is None  # not scanned at dataset depth
        assert ds.data_size == zpath.stat().st_size  # the compressed archive size

    def test_crs_reprojected_through_zip(self, tmp_path: Path) -> None:
        zpath = _zip_shapefile(tmp_path, coords=_LV95_SQUARE, crs="EPSG:2056")
        catalog = Catalog(quiet=True)
        catalog.add_dataset(str(zpath), depth="value")
        bbox = catalog.dataset.all()[0].bbox
        assert bbox is not None
        for got, want in zip(bbox, _WGS84_BOUNDS):
            assert got == pytest.approx(want, abs=1e-3)

    def test_subdir_and_unrelated_members(self, tmp_path: Path) -> None:
        # Members live in a subdir (flattened to basenames) next to a directory entry
        # and an unrelated file — both must be skipped without breaking the scan.
        zpath = _zip_shapefile(
            tmp_path, arc_prefix="data/", extras={"data/": "", "README.txt": "hi"}
        )
        catalog = Catalog(quiet=True)
        catalog.add_dataset(str(zpath), depth="value")
        assert catalog.dataset.all()[0].geometry_type == "polygon"

    def test_zip_slip_member_is_neutralised(self, tmp_path: Path) -> None:
        # A traversal-style member name is extracted under its basename alone.
        zpath = _zip_shapefile(tmp_path, arc_prefix="../")
        catalog = Catalog(quiet=True)
        catalog.add_dataset(str(zpath), depth="value")
        assert catalog.dataset.all()[0].delivery_format == "shapefile"
        assert not (tmp_path.parent / "parcels.shp").exists()  # did not escape

    def test_zip_bomb_refused(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from datannurpy.compression import DecompressionLimitError

        monkeypatch.setattr("datannurpy.compression._DECOMP_MIN_CAP", 1024)
        monkeypatch.setattr("datannurpy.compression._DECOMP_MAX_RATIO", 2)
        zpath = tmp_path / "bomb.zip"
        # A tiny compressed member expanding far past the cap (classified as a
        # Shapefile by name; extraction aborts before any scan).
        with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("parcels.shp", b"\0" * 200_000)
        with pytest.raises(DecompressionLimitError):
            Catalog(quiet=True).add_dataset(str(zpath), depth="value")

    def test_no_shapefile_raises(self, tmp_path: Path) -> None:
        zpath = tmp_path / "docs.zip"
        with zipfile.ZipFile(zpath, "w") as z:
            z.writestr("readme.txt", "hello")
        with pytest.raises(ConfigError, match="exactly one Shapefile"):
            Catalog(quiet=True).add_dataset(str(zpath), depth="value")

    def test_multiple_shapefiles_raises(self, tmp_path: Path) -> None:
        build = tmp_path / "b"
        build.mkdir()
        parts = _write_shapefile(build, "a", _SQUARE, "EPSG:4326") + _write_shapefile(
            build, "b", _SQUARE, "EPSG:4326"
        )
        zpath = tmp_path / "two.zip"
        with zipfile.ZipFile(zpath, "w") as z:
            for part in parts:
                z.write(part, arcname=part.name)
        with pytest.raises(ConfigError, match="exactly one Shapefile"):
            Catalog(quiet=True).add_dataset(str(zpath), depth="value")

    def test_explicit_format_on_non_shapefile_zip_raises(self, tmp_path: Path) -> None:
        # `format: shapefile` bypasses classification, but extraction still fails clean.
        zpath = tmp_path / "forced.zip"
        with zipfile.ZipFile(zpath, "w") as z:
            z.writestr("notes.txt", "hi")
        with pytest.raises(ConfigError, match="exactly one Shapefile"):
            Catalog(quiet=True).add_dataset(
                str(zpath), depth="value", format="shapefile"
            )

    def test_remote_memory(self, tmp_path: Path) -> None:
        zpath = _zip_shapefile(tmp_path)
        root = "/geo_zip"
        mem = fsspec.filesystem("memory")
        mem.pipe(f"{root}/parcels.zip", zpath.read_bytes())
        catalog = Catalog(quiet=True)
        catalog.add_dataset(f"memory://{root}/parcels.zip", depth="value")
        ds = catalog.dataset.all()[0]
        assert ds.delivery_format == "shapefile"
        assert ds.geometry_type == "polygon"
