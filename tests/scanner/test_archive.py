"""Tests for zip-archive handling (zipped Shapefile and zipped tabular files)."""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

import fsspec
import pytest

from datannurpy.errors import ConfigError
from datannurpy.scanner.archive import (
    unsupported_zip_error,
    zip_scannable_member,
)
from datannurpy.scanner.utils import is_zip

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
        assert is_zip("data.zip?token=abc")  # URL query string ignored
        assert not is_zip("data.shp")

    def test_zip_scannable_member(self) -> None:
        shp = ("a.shp", "shapefile")
        assert zip_scannable_member(["a.shp", "a.dbf", "a.shx"]) == shp
        assert zip_scannable_member(["a.dbf", "readme.txt"]) is None  # nothing
        assert zip_scannable_member(["a.shp", "b.shp"]) is None  # ambiguous
        assert zip_scannable_member(["sub/", "sub/a.shp"]) == ("sub/a.shp", "shapefile")
        assert zip_scannable_member(["data.csv", "readme.txt"]) == ("data.csv", "csv")
        assert zip_scannable_member(["book.xlsx"]) == ("book.xlsx", "excel")
        assert zip_scannable_member(["a.csv", "b.csv"]) is None  # ambiguous

    def test_zip_scannable_member_shapefile_priority(self) -> None:
        # A lone .shp wins over extra data members (a codebook CSV) — the Shapefile
        # is what such archives distribute.
        names = ["roads.shp", "roads.dbf", "codebook.csv"]
        assert zip_scannable_member(names) == ("roads.shp", "shapefile")
        assert zip_scannable_member(["a.shp", "b.shp", "c.csv"]) is None

    def test_zip_scannable_member_ignores_packaging_junk(self) -> None:
        # macOS Finder zips ship AppleDouble twins under __MACOSX/; Office lock
        # files ride along too. None of these may count as a second member.
        assert zip_scannable_member(
            ["data.csv", "__MACOSX/", "__MACOSX/._data.csv"]
        ) == ("data.csv", "csv")
        assert zip_scannable_member(["book.xlsx", "~$book.xlsx"]) == (
            "book.xlsx",
            "excel",
        )
        assert zip_scannable_member(["._data.csv"]) is None  # junk only

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

    def test_shapefile_wins_over_codebook_csv(self, tmp_path: Path) -> None:
        # Portals ship a data-dictionary CSV next to the Shapefile; the .shp is
        # still the dataset (pre-generalization behavior preserved).
        zpath = _zip_shapefile(tmp_path, extras={"codebook.csv": "field,label\na,A\n"})
        catalog = Catalog(quiet=True)
        catalog.add_dataset(str(zpath), depth="value")
        ds = catalog.dataset.all()[0]
        assert ds.delivery_format == "shapefile"
        assert ds.geometry_type == "polygon"

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

    def test_no_scannable_member_raises(self, tmp_path: Path) -> None:
        zpath = tmp_path / "docs.zip"
        with zipfile.ZipFile(zpath, "w") as z:
            z.writestr("readme.txt", "hello")
        with pytest.raises(ConfigError, match="exactly one scannable"):
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
        with pytest.raises(ConfigError, match="exactly one scannable"):
            Catalog(quiet=True).add_dataset(str(zpath), depth="value")

    def test_explicit_format_on_unscannable_zip_raises(self, tmp_path: Path) -> None:
        # `format: shapefile` bypasses classification, but extraction still fails clean.
        zpath = tmp_path / "forced.zip"
        with zipfile.ZipFile(zpath, "w") as z:
            z.writestr("notes.txt", "hi")
        with pytest.raises(ConfigError, match="exactly one scannable"):
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


# --------------------------------------------------------------------------- #
# Integration — a zipped tabular file behaves like its plain twin
# --------------------------------------------------------------------------- #
def _zip_csv(tmp_path: Path, extras: dict[str, str] | None = None) -> Path:
    """Build a ``sales.zip`` holding one CSV (plus optional extra members)."""
    zpath = tmp_path / "sales.zip"
    with zipfile.ZipFile(zpath, "w") as z:
        z.writestr("sales.csv", "city,amount\nBern,10\nSion,20\n")
        for name, content in (extras or {}).items():
            z.writestr(name, content)
    return zpath


class TestZippedTabular:
    def test_csv_value_depth(self, tmp_path: Path) -> None:
        catalog = Catalog(quiet=True)
        catalog.add_dataset(str(_zip_csv(tmp_path, extras={"README.txt": "hi"})))
        ds = catalog.dataset.all()[0]
        assert ds.id == "sales"
        assert ds.delivery_format == "csv"
        assert ds.nb_row == 2
        assert [v.name for v in catalog.variable.all()] == ["city", "amount"]

    def test_csv_variable_depth_streams_header(self, tmp_path: Path) -> None:
        # Schema-only mode reads the header straight out of the archive — no
        # full extraction of the member.
        catalog = Catalog(quiet=True)
        catalog.add_dataset(str(_zip_csv(tmp_path)), depth="variable")
        ds = catalog.dataset.all()[0]
        assert ds.delivery_format == "csv"
        assert ds.nb_row is None  # not scanned at variable depth
        assert [v.name for v in catalog.variable.all()] == ["city", "amount"]

    def test_xlsx_variable_depth_extracts(self, tmp_path: Path) -> None:
        # Non-CSV members have no streamable header; schema-only still extracts.
        import pandas as pd

        xlsx = tmp_path / "book.xlsx"
        pd.DataFrame({"a": [1], "b": ["x"]}).to_excel(xlsx, index=False)
        zpath = tmp_path / "book.zip"
        with zipfile.ZipFile(zpath, "w") as z:
            z.write(xlsx, arcname="book.xlsx")
        catalog = Catalog(quiet=True)
        catalog.add_dataset(str(zpath), depth="variable")
        assert [v.name for v in catalog.variable.all()] == ["a", "b"]

    def test_variable_depth_mismatch_raises(self, tmp_path: Path) -> None:
        # format: csv on an xlsx-bearing zip: no streamable CSV header, and the
        # extraction fallback surfaces the contradiction.
        import pandas as pd

        xlsx = tmp_path / "book.xlsx"
        pd.DataFrame({"a": [1]}).to_excel(xlsx, index=False)
        zpath = tmp_path / "book.zip"
        with zipfile.ZipFile(zpath, "w") as z:
            z.write(xlsx, arcname="book.xlsx")
        with pytest.raises(ConfigError, match="contradicts"):
            Catalog(quiet=True).add_dataset(str(zpath), depth="variable", format="csv")

    def test_variable_depth_unscannable_zip_raises(self, tmp_path: Path) -> None:
        # format: csv on a zip without any scannable member: the streaming fast
        # path declines and extraction raises the clear archive error.
        zpath = tmp_path / "notes.zip"
        with zipfile.ZipFile(zpath, "w") as z:
            z.writestr("notes.txt", "hi")
        with pytest.raises(ConfigError, match="exactly one scannable"):
            Catalog(quiet=True).add_dataset(str(zpath), depth="variable", format="csv")

    def test_misnamed_zip_variable_depth(self, tmp_path: Path) -> None:
        # Misnamed .zip + schema-only: the streaming fast path declines
        # (BadZipFile), extraction yields None, and the raw CSV header is read.
        fake = tmp_path / "export.zip"
        fake.write_text("city,amount\nBern,10\n")
        catalog = Catalog(quiet=True)
        catalog.add_dataset(str(fake), depth="variable", format="csv")
        assert [v.name for v in catalog.variable.all()] == ["city", "amount"]

    def test_xlsx_value_depth(self, tmp_path: Path) -> None:
        import pandas as pd

        xlsx = tmp_path / "book.xlsx"
        pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_excel(xlsx, index=False)
        zpath = tmp_path / "book.zip"
        with zipfile.ZipFile(zpath, "w") as z:
            z.write(xlsx, arcname="book.xlsx")
        catalog = Catalog(quiet=True)
        catalog.add_dataset(str(zpath))
        ds = catalog.dataset.all()[0]
        assert ds.delivery_format == "excel"
        assert ds.nb_row == 2

    def test_two_tabular_members_raises(self, tmp_path: Path) -> None:
        zpath = tmp_path / "two.zip"
        with zipfile.ZipFile(zpath, "w") as z:
            z.writestr("a.csv", "x\n1\n")
            z.writestr("b.csv", "y\n2\n")
        with pytest.raises(ConfigError, match="exactly one scannable"):
            Catalog(quiet=True).add_dataset(str(zpath))

    def test_macos_finder_zip_scans(self, tmp_path: Path) -> None:
        # Right-click > Compress on macOS adds __MACOSX/._twins whose suffix still
        # looks scannable; they must not make the archive ambiguous.
        zpath = _zip_csv(
            tmp_path, extras={"__MACOSX/": "", "__MACOSX/._sales.csv": "\x00junk"}
        )
        catalog = Catalog(quiet=True)
        catalog.add_dataset(str(zpath))
        assert catalog.dataset.all()[0].nb_row == 2

    def test_explicit_format_mismatch_raises(self, tmp_path: Path) -> None:
        # format: shapefile on a CSV-bearing zip is a misconfiguration, not a
        # licence to scan the CSV as a Shapefile.
        with pytest.raises(ConfigError, match="contradicts"):
            Catalog(quiet=True).add_dataset(str(_zip_csv(tmp_path)), format="shapefile")

    def test_misnamed_zip_scans_as_declared_format(self, tmp_path: Path) -> None:
        # A .zip-named resource that is plain data (misnamed endpoint/file) falls
        # back to scanning the raw bytes as the declared format.
        fake = tmp_path / "export.zip"
        fake.write_text("city,amount\nBern,10\nSion,20\n")
        catalog = Catalog(quiet=True)
        catalog.add_dataset(str(fake), format="csv")
        assert catalog.dataset.all()[0].nb_row == 2

    def test_misnamed_remote_zip_detected_by_sniffing(self, tmp_path: Path) -> None:
        # Without an explicit format, detection skips the zip classification for a
        # non-archive and the cascade ends in content sniffing (CSV here).
        mem = fsspec.filesystem("memory")
        mem.pipe("/fake_zip/export.zip", b"city,amount\nBern,10\nSion,20\n")
        catalog = Catalog(quiet=True)
        catalog.add_dataset("memory:///fake_zip/export.zip")
        ds = catalog.dataset.all()[0]
        assert ds.delivery_format == "csv"
        assert ds.nb_row == 2

    def test_remote_memory_csv(self, tmp_path: Path) -> None:
        zpath = _zip_csv(tmp_path)
        mem = fsspec.filesystem("memory")
        mem.pipe("/tab_zip/sales.zip", zpath.read_bytes())
        catalog = Catalog(quiet=True)
        catalog.add_dataset("memory:///tab_zip/sales.zip")
        ds = catalog.dataset.all()[0]
        assert ds.delivery_format == "csv"
        assert ds.nb_row == 2


# --------------------------------------------------------------------------- #
# Integration — folder: discovery picks up archives like the dataset: path
# --------------------------------------------------------------------------- #
class TestArchiveInFolderScan:
    def test_zipped_shapefile_discovered_and_scanned(self, tmp_path: Path) -> None:
        data = tmp_path / "data"
        data.mkdir()
        _zip_shapefile(data)
        catalog = Catalog(quiet=True)
        catalog.add_folder(str(data), depth="value")
        ds = catalog.dataset.get_by("name", "parcels")
        assert ds is not None
        assert ds.delivery_format == "shapefile"
        assert ds.geometry_type == "polygon"
        assert ds.crs == "EPSG:4326"
        assert "label" in [v.name for v in catalog.variable.all()]
