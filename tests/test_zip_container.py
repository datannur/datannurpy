"""Container archives in folder scans: zipped ``.gdb`` trees, ``.gpkg`` members
and lone plain-``.json`` GeoJSON members — one dataset per layer, anchored on the
archive's identity."""

from __future__ import annotations

import os
import sqlite3
import zipfile
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from datannurpy import Catalog
from datannurpy.errors import ConfigError
from datannurpy.scanner.archive import (
    ZipContainer,
    local_container_from_zip,
    zip_container_member,
    zip_member_is_geojson,
    zip_scannable_member,
)
from datannurpy.scanner.database import scan_table as _original_scan_table
from datannurpy.utils.version import scanner_version

from .database.test_geopackage import _make_geopackage

_GEOJSON = (
    '{"type": "FeatureCollection", "features": [{"type": "Feature", '
    '"geometry": {"type": "Point", "coordinates": [7.44, 46.95]}, '
    '"properties": {"name": "Bern"}}]}'
)


def _write_gpkg(path: Path, *, second_table: bool = False) -> None:
    """A minimal GeoPackage; optionally with a second plain table."""
    _make_geopackage(path)
    if second_table:
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE extra (id INTEGER, val TEXT)")
        conn.execute("INSERT INTO extra VALUES (1, 'x')")
        conn.commit()
        conn.close()


def _zip_gpkg(
    tmp_path: Path,
    zip_name: str = "parcels.zip",
    *,
    second_table: bool = False,
    extras: dict[str, str] | None = None,
) -> Path:
    """Build a zip holding one ``.gpkg`` member (plus optional companions)."""
    gpkg = tmp_path / "_build.gpkg"
    _write_gpkg(gpkg, second_table=second_table)
    zpath = tmp_path / zip_name
    with zipfile.ZipFile(zpath, "w") as z:
        z.write(gpkg, arcname="parcels.gpkg")
        for name, content in (extras or {}).items():
            z.writestr(name, content)
    gpkg.unlink()
    return zpath


def _scan_dir(tmp_path: Path) -> Path:
    data = tmp_path / "scan"
    data.mkdir(exist_ok=True)
    return data


def _fail_first_scan_table(n: int = 1):
    """A ``scan_table`` side effect raising on the first ``n`` calls."""
    state = {"count": 0}

    def side_effect(*args: Any, **kwargs: Any) -> Any:
        state["count"] += 1
        if state["count"] <= n:
            raise RuntimeError("simulated aggregate failure")
        return _original_scan_table(*args, **kwargs)

    return side_effect


# --------------------------------------------------------------------------- #
# Pure helpers — classification, extraction, sniff
# --------------------------------------------------------------------------- #
class TestContainerClassification:
    def test_zip_container_member(self) -> None:
        gdb = ZipContainer("geodatabase", "foo.gdb")
        assert zip_container_member(["foo.gdb/a.gdbtable", "foo.gdb/gdb"]) == gdb
        assert (
            zip_container_member(["foo.gdb/a.gdbtable", "license.txt", "doc.pdf"])
            == gdb
        )  # non-data companions tolerated
        assert zip_container_member(["data/foo.gdb/a.gdbtable"]) == ZipContainer(
            "geodatabase", "data/foo.gdb"
        )
        assert zip_container_member(["a.gdb/x", "b.gdb/x"]) is None  # two containers
        assert zip_container_member(["data.gpkg", "license.txt"]) == ZipContainer(
            "geopackage", "data.gpkg"
        )
        assert zip_container_member(["a.gpkg", "b.gpkg"]) is None
        assert zip_container_member(["a.gpkg", "b.gdb/x"]) is None  # mixed
        assert zip_container_member(["a.gpkg", "data.csv"]) is None  # candidate wins
        assert zip_container_member(["readme.pdf", "notes.txt"]) is None
        assert zip_container_member(["x.gdb"]) is None  # a *file* named .gdb

    def test_lone_json_member_resolves_as_geojson(self) -> None:
        assert zip_scannable_member(["data.json", "license.txt"]) == (
            "data.json",
            "geojson",
        )
        # Never competing with a real candidate, a container or a second .json.
        assert zip_scannable_member(["meta.json", "data.csv"]) == ("data.csv", "csv")
        assert zip_scannable_member(["a.json", "b.json"]) is None
        assert zip_scannable_member(["meta.json", "data.gpkg"]) is None
        assert zip_scannable_member(["meta.json", "foo.gdb/a.gdbtable"]) is None


class TestContainerExtraction:
    def test_extracts_single_gpkg_member(self, tmp_path: Path) -> None:
        zpath = _zip_gpkg(tmp_path, extras={"license.txt": "CC-BY"})
        container = ZipContainer("geopackage", "parcels.gpkg")
        with local_container_from_zip(zpath, None, container) as local:
            assert local.name == "parcels.gpkg"
            assert local.read_bytes().startswith(b"SQLite format 3\x00")
            tmp_root = local.parent
        assert not tmp_root.exists()  # temp dir cleaned up

    def test_extracts_gdb_tree(self, tmp_path: Path) -> None:
        zpath = tmp_path / "store.zip"
        with zipfile.ZipFile(zpath, "w") as z:
            z.writestr("data/store.gdb/a.gdbtable", "A")
            z.writestr("data/store.gdb/sub/b.gdbtablx", "B")
            z.writestr("data/store.gdb/", "")  # directory entry
            z.writestr("data/store.gdb/._junk", "resource fork")
            z.writestr("data/store.gdb/../evil.txt", "escape attempt")
            z.writestr("license.txt", "companion outside the tree")
        container = ZipContainer("geodatabase", "data/store.gdb")
        with local_container_from_zip(zpath, None, container) as local:
            assert local.name == "store.gdb"
            assert (local / "a.gdbtable").read_text() == "A"
            assert (local / "sub" / "b.gdbtablx").read_text() == "B"
            assert not (local / "._junk").exists()
            assert not (local / "license.txt").exists()
            extracted = sorted(p.name for p in local.rglob("*") if p.is_file())
            assert extracted == ["a.gdbtable", "b.gdbtablx"]
            tmp_root = local.parent
        assert not tmp_root.exists()
        assert not (tmp_path / "evil.txt").exists()  # traversal neutralised

    def test_zip_member_is_geojson(self, tmp_path: Path) -> None:
        zpath = tmp_path / "geo.zip"
        with zipfile.ZipFile(zpath, "w") as z:
            z.writestr("data.json", _GEOJSON)
            z.writestr("config.json", '{"api": true}')
        assert zip_member_is_geojson(zpath, None, "data.json")
        assert not zip_member_is_geojson(zpath, None, "config.json")
        assert not zip_member_is_geojson(zpath, None, "missing.json")
        fake = tmp_path / "fake.zip"
        fake.write_text("not a zip")
        assert not zip_member_is_geojson(fake, None, "data.json")


# --------------------------------------------------------------------------- #
# Integration — zipped GeoPackage in a folder scan
# --------------------------------------------------------------------------- #
class TestZippedGeoPackage:
    def test_layers_nested_under_container_folder(self, tmp_path: Path) -> None:
        data = _scan_dir(tmp_path)
        _zip_gpkg(data, extras={"license.txt": "CC-BY"})
        catalog = Catalog(quiet=True)
        catalog.add_folder(data)

        ds = catalog.dataset.all()[0]
        assert ds.delivery_format == "geopackage"
        assert ds.name == "parcels"
        assert ds.nb_row == 2
        assert ds.crs == "EPSG:2056"
        assert ds.data_path == "parcels.zip/parcels"
        assert ds.scan_failed_version is None
        assert ds.folder_id is not None
        folder = catalog.folder.get(ds.folder_id)
        assert folder is not None
        assert folder.type == "geopackage"
        assert folder.data_path == "parcels.zip"

    def test_unchanged_rerun_skips_without_extraction(self, tmp_path: Path) -> None:
        data = _scan_dir(tmp_path)
        _zip_gpkg(data)
        catalog = Catalog(quiet=True)
        catalog.add_folder(data)
        ds_id = catalog.dataset.all()[0].id

        def _no_extraction(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError("unchanged archive must not be extracted")

        with patch(
            "datannurpy.add_folder.local_container_from_zip",
            side_effect=_no_extraction,
        ):
            catalog.add_folder(data)
        ds = catalog.dataset.get(ds_id)
        assert ds is not None
        assert ds._seen is True
        assert ds.folder_id is not None
        folder = catalog.folder.get(ds.folder_id)
        assert folder is not None
        assert folder._seen is True

    def test_unchanged_skip_survives_the_scan_cache(self, tmp_path: Path) -> None:
        # A fresh process reloads layer _match_paths from data_path
        # ("parcels.zip/parcels") — the wholesale skip must still recognise the
        # archive through that spelling and skip without extracting.
        app_dir = tmp_path / "app"
        data = _scan_dir(tmp_path)
        _zip_gpkg(data)
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_folder(data)
        catalog1.export_db()

        def _no_extraction(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError("unchanged archive must not be extracted")

        catalog2 = Catalog(app_path=app_dir, quiet=True)
        with patch(
            "datannurpy.add_folder.local_container_from_zip",
            side_effect=_no_extraction,
        ):
            catalog2.add_folder(data)
        ds = catalog2.dataset.all()[0]
        assert ds._seen is True
        assert ds.nb_row == 2

    def test_mtime_bump_rescans(self, tmp_path: Path) -> None:
        data = _scan_dir(tmp_path)
        zpath = _zip_gpkg(data)
        catalog = Catalog(quiet=True)
        catalog.add_folder(data)
        stamp = os.stat(zpath).st_mtime + 10
        os.utime(zpath, (stamp, stamp))
        catalog.add_folder(data)
        assert catalog.dataset.count == 1
        assert catalog.dataset.all()[0].nb_row == 2

    def test_refresh_rescans(self, tmp_path: Path) -> None:
        data = _scan_dir(tmp_path)
        _zip_gpkg(data)
        catalog = Catalog(quiet=True)
        catalog.add_folder(data)
        catalog.add_folder(data, refresh=True)
        assert catalog.dataset.count == 1
        assert catalog.dataset.all()[0].nb_row == 2

    def test_failed_layer_stamped_then_retried_on_new_version(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        data = _scan_dir(tmp_path)
        _zip_gpkg(data)
        catalog = Catalog(quiet=True)
        with patch(
            "datannurpy.scanner.database.scan_table",
            side_effect=_fail_first_scan_table(),
        ):
            catalog.add_folder(data)
        ds = catalog.dataset.all()[0]
        assert ds.scan_failed_version == scanner_version()
        assert ds.nb_row is None  # degraded to schema-only
        assert catalog.run_errors == 1

        # Same version: the failure is remembered, the archive stays skipped.
        catalog.add_folder(data)
        assert catalog.dataset.all()[0].scan_failed_version == scanner_version()

        # A new release retries and the clean scan clears the stamp.
        monkeypatch.setattr("datannurpy.utils.version._VERSION", "999.0.0")
        catalog.add_folder(data)
        ds = catalog.dataset.all()[0]
        assert ds.scan_failed_version is None
        assert ds.nb_row == 2

    def test_stale_layer_rescans_while_clean_sibling_skips(
        self, tmp_path: Path
    ) -> None:
        # One stamped layer declines the wholesale skip; after extraction the
        # clean sibling still skips per layer while the stale one rescans.
        data = _scan_dir(tmp_path)
        _zip_gpkg(data, second_table=True)
        catalog = Catalog(quiet=True)
        catalog.add_folder(data)
        parcels = next(d for d in catalog.dataset.all() if d.name == "parcels")
        catalog.dataset.update(parcels.id, scan_failed_version="0.0.1")

        catalog.add_folder(data)
        by_name = {d.name: d for d in catalog.dataset.all()}
        assert by_name["parcels"].scan_failed_version is None  # rescanned clean
        assert by_name["extra"]._seen is True  # per-layer skip

    def test_zip_classification_error_is_a_scan_error(self, tmp_path: Path) -> None:
        data = _scan_dir(tmp_path)
        with zipfile.ZipFile(data / "sales.zip", "w") as z:
            z.writestr("sales.csv", "a\n1\n")
        catalog = Catalog(quiet=True)
        with patch(
            "datannurpy.add_folder._resolve_zip_format",
            side_effect=RuntimeError("cannot read the archive"),
        ):
            catalog.add_folder(data)
        assert catalog.dataset.count == 0
        assert catalog.run_errors == 1

    def test_layer_lost_when_even_schema_fails(self, tmp_path: Path) -> None:
        data = _scan_dir(tmp_path)
        _zip_gpkg(data)
        catalog = Catalog(quiet=True)
        with patch(
            "datannurpy.scanner.database.scan_table",
            side_effect=RuntimeError("unreadable"),
        ):
            catalog.add_folder(data)
        assert catalog.dataset.count == 0
        assert catalog.run_errors == 1

    def test_fake_gpkg_member_skipped(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        data = _scan_dir(tmp_path)
        with zipfile.ZipFile(data / "fake.zip", "w") as z:
            z.writestr("data.gpkg", "not sqlite at all")
        catalog = Catalog(quiet=False)
        catalog.add_folder(data)
        assert catalog.dataset.count == 0
        assert catalog.run_errors == 0
        assert "not a SQLite/GeoPackage member" in capsys.readouterr().err

    def test_extraction_error_is_a_scan_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("datannurpy.compression._DECOMP_MIN_CAP", 1024)
        monkeypatch.setattr("datannurpy.compression._DECOMP_MAX_RATIO", 2)
        data = _scan_dir(tmp_path)
        with zipfile.ZipFile(data / "bomb.zip", "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("data.gpkg", b"\0" * 200_000)
        catalog = Catalog(quiet=True)
        catalog.add_folder(data)
        assert catalog.dataset.count == 0
        assert catalog.run_errors == 1

    def test_schema_only_depth(self, tmp_path: Path) -> None:
        data = _scan_dir(tmp_path)
        _zip_gpkg(data)
        catalog = Catalog(quiet=True)
        catalog.add_folder(data, depth="variable")
        ds = catalog.dataset.all()[0]
        assert ds.nb_row is None
        assert {v.name for v in catalog.variable.all()} == {"id", "name", "geom"}

    def test_dataset_depth_stays_one_dataset(self, tmp_path: Path) -> None:
        # Nothing is read at dataset depth, so the layers are unknown: the
        # archive is one dataset whose delivery_format names the container kind.
        data = _scan_dir(tmp_path)
        _zip_gpkg(data)
        catalog = Catalog(quiet=True)
        catalog.add_folder(data, depth="dataset")
        ds = catalog.dataset.all()[0]
        assert ds.delivery_format == "geopackage"
        assert ds.nb_row is None
        assert catalog.variable.count == 0

    def test_create_folders_layer_metadata_wins_ids(self, tmp_path: Path) -> None:
        data = _scan_dir(tmp_path)
        zpath = _zip_gpkg(data)
        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        (meta_dir / "dataset.json").write_text(
            f'[{{"id": "custom_parcels", "_match_path": "{zpath}::parcels"}}]'
        )
        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data)
        ds = catalog.dataset.get("custom_parcels")
        assert ds is not None
        assert ds.nb_row == 2
        # No folder in metadata: the layer nests in the container folder.
        assert ds.folder_id is not None
        folder = catalog.folder.get(ds.folder_id)
        assert folder is not None
        assert folder.type == "geopackage"


class TestZippedGeoPackageMetadataFirst:
    def test_no_match_warns(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        data = _scan_dir(tmp_path)
        _zip_gpkg(data)
        catalog = Catalog(quiet=False)
        catalog.add_folder(data, create_folders=False)
        assert catalog.dataset.count == 0
        assert "no metadata match" in capsys.readouterr().err

    def test_no_match_errors(self, tmp_path: Path) -> None:
        data = _scan_dir(tmp_path)
        _zip_gpkg(data)
        catalog = Catalog(quiet=True)
        with pytest.raises(ConfigError, match="No metadata match"):
            catalog.add_folder(data, create_folders=False, on_unmatched="error")

    def test_partial_layer_match(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        data = _scan_dir(tmp_path)
        zpath = _zip_gpkg(data, second_table=True)
        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        (meta_dir / "dataset.json").write_text(
            f'[{{"id": "custom_parcels", "_match_path": "{zpath}::parcels"}}]'
        )
        catalog = Catalog(metadata_path=meta_dir, quiet=False)
        catalog.add_folder(data, create_folders=False)
        assert [d.id for d in catalog.dataset.all() if d.nb_row is not None] == [
            "custom_parcels"
        ]
        assert "extra: no metadata match" in capsys.readouterr().err

    def test_file_match_anchors_layers(self, tmp_path: Path) -> None:
        data = _scan_dir(tmp_path)
        zpath = _zip_gpkg(data)
        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        (meta_dir / "dataset.json").write_text(
            f'[{{"id": "zip_ds", "_match_path": "{zpath}"}}]'
        )
        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data, create_folders=False)
        ds = catalog.dataset.get("zip_ds---parcels")
        assert ds is not None
        assert ds.nb_row == 2

    def test_unchanged_rerun_skips(self, tmp_path: Path) -> None:
        data = _scan_dir(tmp_path)
        zpath = _zip_gpkg(data)
        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        (meta_dir / "dataset.json").write_text(
            f'[{{"id": "custom_parcels", "_match_path": "{zpath}::parcels"}}]'
        )
        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data, create_folders=False)
        catalog.add_folder(data, create_folders=False)  # no folder to re-mark
        ds = catalog.dataset.get("custom_parcels")
        assert ds is not None
        assert ds._seen is True

    def test_rerun_with_create_folders_skips_without_container_folder(
        self, tmp_path: Path
    ) -> None:
        # Scanned metadata-first (no folder), then rerun in create_folders mode:
        # the wholesale skip finds no container folder to re-mark and stays a skip.
        data = _scan_dir(tmp_path)
        zpath = _zip_gpkg(data)
        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        (meta_dir / "dataset.json").write_text(
            f'[{{"id": "custom_parcels", "_match_path": "{zpath}::parcels"}}]'
        )
        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data, create_folders=False)
        catalog.add_folder(data)
        ds = catalog.dataset.get("custom_parcels")
        assert ds is not None
        assert ds._seen is True


# --------------------------------------------------------------------------- #
# Integration — zipped File Geodatabase in a folder scan
# --------------------------------------------------------------------------- #
class TestZippedGeodatabase:
    @pytest.fixture()
    def gdb_zip_dir(self, tmp_path: Path) -> Path:
        pytest.importorskip("pyogrio", reason="pyogrio (geo extra) not installed")
        from .scanner.test_geodatabase import _write_gdb

        _write_gdb(tmp_path / "store.gdb", ("roads", "rivers"), crs="EPSG:2056")
        data = _scan_dir(tmp_path)
        with zipfile.ZipFile(data / "store.zip", "w") as z:
            for f in sorted((tmp_path / "store.gdb").iterdir()):
                z.write(f, arcname=f"store.gdb/{f.name}")
            z.writestr("readme.pdf", "companion")
        return data

    def test_layers_nested_under_container_folder(self, gdb_zip_dir: Path) -> None:
        catalog = Catalog(quiet=True)
        catalog.add_folder(gdb_zip_dir)
        by_name = {d.name: d for d in catalog.dataset.all()}
        assert set(by_name) == {"roads", "rivers"}
        roads = by_name["roads"]
        assert roads.delivery_format == "geodatabase"
        assert roads.crs == "EPSG:2056"
        assert roads.data_path == "store.zip/roads"
        assert roads.folder_id is not None
        folder = catalog.folder.get(roads.folder_id)
        assert folder is not None
        assert folder.type == "geodatabase"

    def test_unchanged_rerun_skips_without_extraction(self, gdb_zip_dir: Path) -> None:
        catalog = Catalog(quiet=True)
        catalog.add_folder(gdb_zip_dir)

        def _no_extraction(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError("unchanged archive must not be extracted")

        with patch(
            "datannurpy.add_folder.local_container_from_zip",
            side_effect=_no_extraction,
        ):
            catalog.add_folder(gdb_zip_dir)
        assert all(d._seen for d in catalog.dataset.all())

    def test_stale_layer_rescans_while_clean_sibling_skips(
        self, gdb_zip_dir: Path
    ) -> None:
        catalog = Catalog(quiet=True)
        catalog.add_folder(gdb_zip_dir)
        roads = next(d for d in catalog.dataset.all() if d.name == "roads")
        catalog.dataset.update(roads.id, scan_failed_version="0.0.1")

        catalog.add_folder(gdb_zip_dir)
        by_name = {d.name: d for d in catalog.dataset.all()}
        assert by_name["roads"].scan_failed_version is None  # rescanned clean
        assert by_name["rivers"]._seen is True  # per-layer skip

    def test_layer_scan_error_stamped(
        self, gdb_zip_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from .conftest import empty_geo_scan

        monkeypatch.setattr(
            "datannurpy.scanner.geo_vector.scan_geo_vector", empty_geo_scan
        )
        catalog = Catalog(quiet=True)
        catalog.add_folder(gdb_zip_dir)
        assert catalog.dataset.count == 2
        assert all(
            d.scan_failed_version == scanner_version() for d in catalog.dataset.all()
        )
        assert catalog.run_errors == 2

    def test_metadata_first_no_match_warns(
        self, gdb_zip_dir: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        catalog = Catalog(quiet=False)
        catalog.add_folder(gdb_zip_dir, create_folders=False)
        assert catalog.dataset.count == 0
        assert "no metadata match" in capsys.readouterr().err

    def test_metadata_first_partial_layer_match(
        self, gdb_zip_dir: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        zpath = gdb_zip_dir / "store.zip"
        meta_dir = gdb_zip_dir.parent / "meta"
        meta_dir.mkdir()
        (meta_dir / "dataset.json").write_text(
            f'[{{"id": "custom_roads", "_match_path": "{zpath}::roads"}}]'
        )
        catalog = Catalog(metadata_path=meta_dir, quiet=False)
        catalog.add_folder(gdb_zip_dir, create_folders=False)
        assert catalog.dataset.get("custom_roads") is not None
        assert "rivers: no metadata match" in capsys.readouterr().err


# --------------------------------------------------------------------------- #
# Integration — lone plain-.json member (GeoJSON published as .json)
# --------------------------------------------------------------------------- #
class TestLoneJsonMember:
    def test_geojson_member_scanned(self, tmp_path: Path) -> None:
        pytest.importorskip("pyogrio", reason="pyogrio (geo extra) not installed")
        data = _scan_dir(tmp_path)
        with zipfile.ZipFile(data / "geo.zip", "w") as z:
            z.writestr("data.json", _GEOJSON)
            z.writestr("license.txt", "CC-BY")
        catalog = Catalog(quiet=True)
        catalog.add_folder(data)
        ds = catalog.dataset.all()[0]
        assert ds.delivery_format == "geojson"
        assert ds.nb_row == 1
        assert ds.geometry_type == "point"

    def test_geojson_extension_member_needs_no_sniff(self, tmp_path: Path) -> None:
        pytest.importorskip("pyogrio", reason="pyogrio (geo extra) not installed")
        data = _scan_dir(tmp_path)
        with zipfile.ZipFile(data / "geo.zip", "w") as z:
            z.writestr("data.geojson", _GEOJSON)
        catalog = Catalog(quiet=True)
        catalog.add_folder(data)
        assert catalog.dataset.all()[0].delivery_format == "geojson"

    def test_non_geojson_member_skipped_quietly(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        data = _scan_dir(tmp_path)
        with zipfile.ZipFile(data / "config.zip", "w") as z:
            z.writestr("config.json", '{"api": true}')
        catalog = Catalog(quiet=False)
        catalog.add_folder(data)
        assert catalog.dataset.count == 0
        assert catalog.run_errors == 0
        assert "not GeoJSON" in capsys.readouterr().err
