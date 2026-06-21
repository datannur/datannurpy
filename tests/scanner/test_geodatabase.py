"""File Geodatabase (.gdb): explicit add_geodatabase entry point, one dataset/layer."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyogrio", reason="pyogrio (geo extra) not installed")

from pyogrio.raw import read_arrow, write_arrow

from datannurpy import Catalog, EntityMetadata
from datannurpy.errors import ConfigError
from datannurpy.scanner.geo_vector import list_geo_layers

_SQUARE = [[[7.4, 46.0], [7.7, 46.0], [7.7, 46.2], [7.4, 46.2], [7.4, 46.0]]]


def _write_gdb(gdb: Path, layers: tuple[str, ...], *, crs: str = "EPSG:4326") -> None:
    """Write a File Geodatabase with one polygon feature per named layer."""
    src = gdb.parent / "_src.geojson"
    src.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"lbl": "a"},
                        "geometry": {"type": "Polygon", "coordinates": _SQUARE},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    _, table = read_arrow(src)
    src.unlink()
    for layer in layers:
        write_arrow(
            table,
            gdb,
            driver="OpenFileGDB",
            layer=layer,
            geometry_name="wkb_geometry",
            geometry_type="Polygon",
            crs=crs,
        )


class TestAddGeodatabase:
    def test_layers_nested_under_container_folder(self, tmp_path: Path) -> None:
        _write_gdb(tmp_path / "store.gdb", ("roads", "rivers"), crs="EPSG:2056")
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_geodatabase(str(tmp_path / "store.gdb"))
        folder = catalog.folder.get("store")
        assert folder is not None
        assert folder.type == "geodatabase"
        datasets = {d.name: d for d in catalog.dataset.all()}
        assert {"roads", "rivers"} <= set(datasets)
        roads = datasets["roads"]
        assert roads.folder_id == "store"
        assert roads.delivery_format == "geodatabase"
        assert roads.crs == "EPSG:2056"
        assert roads.geometry_type in ("polygon", "multipolygon")
        assert roads.bbox is not None

    def test_metadata_names_the_folder(self, tmp_path: Path) -> None:
        _write_gdb(tmp_path / "store.gdb", ("roads",))
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_geodatabase(
            str(tmp_path / "store.gdb"),
            metadata=EntityMetadata(id="geo", name="Geo Store", type="cadastre"),
            depth="variable",  # non-"value" depth: no frequency pass
        )
        folder = catalog.folder.get("geo")
        assert folder is not None
        assert folder.type == "cadastre"  # explicit metadata type is kept
        roads = catalog.dataset.get_by("name", "roads")
        assert roads is not None
        assert roads.folder_id == "geo"

    def test_value_depth_extracts_layers(self, tmp_path: Path) -> None:
        _write_gdb(tmp_path / "store.gdb", ("roads",))
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_geodatabase(str(tmp_path / "store.gdb"), depth="value")
        assert catalog.dataset.get_by("name", "roads") is not None

    def test_incremental_rerun_is_stable(self, tmp_path: Path) -> None:
        _write_gdb(tmp_path / "store.gdb", ("roads",))
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_geodatabase(str(tmp_path / "store.gdb"))
        catalog.add_geodatabase(str(tmp_path / "store.gdb"))  # unchanged → skipped
        assert [d.name for d in catalog.dataset.all()] == ["roads"]

    def test_not_a_geodatabase_path(self, tmp_path: Path) -> None:
        plain = tmp_path / "plain"
        plain.mkdir()
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        with pytest.raises(ConfigError, match="File Geodatabase"):
            catalog.add_geodatabase(str(plain))

    def test_gdb_suffix_but_not_a_directory(self, tmp_path: Path) -> None:
        fake = tmp_path / "fake.gdb"
        fake.write_text("not a directory", encoding="utf-8")
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        with pytest.raises(ConfigError, match="File Geodatabase"):
            catalog.add_geodatabase(str(fake))

    def test_invalid_geodatabase_is_logged(self, tmp_path: Path) -> None:
        bogus = tmp_path / "broken.gdb"
        bogus.mkdir()  # a .gdb directory that holds no readable layers
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        catalog.add_geodatabase(str(bogus))
        assert catalog.dataset.all() == []


class TestAddDatasetRejectsGeodatabase:
    def test_add_dataset_on_gdb_redirects(self, tmp_path: Path) -> None:
        _write_gdb(tmp_path / "store.gdb", ("roads",))
        catalog = Catalog(app_path=tmp_path / "app", quiet=True)
        with pytest.raises(ConfigError, match="add_geodatabase"):
            catalog.add_dataset(str(tmp_path / "store.gdb"))


class TestHelpers:
    def test_list_geo_layers(self, tmp_path: Path) -> None:
        _write_gdb(tmp_path / "store.gdb", ("roads", "rivers"))
        assert sorted(list_geo_layers(tmp_path / "store.gdb")) == ["rivers", "roads"]

    def test_list_geo_layers_without_pyogrio(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setitem(sys.modules, "pyogrio", None)
        with pytest.raises(ImportError, match="datannurpy\\[geo\\]"):
            list_geo_layers("any.gdb")
