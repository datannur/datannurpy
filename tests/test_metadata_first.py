"""Tests for the metadata-first pattern (create_folders=False + on_unmatched)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from datannurpy import Catalog, Folder
from datannurpy.errors import ConfigError

DATA_DIR = Path(__file__).parent.parent / "data"


def _write_csv(path: Path, name: str = "data.csv") -> Path:
    p = path / name
    p.write_text("a,b\n1,2\n3,4\n")
    return p


def _write_metadata(
    meta_dir: Path,
    *,
    folder_rows: list[tuple[str, str]] | None = None,
    dataset_rows: list[tuple[str, str, str, str]] | None = None,
) -> None:
    """Helper to write metadata CSVs.

    folder_rows: (id, name)
    dataset_rows: (id, name, folder_id, data_path)
    """
    meta_dir.mkdir(parents=True, exist_ok=True)
    if folder_rows:
        lines = ["id,name"] + [f"{i},{n}" for i, n in folder_rows]
        (meta_dir / "folder.csv").write_text("\n".join(lines) + "\n")
    if dataset_rows:
        lines = ["id,name,folder_id,data_path"] + [
            f"{i},{n},{f},{d}" for i, n, f, d in dataset_rows
        ]
        (meta_dir / "dataset.csv").write_text("\n".join(lines) + "\n")


class TestMatchPathRuntimeField:
    """The _match_path runtime field is the matching key."""

    def test_match_path_set_at_scan_time(self, tmp_path: Path):
        csv = _write_csv(tmp_path)
        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="src", name="Src"))
        df = catalog.dataset._df
        assert "_match_path" in df.columns
        match_paths = df["_match_path"].to_list()
        assert str(csv) in match_paths

    def test_match_path_is_runtime_field_not_persisted(self, tmp_path: Path):
        _write_csv(tmp_path)
        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="src", name="Src"))
        assert "_match_path" in catalog.dataset.runtime_fields


class TestMetadataFirstE2E:
    """End-to-end metadata-first scenarios."""

    def test_create_folders_false_uses_metadata_ids(self, tmp_path: Path):
        """With metadata + create_folders=False, scan reuses metadata ids."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        csv = _write_csv(data_dir, "sales.csv")

        meta_dir = tmp_path / "meta"
        _write_metadata(
            meta_dir,
            folder_rows=[("custom_folder", "Custom Folder")],
            dataset_rows=[("custom_ds", "My Dataset", "custom_folder", str(csv))],
        )

        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data_dir, create_folders=False)

        datasets = catalog.dataset.all()
        assert len(datasets) == 1
        # The dataset was scanned using the metadata-defined id
        ids = [d.id for d in datasets]
        assert "custom_ds" in ids
        # Variables attached to the metadata dataset id
        variables = catalog.variable.all()
        assert len(variables) == 2
        for v in variables:
            assert v.dataset_id == "custom_ds"

    def test_create_folders_false_no_folder_created(self, tmp_path: Path):
        """create_folders=False creates no folder from the scan path itself."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        csv = _write_csv(data_dir, "sales.csv")

        meta_dir = tmp_path / "meta"
        _write_metadata(
            meta_dir,
            folder_rows=[("editorial", "Editorial Folder")],
            dataset_rows=[("editorial---ds", "DS", "editorial", str(csv))],
        )

        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data_dir, create_folders=False)

        # No folder created from the scan path; metadata is applied at export.
        folder_ids = {f.id for f in catalog.folder.all()}
        # Folder named after data dir is not created
        assert "data" not in folder_ids
        # Editorial folder is in pre-loaded metadata, not yet in catalog tables.
        assert "editorial" not in folder_ids

    def test_create_folders_false_unmatched_warn(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """Unmatched files emit a warning by default."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_csv(data_dir, "orphan.csv")

        meta_dir = tmp_path / "meta"
        _write_metadata(meta_dir, folder_rows=[("f", "F")])

        catalog = Catalog(metadata_path=meta_dir, quiet=False)
        catalog.add_folder(data_dir, create_folders=False)

        captured = capsys.readouterr()
        assert "orphan.csv" in (captured.out + captured.err)
        # No dataset created for the unmatched file
        assert len(catalog.dataset.all()) == 0

    def test_create_folders_false_unmatched_skip(self, tmp_path: Path):
        """on_unmatched='skip' silently ignores unmatched files."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_csv(data_dir, "orphan.csv")

        meta_dir = tmp_path / "meta"
        _write_metadata(meta_dir, folder_rows=[("f", "F")])

        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data_dir, create_folders=False, on_unmatched="skip")

        assert len(catalog.dataset.all()) == 0

    def test_create_folders_false_unmatched_error(self, tmp_path: Path):
        """on_unmatched='error' raises ConfigError on unmatched files."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_csv(data_dir, "orphan.csv")

        meta_dir = tmp_path / "meta"
        _write_metadata(meta_dir, folder_rows=[("f", "F")])

        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        with pytest.raises(ConfigError, match="orphan.csv"):
            catalog.add_folder(data_dir, create_folders=False, on_unmatched="error")

    def test_create_folders_false_with_folder_arg_raises(self, tmp_path: Path):
        """create_folders=False is incompatible with folder argument."""
        catalog = Catalog()
        with pytest.raises(ConfigError, match="create_folders=False"):
            catalog.add_folder(
                tmp_path,
                Folder(id="x", name="X"),
                create_folders=False,
            )

    def test_relative_data_path_resolved_against_metadata_dir(self, tmp_path: Path):
        """Relative data_path in metadata is resolved against metadata dir."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        csv = _write_csv(data_dir, "rel.csv")

        meta_dir = tmp_path / "meta"
        # Use a relative path expressed from meta_dir
        rel_path = Path("../data/rel.csv")
        _write_metadata(
            meta_dir,
            folder_rows=[("f", "F")],
            dataset_rows=[("f---rel", "Rel", "f", str(rel_path))],
        )

        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data_dir, create_folders=False)

        # Confirm match worked: dataset uses the metadata id
        ds_ids = [d.id for d in catalog.dataset.all()]
        assert "f---rel" in ds_ids
        assert csv.exists()

    def test_default_create_folders_true_unchanged_behavior(self, tmp_path: Path):
        """Without create_folders=False, existing behavior is preserved."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_csv(data_dir, "x.csv")
        catalog = Catalog()
        catalog.add_folder(data_dir, Folder(id="src", name="Src"))
        # Folder gets created
        assert any(f.id == "src" for f in catalog.folder.all())
        assert len(catalog.dataset.all()) == 1


class TestMetadataFirstStructureOnly:
    """Coverage for depth=dataset (structure-only) scan path."""

    def test_structure_only_peek_hit(self, tmp_path: Path):
        """depth=dataset + peek hit reuses metadata id, no scan of contents."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        csv = _write_csv(data_dir, "x.csv")
        meta_dir = tmp_path / "meta"
        _write_metadata(
            meta_dir,
            folder_rows=[("ed", "Ed")],
            dataset_rows=[("ed---x", "X", "ed", str(csv))],
        )
        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data_dir, depth="dataset", create_folders=False)
        ds_ids = [d.id for d in catalog.dataset.all()]
        assert "ed---x" in ds_ids
        # Structure-only: no variables scanned
        assert len(catalog.variable.all()) == 0

    def test_structure_only_unmatched_skip(self, tmp_path: Path):
        """depth=dataset + create_folders=False + unmatched."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_csv(data_dir, "orphan.csv")
        meta_dir = tmp_path / "meta"
        _write_metadata(meta_dir, folder_rows=[("f", "F")])
        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        catalog.add_folder(
            data_dir, depth="dataset", create_folders=False, on_unmatched="skip"
        )
        assert len(catalog.dataset.all()) == 0


class TestMetadataFirstTimeSeries:
    """Coverage for time series scan path with metadata-first."""

    def _make_series(self, data_dir: Path) -> Path:
        for year in (2022, 2023, 2024):
            p = data_dir / f"sales_{year}.csv"
            p.write_text("a,b\n1,2\n")
        return data_dir / "sales_2024.csv"

    def test_timeseries_peek_hit(self, tmp_path: Path):
        """Time series matches on latest period and reuses metadata id."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        last = self._make_series(data_dir)
        meta_dir = tmp_path / "meta"
        _write_metadata(
            meta_dir,
            folder_rows=[("ed", "Ed")],
            dataset_rows=[("ed---ts", "TS", "ed", str(last))],
        )
        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data_dir, create_folders=False)
        ds_ids = [d.id for d in catalog.dataset.all()]
        assert "ed---ts" in ds_ids

    def test_timeseries_unmatched_skip(self, tmp_path: Path):
        """Time series + create_folders=False + no match in metadata."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        self._make_series(data_dir)
        meta_dir = tmp_path / "meta"
        _write_metadata(meta_dir, folder_rows=[("f", "F")])
        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data_dir, create_folders=False, on_unmatched="skip")
        assert len(catalog.dataset.all()) == 0


class TestPeekFolderIdFallback:
    """Peek-hit with empty folder_id falls back to scan-derived folder_id."""

    def test_peek_hit_empty_folder_id_uses_fallback(self, tmp_path: Path):
        """Metadata with peek hit + empty folder_id + create_folders=True."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        csv = _write_csv(data_dir, "x.csv")
        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        # folder_id intentionally empty (whitespace) -> _optional_str returns None
        (meta_dir / "dataset.csv").write_text(
            f"id,name,folder_id,data_path\nmeta_id,X, ,{csv}\n"
        )
        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data_dir, Folder(id="src", name="Src"))
        ds = catalog.dataset.get_by("id", "meta_id")
        assert ds is not None
        assert ds.folder_id == "src"


class TestLoadedMetadataIndexEdgeCases:
    """Edge cases in _build_dataset_match_index."""

    def test_no_dataset_table(self, tmp_path: Path):
        """Metadata without a dataset.csv builds an empty match index."""
        meta_dir = tmp_path / "meta"
        _write_metadata(meta_dir, folder_rows=[("f", "F")])
        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        from datannurpy.add_metadata import find_loaded_dataset_by_match_path

        assert find_loaded_dataset_by_match_path(catalog, "/nope") is None

    def test_dataset_without_data_path_column(self, tmp_path: Path):
        """Dataset table without data_path yields no _match_path entries."""
        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        (meta_dir / "dataset.csv").write_text("id,name\nfoo,Foo\n")
        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        from datannurpy.add_metadata import find_loaded_dataset_by_match_path

        assert find_loaded_dataset_by_match_path(catalog, "/nope") is None

    def test_multiple_sources_one_without_dataset(self, tmp_path: Path):
        """Multi-source: a source without dataset.csv is skipped, others indexed."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        csv = _write_csv(data_dir, "x.csv")
        meta1 = tmp_path / "meta1"
        _write_metadata(meta1, folder_rows=[("f", "F")])  # no dataset.csv
        meta2 = tmp_path / "meta2"
        _write_metadata(
            meta2,
            dataset_rows=[("ed---x", "X", "f", str(csv))],
        )
        catalog = Catalog(metadata_path=[meta1, meta2], quiet=True)
        from datannurpy.add_metadata import find_loaded_dataset_by_match_path

        ref = find_loaded_dataset_by_match_path(catalog, str(csv))
        assert ref is not None
        assert ref.id == "ed---x"


class TestResolveMatchPathEdgeCases:
    """Coverage for _resolve_match_path edge cases."""

    def test_url_returns_none(self, tmp_path: Path):
        """URLs are returned as None (not resolved as files)."""
        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        (meta_dir / "dataset.csv").write_text(
            "id,name,data_path\nweb,Web,https://example.com/x.csv\n"
        )
        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        from datannurpy.add_metadata import find_loaded_dataset_by_match_path

        assert (
            find_loaded_dataset_by_match_path(catalog, "https://example.com/x.csv")
            is None
        )

    def test_missing_file_returns_none(self, tmp_path: Path):
        """A data_path pointing to a non-existing file is not indexed."""
        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        ghost = tmp_path / "ghost.csv"
        (meta_dir / "dataset.csv").write_text(f"id,name,data_path\ng,G,{ghost}\n")
        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        from datannurpy.add_metadata import find_loaded_dataset_by_match_path

        assert find_loaded_dataset_by_match_path(catalog, str(ghost)) is None

    def test_empty_data_path_returns_none(self, tmp_path: Path):
        """Empty/whitespace data_path is not indexed."""
        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        (meta_dir / "dataset.csv").write_text("id,name,data_path\ne,E, \n")
        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        from datannurpy.add_metadata import find_loaded_dataset_by_match_path

        assert find_loaded_dataset_by_match_path(catalog, "") is None


class TestHelperUnits:
    """Direct unit tests for internal helpers covering NaN/None branches."""

    def test_build_dataset_match_paths_by_id_filters_invalid_rows(self):
        from datannurpy.add_metadata import _build_dataset_match_paths_by_id

        sources = [
            {"folder": (pd.DataFrame([{"id": "f"}]), "folder.csv")},
            {"dataset": (pd.DataFrame([{"id": "x"}]), "dataset.csv")},
            {
                "dataset": (
                    pd.DataFrame(
                        [
                            {"id": "ok", "_match_path": "/tmp/data.csv"},
                            {"id": "missing_path", "_match_path": None},
                            {"id": None, "_match_path": "/tmp/other.csv"},
                        ]
                    ),
                    "dataset.csv",
                )
            },
        ]

        assert _build_dataset_match_paths_by_id(sources) == {"ok": "/tmp/data.csv"}

    def test_resolve_match_path_none(self, tmp_path: Path):
        from datannurpy.add_metadata import _resolve_match_path

        assert _resolve_match_path(None, tmp_path) is None

    def test_resolve_match_path_nan(self, tmp_path: Path):
        import math

        from datannurpy.add_metadata import _resolve_match_path

        assert _resolve_match_path(math.nan, tmp_path) is None

    def test_optional_str_none(self):
        from datannurpy.add_metadata import _optional_str

        assert _optional_str(None) is None

    def test_optional_str_nan(self):
        import math

        from datannurpy.add_metadata import _optional_str

        assert _optional_str(math.nan) is None

    def test_optional_str_empty(self):
        from datannurpy.add_metadata import _optional_str

        assert _optional_str("   ") is None
        assert _optional_str("ok") == "ok"


class TestPeekFolderIdFallbackOtherPaths:
    """Cover folder_id fallback in structure-only and time-series paths."""

    def test_structure_only_peek_empty_folder_id(self, tmp_path: Path):
        """depth=dataset peek hit + empty folder_id falls back."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        csv = _write_csv(data_dir, "x.csv")
        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        (meta_dir / "dataset.csv").write_text(
            f"id,name,folder_id,data_path\nmid,X, ,{csv}\n"
        )
        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data_dir, Folder(id="src", name="Src"), depth="dataset")
        ds = catalog.dataset.get_by("id", "mid")
        assert ds is not None
        assert ds.folder_id == "src"

    def test_timeseries_peek_empty_folder_id(self, tmp_path: Path):
        """Time-series peek hit + empty folder_id falls back."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        for year in (2022, 2023, 2024):
            (data_dir / f"sales_{year}.csv").write_text("a,b\n1,2\n")
        last = data_dir / "sales_2024.csv"
        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        (meta_dir / "dataset.csv").write_text(
            f"id,name,folder_id,data_path\ntsid,TS, ,{last}\n"
        )
        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data_dir, Folder(id="src", name="Src"))
        ds = catalog.dataset.get_by("id", "tsid")
        assert ds is not None
        assert ds.folder_id == "src"
