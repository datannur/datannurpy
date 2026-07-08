"""Tests for the metadata-first pattern (create_folders=False + on_unmatched)."""

from __future__ import annotations

import json
import os
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

import pandas as pd
import pytest

from datannurpy import Catalog, EntityMetadata
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
        catalog.add_folder(tmp_path, metadata=EntityMetadata(id="src", name="Src"))
        df = catalog.dataset._df
        assert "_match_path" in df.columns
        match_paths = df["_match_path"].to_list()
        assert str(csv) in match_paths

    def test_match_path_is_runtime_field_not_persisted(self, tmp_path: Path):
        _write_csv(tmp_path)
        catalog = Catalog()
        catalog.add_folder(tmp_path, metadata=EntityMetadata(id="src", name="Src"))
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

    def test_create_folders_false_matches_relative_explicit_match_path(
        self, tmp_path: Path
    ):
        """Explicit relative _match_path values resolve from the metadata source."""
        app_dir = tmp_path / "app"
        data_dir = app_dir / "db-source" / "dataset"
        data_dir.mkdir(parents=True)
        _write_csv(data_dir, "sales.csv")

        meta_dir = tmp_path / "overlay"
        meta_dir.mkdir()
        (meta_dir / "dataset.json").write_text(
            '[{"id":"custom_ds","_match_path":"../app/db-source/dataset/sales.csv"}]'
        )

        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data_dir, create_folders=False, on_unmatched="error")

        assert [dataset.id for dataset in catalog.dataset.all()] == ["custom_ds"]

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

    def test_unmatched_files_are_not_counted_as_scanned(self, tmp_path: Path):
        """An unmatched file is skipped, not scanned — it must not inflate the tally.

        Regression: the run bilan derived ``scanned`` from ``len(plan.to_scan)``,
        which wrongly counted metadata-first files that never reach a scan.
        """
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        matched = _write_csv(data_dir, "sales.csv")
        _write_csv(data_dir, "orphan.csv")  # no metadata row → unmatched

        meta_dir = tmp_path / "meta"
        _write_metadata(
            meta_dir,
            folder_rows=[("f", "F")],
            dataset_rows=[("sales", "Sales", "f", str(matched))],
        )

        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data_dir, create_folders=False, on_unmatched="skip")

        assert len(catalog.dataset.all()) == 1
        assert catalog._run_scanned == 1  # only the matched file, not the orphan
        assert catalog._run_unchanged == 0

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

    def test_create_folders_false_with_metadata_arg_raises(self, tmp_path: Path):
        """create_folders=False is incompatible with metadata argument."""
        catalog = Catalog()
        with pytest.raises(ConfigError, match="create_folders=False"):
            catalog.add_folder(
                tmp_path,
                metadata=EntityMetadata(id="x", name="X"),
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

    def test_explicit_match_path_overrides_remote_data_path(self, tmp_path: Path):
        """Explicit _match_path matches scans while data_path remains public."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        csv = _write_csv(data_dir, "resource.csv")

        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        public_url = "https://example.admin.ch/resource.csv"
        link_url = "https://example.admin.ch/dataset-info"
        (meta_dir / "folder.csv").write_text("id,name\nf,F\n")
        (meta_dir / "dataset.csv").write_text(
            "id,name,folder_id,data_path,_match_path,link\n"
            f"dataset-1,Dataset 1,f,{public_url},{csv},{link_url}\n"
        )

        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data_dir, create_folders=False)

        datasets = catalog.dataset.all()
        assert len(datasets) == 1
        assert datasets[0].id == "dataset-1"
        assert datasets[0]._match_path == str(csv)

        out_dir = tmp_path / "out"
        catalog.export_db(out_dir, quiet=True)
        with open(out_dir / "dataset.json") as f:
            exported = json.load(f)

        assert exported[0]["data_path"] == public_url
        assert exported[0]["link"] == link_url
        assert "_match_path" not in exported[0]

    def test_explicit_empty_match_path_does_not_fall_back_to_data_path(
        self, tmp_path: Path
    ):
        """Presence of _match_path strictly disables data_path matching."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        csv = _write_csv(data_dir, "local.csv")

        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        (meta_dir / "dataset.csv").write_text(
            f"id,name,data_path,_match_path\nlocal,Local,{csv},\n"
        )

        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        from datannurpy.add_metadata import find_loaded_dataset_by_match_path

        assert find_loaded_dataset_by_match_path(catalog, str(csv)) is None

        catalog.add_folder(data_dir, create_folders=False, on_unmatched="skip")
        assert len(catalog.dataset.all()) == 0

    def test_default_create_folders_true_unchanged_behavior(self, tmp_path: Path):
        """Without create_folders=False, existing behavior is preserved."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_csv(data_dir, "x.csv")
        catalog = Catalog()
        catalog.add_folder(data_dir, metadata=EntityMetadata(id="src", name="Src"))
        # Folder gets created
        assert any(f.id == "src" for f in catalog.folder.all())
        assert len(catalog.dataset.all()) == 1

    def test_explicit_match_path_with_remote_url_is_indexed(self, tmp_path: Path):
        """Explicit _match_path is a technical key, not a local filesystem path."""
        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        (meta_dir / "dataset.csv").write_text(
            "id,name,_match_path\n"
            "remote,Remote,sftp://user@example.org/shared/data/file.csv\n"
        )

        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        from datannurpy.add_metadata import find_loaded_dataset_by_match_path

        ref = find_loaded_dataset_by_match_path(
            catalog, "sftp://example.org/shared/data/file.csv"
        )
        assert ref is not None
        assert ref.id == "remote"

    def test_time_series_match_path_matches_logical_series(self, tmp_path: Path):
        """Metadata-first can match a logical time series instead of latest file."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_csv(data_dir, "series_2023.csv")
        _write_csv(data_dir, "series_2024.csv")

        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        (meta_dir / "dataset.csv").write_text(
            "id,name,folder_id,_match_path\nseries-id,Series,f,series_[YYYY].csv\n"
        )

        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data_dir, create_folders=False)

        datasets = catalog.dataset.all()
        assert len(datasets) == 1
        assert datasets[0].id == "series-id"
        assert datasets[0].nb_resources == 2

    def test_time_series_data_path_can_fallback_to_logical_series(self, tmp_path: Path):
        """data_path can be the match fallback when _match_path is absent."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_csv(data_dir, "monthly_2024-01.csv")
        _write_csv(data_dir, "monthly_2024-02.csv")

        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        (meta_dir / "dataset.csv").write_text(
            "id,name,folder_id,data_path\nmonthly-id,Monthly,f,monthly_[YYYY/MM].csv\n"
        )

        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data_dir, create_folders=False)

        datasets = catalog.dataset.all()
        assert len(datasets) == 1
        assert datasets[0].id == "monthly-id"
        assert datasets[0].nb_resources == 2


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

    def test_structure_only_logs_per_dataset(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """depth=dataset logs processed and skipped datasets."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_csv(data_dir, "x.csv")

        catalog = Catalog(quiet=False)

        catalog.add_folder(
            data_dir,
            metadata=EntityMetadata(id="src", name="Src"),
            depth="dataset",
        )
        first = capsys.readouterr()
        assert "x.csv" in first.err

        catalog.add_folder(
            data_dir,
            metadata=EntityMetadata(id="src", name="Src"),
            depth="dataset",
        )
        second = capsys.readouterr()
        assert "x.csv (unchanged)" in second.err


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

    def test_yearly_match_path_does_not_match_monthly_group(self, tmp_path: Path):
        """A [YYYY/MM] _match_path must not collapse onto a yearly [YYYY] group.

        When a folder holds two series sharing the same filename skeleton — a
        yearly one and a monthly one — a metadata row keyed on ``[YYYY/MM]``
        must match only the monthly group. Before the fix both granularities
        normalised to the same ``---PERIOD---`` match key, so the yearly group
        stole the id and the monthly group then failed with a duplicate id.
        """
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        # Yearly group: base_<YYYY>.csv
        for year in (2007, 2010, 2011, 2013, 2014):
            (data_dir / f"base_{year}.csv").write_text("a,b\n1,2\n")
        # Monthly group: base_<YYYYMM>.csv (shares the "base_---PERIOD---.csv" skeleton)
        for ym in (200606, 201307, 201407, 201903, 202002):
            (data_dir / f"base_{ym}.csv").write_text("a,b\n1,2\n")

        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        (meta_dir / "dataset.csv").write_text(
            "id,name,folder_id,_match_path\nmy_series,My series,f,base_[YYYY/MM].csv\n"
        )

        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        catalog.add_folder(data_dir, create_folders=False, on_unmatched="skip")

        datasets = catalog.dataset.all()
        # Only the monthly group matches; the yearly group is left unmatched.
        assert len(datasets) == 1
        matched = datasets[0]
        assert matched.id == "my_series"
        # The matched group is the monthly one (latest file base_202002.csv),
        # not the yearly one (latest file base_2014.csv).
        assert matched.data_path is not None
        assert matched.data_path.endswith("base_202002.csv")


class TestMetadataFirstTimeSeriesIncremental:
    """Second-run incremental behavior for metadata-first time series."""

    def _make_series(self, data_dir: Path) -> None:
        for year in (2022, 2023, 2024):
            (data_dir / f"series_{year}.csv").write_text("a,b\n1,2\n")

    def test_unchanged_series_skipped_on_second_run(self, tmp_path: Path):
        """On a second run over an unchanged db, the series must be skipped.

        Reproduces the duplicate-id regression: after loading the existing db,
        the time-series dataset is restored with its normalized metadata
        ``_match_path`` (e.g. ``series_---PERIOD-Y---.csv``), but the scan plan
        only looks it up via the concrete latest file (``series_2024.csv``). The
        series is therefore rescanned and ``dataset.add`` raises
        ``Row with id '...' already exists``.
        """
        db_dir = tmp_path / "db"
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        self._make_series(data_dir)

        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        (meta_dir / "dataset.csv").write_text(
            "id,name,folder_id,_match_path\nseries-id,Series,f,series_[YYYY].csv\n"
        )

        # First run: empty db, scan the series, export to output_dir.
        catalog1 = Catalog(output_dir=db_dir, metadata_path=meta_dir, quiet=True)
        catalog1.add_folder(data_dir, create_folders=False)
        catalog1.export_db()
        assert [d.id for d in catalog1.dataset.all()] == ["series-id"]

        # Second run: reload the db and rescan the unchanged series.
        catalog2 = Catalog(output_dir=db_dir, metadata_path=meta_dir, quiet=True)
        assert catalog2._loaded_from_db is True
        catalog2.add_folder(data_dir, create_folders=False)

        datasets = catalog2.dataset.all()
        assert len(datasets) == 1
        ds = datasets[0]
        assert ds.id == "series-id"
        assert getattr(ds, "_seen", False) is True

    def test_modified_series_replaced_on_second_run(self, tmp_path: Path):
        """A changed series is rescanned and replaced, not duplicated."""
        db_dir = tmp_path / "db"
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        self._make_series(data_dir)

        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        (meta_dir / "dataset.csv").write_text(
            "id,name,folder_id,_match_path\nseries-id,Series,f,series_[YYYY].csv\n"
        )

        catalog1 = Catalog(output_dir=db_dir, metadata_path=meta_dir, quiet=True)
        catalog1.add_folder(data_dir, create_folders=False)
        catalog1.export_db()

        # Add a new period and bump the latest file mtime so the series changes.
        new_file = data_dir / "series_2025.csv"
        new_file.write_text("a,b\n1,2\n")
        new_mtime = int(new_file.stat().st_mtime) + 10
        os.utime(new_file, (new_mtime, new_mtime))

        catalog2 = Catalog(output_dir=db_dir, metadata_path=meta_dir, quiet=True)
        catalog2.add_folder(data_dir, create_folders=False)

        datasets = catalog2.dataset.all()
        assert len(datasets) == 1
        ds = datasets[0]
        assert ds.id == "series-id"
        assert ds.nb_resources == 4


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
        catalog.add_folder(data_dir, metadata=EntityMetadata(id="src", name="Src"))
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

    def test_remote_data_path_is_indexed(self, tmp_path: Path):
        """Remote data_path is a metadata-first fallback match key."""
        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        (meta_dir / "dataset.csv").write_text(
            "id,name,data_path\nweb,Web,https://example.com/x.csv\n"
        )
        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        from datannurpy.add_metadata import find_loaded_dataset_by_match_path

        ref = find_loaded_dataset_by_match_path(catalog, "https://example.com/x.csv")
        assert ref is not None
        assert ref.id == "web"

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

    def test_period_match_placeholder_picks_finest_or_none(self):
        from datannurpy.scanner.timeseries import period_match_placeholder

        assert period_match_placeholder("base_[YYYY].csv") == "---PERIOD-Y---"
        assert period_match_placeholder("base_[YYYY/MM].csv") == "---PERIOD-M---"
        # Mixed patterns collapse to the finest granularity present.
        assert period_match_placeholder("[YYYY]/base_[YYYY/MM].csv") == "---PERIOD-M---"
        # [YYYY] is a substring of [YYYY]Q[N]: the quarterly pattern must win.
        assert period_match_placeholder("base_[YYYY]Q[N].csv") == "---PERIOD-Q---"
        assert period_match_placeholder("plain.csv") is None

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

    def test_normalize_match_key_canonicalizes_remote_port_and_bad_host(self):
        from datannurpy.add_metadata import normalize_match_key

        assert (
            normalize_match_key("sftp://user@example.org:2222/data/file_[YYYY].csv")
            == "sftp://example.org:2222/data/file_---PERIOD-Y---.csv"
        )
        assert normalize_match_key("sftp://@/data/file.csv") == "sftp://@/data/file.csv"

    def test_find_loaded_dataset_by_match_path_reuses_built_index(self, tmp_path: Path):
        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        (meta_dir / "dataset.csv").write_text(
            "id,name,_match_path\nindexed,Indexed,/data/indexed.csv\n"
        )

        catalog = Catalog(metadata_path=meta_dir, quiet=True)
        from datannurpy.add_metadata import find_loaded_dataset_by_match_path

        assert (
            find_loaded_dataset_by_match_path(catalog, "/data/indexed.csv") is not None
        )
        assert catalog._dataset_match_index is not None
        assert find_loaded_dataset_by_match_path(catalog, "/data/missing.csv") is None

    def test_match_path_candidates_include_remote_series_url(self):
        from datannurpy.add_folder import _match_path_candidates
        from datannurpy.scanner.filesystem import FileSystem

        fs = FileSystem.__new__(FileSystem)
        fs.fs = type("FakeFS", (), {"protocol": "sftp"})()
        fs.root = "/shared/data"
        fs._url_parts = urlsplit("sftp://user@example.org/shared/data")

        assert _match_path_candidates(
            PurePosixPath("/shared/data/series_2024.csv"),
            fs,
            series_normalized_path="series_[YYYY].csv",
        ) == [
            "sftp://example.org/shared/data/series_[YYYY].csv",
            "series_[YYYY].csv",
            "/shared/data/series_2024.csv",
            "sftp://example.org/shared/data/series_2024.csv",
        ]

    def test_match_path_candidates_skip_missing_remote_series_url(self):
        from typing import cast

        from datannurpy.add_folder import _match_path_candidates
        from datannurpy.scanner.filesystem import FileSystem

        class FakeRemoteFS:
            is_local = False

            def canonical_url_for_path(self, path: object) -> str | None:
                path_str = str(path)
                if "---PERIOD---" in path_str:
                    return None
                return f"sftp://example.org{path_str}"

        assert _match_path_candidates(
            PurePosixPath("/shared/data/series_2024.csv"),
            cast(FileSystem, FakeRemoteFS()),
            series_normalized_path="series_---PERIOD---.csv",
        ) == [
            "series_---PERIOD---.csv",
            "/shared/data/series_2024.csv",
            "sftp://example.org/shared/data/series_2024.csv",
        ]

    def test_match_path_candidates_include_local_absolute_series(self):
        from datannurpy.add_folder import _match_path_candidates

        assert _match_path_candidates(
            PurePosixPath("/shared/data/series_2024.csv"),
            None,
            series_normalized_path="series_---PERIOD---.csv",
            root=PurePosixPath("/shared/data"),
        ) == [
            "/shared/data/series_---PERIOD---.csv",
            "series_---PERIOD---.csv",
            "/shared/data/series_2024.csv",
        ]

    def test_match_path_candidates_include_unc_absolute_series(self):
        from pathlib import PureWindowsPath

        from datannurpy.add_folder import _match_path_candidates
        from datannurpy.add_metadata import normalize_match_key
        from datannurpy.scanner.timeseries import series_match_normalized_path

        # Callers tag the generic placeholder with the series frequency before
        # building candidates (here: a yearly group).
        tagged = series_match_normalized_path(
            "my_series_---PERIOD---.sas7bdat", ["2024"]
        )
        candidates = _match_path_candidates(
            PureWindowsPath(r"\\SERVER\SHARE\data\my_series_2024.sas7bdat"),
            None,
            series_normalized_path=tagged,
            root=PureWindowsPath(r"\\SERVER\SHARE\data"),
        )
        absolute = rf"\\SERVER\SHARE\data\{tagged}"
        assert absolute in candidates
        # The absolute UNC candidate normalizes to the same key as a metadata
        # `_match_path` written as `\\SERVER\SHARE\data\my_series_[YYYY].sas7bdat`.
        assert normalize_match_key(absolute) == normalize_match_key(
            r"\\SERVER\SHARE\data\my_series_[YYYY].sas7bdat"
        )

    def test_resolve_match_path_none(self, tmp_path: Path):
        from datannurpy.add_metadata import _resolve_match_path

        assert _resolve_match_path(None, tmp_path) is None

    def test_resolve_match_path_nan(self, tmp_path: Path):
        import math

        from datannurpy.add_metadata import _resolve_match_path

        assert _resolve_match_path(math.nan, tmp_path) is None

    def test_resolve_match_path_existing_file_without_cache(self, tmp_path: Path):
        from datannurpy.add_metadata import _resolve_match_path

        path = tmp_path / "data.csv"
        path.write_text("id\n1\n")

        assert _resolve_match_path(path, tmp_path) == str(path)

    def test_resolve_match_path_uses_exists_cache(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        from datannurpy.add_metadata import _resolve_match_path

        path = tmp_path / "missing.csv"
        exists_calls = 0
        original_exists = Path.exists

        def count_exists(candidate: Path) -> bool:
            nonlocal exists_calls
            if candidate == path:
                exists_calls += 1
            return original_exists(candidate)

        cache: dict[str, bool] = {}
        monkeypatch.setattr(Path, "exists", count_exists)

        assert _resolve_match_path(path, tmp_path, cache) is None
        assert _resolve_match_path(path, tmp_path, cache) is None
        assert exists_calls == 1

    def test_explicit_match_path_does_not_resolve_data_path(self, tmp_path: Path):
        from datannurpy.add_metadata import _load_tables_from_folder

        meta_dir = tmp_path / "meta"
        meta_dir.mkdir()
        match_path = tmp_path / "data.csv"
        match_path.write_text("id\n1\n")
        (meta_dir / "dataset.csv").write_text(
            f"id,data_path,_match_path\nds,missing.csv,{match_path}\n"
        )

        tables = _load_tables_from_folder(meta_dir, {"dataset"}, quiet=True)
        df = tables["dataset"][0]

        assert df.loc[0, "data_path"] == "missing.csv"
        assert df.loc[0, "_match_path"] == str(match_path)

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
        catalog.add_folder(
            data_dir,
            metadata=EntityMetadata(id="src", name="Src"),
            depth="dataset",
        )
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
        catalog.add_folder(data_dir, metadata=EntityMetadata(id="src", name="Src"))
        ds = catalog.dataset.get_by("id", "tsid")
        assert ds is not None
        assert ds.folder_id == "src"
