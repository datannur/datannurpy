"""Tests for scanner discovery module."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from datannurpy import Catalog
from datannurpy.scanner.discovery import (
    DatasetInfo,
    _match_path_index_keys,
    _match_path_keys,
    compute_scan_plan,
    discover_datasets,
)


class TestLocalDiscoveryIsSingleWalkNoPerFileStat:
    """The local discovery walks each directory once and reads mtime from the
    scandir entry — no double walk, no per-file stat (matters on NFS/SMB mounts)."""

    def test_lists_each_dir_once_and_captures_mtime_from_scandir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        for sub in ("", "a", "b", "b/deep"):
            base = tmp_path / sub
            base.mkdir(parents=True, exist_ok=True)
            for i in range(2):
                (base / f"f{i}.csv").write_text("x,y\n1,2")

        scandir_dirs: list[str] = []
        real_scandir = os.scandir
        real_path_stat = Path.stat
        stat_calls = 0

        def counting_scandir(path: str | os.PathLike[str]) -> Any:
            scandir_dirs.append(str(path))
            return real_scandir(path)

        def counting_path_stat(self: Path, *a: Any, **k: Any) -> Any:
            nonlocal stat_calls
            stat_calls += 1
            return real_path_stat(self, *a, **k)

        monkeypatch.setattr(os, "scandir", counting_scandir)
        monkeypatch.setattr(Path, "stat", counting_path_stat)

        result = discover_datasets(tmp_path, fs=None)

        assert len(result.datasets) == 8
        # 4 directories, each listed exactly once (no second walk for parquet).
        assert len(scandir_dirs) == 4
        assert len(set(scandir_dirs)) == 4
        # mtime came from the scandir DirEntry, not a per-file Path.stat.
        assert stat_calls == 0
        # And the captured mtimes are correct.
        monkeypatch.setattr(Path, "stat", real_path_stat)
        for ds in result.datasets:
            assert ds.mtime == int(Path(ds.path).stat().st_mtime)


def _info(path: Path, **kwargs: object) -> DatasetInfo:
    """Build a minimal DatasetInfo for match-key tests."""
    return DatasetInfo(path=path, format="csv", mtime=0, **kwargs)  # type: ignore[arg-type]


class TestDatasetInfo:
    """Test DatasetInfo dataclass."""

    def test_create_dataset_info(self, tmp_path: Path):
        """DatasetInfo should hold path, format, mtime."""
        info = DatasetInfo(path=tmp_path / "test.csv", format="csv", mtime=1234567890)
        assert info.path == tmp_path / "test.csv"
        assert info.format == "csv"
        assert info.mtime == 1234567890


class TestComputeScanPlan:
    """Test compute_scan_plan function."""

    def test_new_dataset_goes_to_scan(self, tmp_path: Path):
        """New datasets (not in catalog) should be in to_scan."""
        catalog = Catalog()
        info = DatasetInfo(path=tmp_path / "new.csv", format="csv", mtime=1234567890)

        plan = compute_scan_plan([info], catalog, refresh=False)

        assert len(plan.to_scan) == 1
        assert len(plan.to_skip) == 0

    def test_unchanged_dataset_goes_to_skip(self, tmp_path: Path):
        """Unchanged datasets (same mtime) should be in to_skip."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")
        mtime = int(csv_file.stat().st_mtime)

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        info = DatasetInfo(path=csv_file, format="csv", mtime=mtime)
        plan = compute_scan_plan([info], catalog, refresh=False)

        assert len(plan.to_scan) == 0
        assert len(plan.to_skip) == 1

    def test_modified_dataset_goes_to_scan(self, tmp_path: Path):
        """Modified datasets (different mtime) should be in to_scan."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        info = DatasetInfo(path=csv_file, format="csv", mtime=9999999999)
        plan = compute_scan_plan([info], catalog, refresh=False)

        assert len(plan.to_scan) == 1
        assert len(plan.to_skip) == 0

    def test_refresh_forces_scan(self, tmp_path: Path):
        """With refresh=True, unchanged datasets should be in to_scan."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")
        mtime = int(csv_file.stat().st_mtime)

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        info = DatasetInfo(path=csv_file, format="csv", mtime=mtime)
        plan = compute_scan_plan([info], catalog, refresh=True)

        assert len(plan.to_scan) == 1
        assert len(plan.to_skip) == 0

    def test_mixed_datasets(self, tmp_path: Path):
        """Should correctly categorize multiple datasets."""
        csv_file = tmp_path / "existing.csv"
        csv_file.write_text("a,b\n1,2\n")
        mtime = int(csv_file.stat().st_mtime)

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        datasets = [
            DatasetInfo(path=csv_file, format="csv", mtime=mtime),
            DatasetInfo(path=tmp_path / "new.csv", format="csv", mtime=1234567890),
        ]

        plan = compute_scan_plan(datasets, catalog, refresh=False)

        assert len(plan.to_scan) == 1
        assert len(plan.to_skip) == 1

    def test_match_path_keys_include_relative_path(self, tmp_path: Path):
        """Path aliases include runtime absolute and persisted relative keys."""
        csv_file = tmp_path / "nested" / "data.csv"
        csv_file.parent.mkdir()
        csv_file.write_text("a\n1\n")

        assert _match_path_keys(_info(csv_file), tmp_path) == [
            str(csv_file).replace("\\", "/"),
            "nested/data.csv",
        ]

    def test_match_path_keys_handle_root_and_outside_paths(self, tmp_path: Path):
        """Path aliases cover root folders and paths outside the scan root."""
        outside = tmp_path.parent / "outside.csv"
        norm = lambda p: str(p).replace("\\", "/")  # noqa: E731

        assert _match_path_keys(_info(tmp_path), tmp_path) == [norm(tmp_path), ""]
        assert _match_path_keys(_info(outside), tmp_path) == [norm(outside)]
        assert _match_path_keys(_info(Path("data.csv")), Path(".")) == ["data.csv"]

    def test_match_path_keys_include_series_normalized_keys(self, tmp_path: Path):
        """A time series also yields its frequency-tagged normalized keys."""
        latest = tmp_path / "series_2024.csv"
        info = _info(
            latest,
            series_normalized_path="series_---PERIOD---.csv",
            series_files=[("2022", tmp_path / "series_2022.csv"), ("2024", latest)],
        )

        keys = _match_path_keys(info, tmp_path)
        # Frequency-tagged (yearly) normalized key, relative and absolute.
        assert "series_---PERIOD-Y---.csv" in keys
        assert str(tmp_path / "series_---PERIOD-Y---.csv").replace("\\", "/") in keys

    def test_match_path_index_keys_split_url_path(self):
        """A URL match path also indexes its path-only form for lookup."""
        assert _match_path_index_keys(
            "sftp://example.org/shared/data/series_---PERIOD-Y---.csv"
        ) == [
            "sftp://example.org/shared/data/series_---PERIOD-Y---.csv",
            "/shared/data/series_---PERIOD-Y---.csv",
        ]
        # Non-URL keys pass through, separator-normalized.
        assert _match_path_index_keys(r"C:\data\x.csv") == ["C:/data/x.csv"]
        # A URL-like value without a path keeps only the full key.
        assert _match_path_index_keys("sftp://example.org") == ["sftp://example.org"]

    def test_match_path_keys_series_without_files_or_root(self):
        """Series keys handle a missing periods list and a missing root."""
        info = _info(
            Path("/data/series_2024.csv"),
            series_normalized_path="series_---PERIOD-Y---.csv",
        )
        # series_files None -> the normalized path is used as-is (no re-tagging);
        # root None -> no absolute series candidate is added.
        assert _match_path_keys(info, None) == [
            "/data/series_2024.csv",
            "series_---PERIOD-Y---.csv",
        ]

    def test_scan_plan_ignores_datasets_without_match_path(self, tmp_path: Path):
        """Existing datasets with an empty _match_path are skipped in the index."""
        from datannurpy.schema import Dataset

        catalog = Catalog()
        catalog.dataset.add(Dataset(id="no_match", name="No match"))

        plan = compute_scan_plan([], catalog, refresh=False)

        assert plan.to_scan == []
        assert plan.to_skip == []
        assert plan.existing_by_path == {}


class TestDiscoverDatasets:
    """Test discover_datasets function."""

    def test_discovers_csv_files(self, tmp_path: Path):
        """Should discover CSV files."""
        (tmp_path / "data.csv").write_text("a,b\n1,2\n")

        result = discover_datasets(tmp_path)

        assert len(result.datasets) == 1
        assert result.datasets[0].format == "csv"

    def test_discovers_parquet_files(self, tmp_path: Path):
        """Should discover parquet files."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.table({"a": [1, 2], "b": [3, 4]})
        pq.write_table(table, tmp_path / "data.parquet")

        result = discover_datasets(tmp_path)

        assert len(result.datasets) == 1
        assert result.datasets[0].format == "parquet"

    def test_discovers_multiple_formats(self, tmp_path: Path):
        """Should discover all supported formats."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        (tmp_path / "data.csv").write_text("a,b\n1,2\n")
        table = pa.table({"a": [1]})
        pq.write_table(table, tmp_path / "data.parquet")

        result = discover_datasets(tmp_path)

        assert len(result.datasets) == 2
        formats = {d.format for d in result.datasets}
        assert formats == {"csv", "parquet"}

    def test_respects_include_pattern(self, tmp_path: Path):
        """Should filter by include pattern."""
        (tmp_path / "keep.csv").write_text("a\n1\n")
        (tmp_path / "skip.csv").write_text("b\n2\n")

        result = discover_datasets(tmp_path, include=["keep.csv"])

        assert len(result.datasets) == 1
        assert result.datasets[0].path.name == "keep.csv"

    def test_respects_exclude_pattern(self, tmp_path: Path):
        """Should filter by exclude pattern."""
        (tmp_path / "keep.csv").write_text("a\n1\n")
        (tmp_path / "skip.csv").write_text("b\n2\n")

        result = discover_datasets(tmp_path, exclude=["skip.csv"])

        assert len(result.datasets) == 1
        assert result.datasets[0].path.name == "keep.csv"

    def test_returns_sorted_by_path(self, tmp_path: Path):
        """Datasets should be sorted by path."""
        (tmp_path / "z.csv").write_text("a\n1\n")
        (tmp_path / "a.csv").write_text("b\n2\n")

        result = discover_datasets(tmp_path)

        assert len(result.datasets) == 2
        assert result.datasets[0].path.name == "a.csv"
        assert result.datasets[1].path.name == "z.csv"

    def test_excludes_files_inside_parquet_directories(self, tmp_path: Path):
        """Non-parquet files inside Hive-partitioned directories should be excluded."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create a Hive-partitioned parquet directory (year=2024/)
        pq_dir = tmp_path / "partitioned"
        partition_dir = pq_dir / "year=2024"
        partition_dir.mkdir(parents=True)
        table = pa.table({"a": [1, 2]})
        pq.write_table(table, partition_dir / "part-0.parquet")
        # CSV file directly inside the parquet directory
        (pq_dir / "extra.csv").write_text("x\n1\n")
        # CSV file in a subdirectory of parquet directory (tests parents check)
        (partition_dir / "nested.csv").write_text("z\n3\n")
        # CSV file outside should be included
        (tmp_path / "outside.csv").write_text("y\n2\n")

        result = discover_datasets(tmp_path)

        # Should find: partitioned (as parquet dir) + outside.csv
        # Should NOT find: extra.csv or nested.csv (inside parquet dir)
        assert len(result.datasets) == 2
        paths = {d.path.name for d in result.datasets}
        assert "partitioned" in paths
        assert "outside.csv" in paths
        assert "extra.csv" not in paths
        assert "nested.csv" not in paths

    """Test build_variables_from_schema function."""

    def test_builds_variables_from_pyarrow_schema(self):
        """Should create variables from PyArrow schema."""
        import pyarrow as pa

        from datannurpy.scanner.utils import build_variables_from_schema

        schema = pa.schema(
            [
                pa.field("int_col", pa.int64()),
                pa.field("str_col", pa.string()),
                pa.field("float_col", pa.float64()),
                pa.field("bool_col", pa.bool_()),
                pa.field("date_col", pa.date32()),
                pa.field("time_col", pa.time64("us")),
                pa.field("ts_col", pa.timestamp("us")),
                pa.field("bin_col", pa.binary()),
                pa.field("null_col", pa.null()),
            ]
        )

        variables = build_variables_from_schema(schema, "test_ds")

        assert len(variables) == 9
        types = {v.name: v.type for v in variables}
        assert types["int_col"] == "integer"
        assert types["str_col"] == "string"
        assert types["float_col"] == "float"
        assert types["bool_col"] == "boolean"
        assert types["date_col"] == "date"
        assert types["time_col"] == "time"
        assert types["ts_col"] == "datetime"
        assert types["bin_col"] == "binary"
        assert types["null_col"] == "null"

    def test_pyarrow_large_string_type(self):
        """Should handle large string type."""
        import pyarrow as pa

        from datannurpy.scanner.utils import build_variables_from_schema

        schema = pa.schema([pa.field("large_str", pa.large_string())])
        variables = build_variables_from_schema(schema, "ds")

        assert variables[0].type == "string"

    def test_pyarrow_large_binary_type(self):
        """Should handle large binary type."""
        import pyarrow as pa

        from datannurpy.scanner.utils import build_variables_from_schema

        schema = pa.schema([pa.field("large_bin", pa.large_binary())])
        variables = build_variables_from_schema(schema, "ds")

        assert variables[0].type == "binary"

    def test_pyarrow_unknown_type(self):
        """Should return 'unknown' for unrecognized types."""
        import pyarrow as pa

        from datannurpy.scanner.utils import build_variables_from_schema

        # Use a complex type that's not explicitly handled
        schema = pa.schema([pa.field("list_col", pa.list_(pa.int32()))])
        variables = build_variables_from_schema(schema, "ds")

        assert variables[0].type == "unknown"
