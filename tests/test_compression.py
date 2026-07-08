"""Tests for transparent gzip (``.csv.gz``) handling across all sources."""

from __future__ import annotations

import gzip
import io
import uuid
from pathlib import Path

import fsspec
import pytest

from datannurpy import Catalog
from datannurpy.compression import (
    DecompressionLimitError,
    bounded_gzip_stream,
    compression_suffix,
    decompressed_cap,
    is_gzipped,
    strip_compression_suffix,
)
from datannurpy.scanner.format_detect import format_from_extension
from datannurpy.scanner.utils import supported_format_for

_CSV = "id,name,score\n1,alice,10.5\n2,bob,20.0\n3,carol,30.5\n"


# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #
class TestSuffixHelpers:
    @pytest.mark.parametrize(
        "name,expected",
        [
            ("sales.csv.gz", ".gz"),
            ("sales.CSV.GZ", ".gz"),
            ("sales.csv", ""),
            ("archive.zip", ""),  # .zip is a separate, out-of-scope concern
            ("noext", ""),
        ],
    )
    def test_compression_suffix(self, name: str, expected: str) -> None:
        assert compression_suffix(name) == expected

    def test_is_gzipped(self) -> None:
        assert is_gzipped("a.csv.gz")
        assert not is_gzipped("a.csv")

    @pytest.mark.parametrize(
        "name,expected",
        [
            ("sales.csv.gz", "sales.csv"),
            ("sales.csv", "sales.csv"),  # no-op when uncompressed
            ("dir/sales.tsv.gz", "dir/sales.tsv"),
        ],
    )
    def test_strip_compression_suffix(self, name: str, expected: str) -> None:
        assert strip_compression_suffix(name) == expected

    def test_decompressed_cap_floor_and_ratio(self) -> None:
        # Small file: the absolute floor wins.
        assert decompressed_cap(10) == 1 << 30
        # Large file: the ratio wins.
        assert decompressed_cap(1 << 30) == (1 << 30) * 200

    def test_format_resolution_sees_through_gz(self) -> None:
        assert format_from_extension("sales.csv.gz") == "csv"
        assert format_from_extension("sales.csv.gz?token=x") == "csv"
        assert supported_format_for("sales.csv.gz") == "csv"
        assert supported_format_for("sales.unknown.gz") is None

    def test_gzip_only_seen_through_for_decompressible_formats(self) -> None:
        # A gzip wrapping a format we don't decompress stays unsupported, rather than
        # being admitted and then failing to scan as raw gzip bytes.
        assert supported_format_for("data.parquet.gz") is None
        assert supported_format_for("data.dta.gz") is None
        assert format_from_extension("data.parquet.gz") is None
        # ...but the uncompressed form is of course still supported.
        assert supported_format_for("data.parquet") == "parquet"


# --------------------------------------------------------------------------- #
# Bounded gzip stream (bomb guard)
# --------------------------------------------------------------------------- #
class TestBoundedGzipStream:
    def test_reads_full_payload(self) -> None:
        data = b"hello world\n" * 1000
        stream = bounded_gzip_stream(io.BytesIO(gzip.compress(data)), cap=len(data))
        assert stream.readable()
        assert stream.read() == data
        stream.close()

    def test_raises_past_cap(self) -> None:
        data = b"x" * 10_000
        stream = bounded_gzip_stream(io.BytesIO(gzip.compress(data)), cap=100)
        with pytest.raises(DecompressionLimitError):
            stream.read()


# --------------------------------------------------------------------------- #
# Local integration
# --------------------------------------------------------------------------- #
class TestLocalGzip:
    def test_value_depth_scans_content(self, tmp_path: Path) -> None:
        (tmp_path / "sales.csv.gz").write_bytes(gzip.compress(_CSV.encode()))
        catalog = Catalog(quiet=True)
        catalog.add_dataset(tmp_path / "sales.csv.gz", depth="value")
        ds = catalog.dataset.all()[0]
        assert ds.delivery_format == "csv"
        assert ds.nb_row == 3
        assert [v.name for v in catalog.variable.all()] == ["id", "name", "score"]

    def test_variable_depth_reads_schema(self, tmp_path: Path) -> None:
        (tmp_path / "sales.csv.gz").write_bytes(gzip.compress(_CSV.encode()))
        catalog = Catalog(quiet=True)
        catalog.add_dataset(tmp_path / "sales.csv.gz", depth="variable")
        assert catalog.dataset.all()[0].nb_row is None
        assert [v.name for v in catalog.variable.all()] == ["id", "name", "score"]

    def test_id_and_name_match_uncompressed_twin(self, tmp_path: Path) -> None:
        (tmp_path / "sales.csv.gz").write_bytes(gzip.compress(_CSV.encode()))
        (tmp_path / "plain.csv").write_text(_CSV)
        gz = Catalog(quiet=True)
        gz.add_dataset(tmp_path / "sales.csv.gz", depth="dataset")
        plain = Catalog(quiet=True)
        plain.add_dataset(tmp_path / "plain.csv", depth="dataset")
        gz_ds, plain_ds = gz.dataset.all()[0], plain.dataset.all()[0]
        # A .gz strips to the same stem/name as if it were the plain file.
        assert gz_ds.id == "sales"
        assert gz_ds.name == "sales"
        assert plain_ds.name == "plain"

    def test_cp1252_gzip_is_transcoded(self, tmp_path: Path) -> None:
        # The whole reason we decompress ourselves: a non-UTF-8 gzipped CSV must still
        # go through the encoding transcode. "café" in cp1252 is not valid UTF-8.
        content = "id;city\n1;café\n".encode("cp1252")
        (tmp_path / "fr.csv.gz").write_bytes(gzip.compress(content))
        catalog = Catalog(quiet=True)
        catalog.add_dataset(tmp_path / "fr.csv.gz", depth="value")
        assert catalog.dataset.all()[0].nb_row == 1
        assert [v.name for v in catalog.variable.all()] == ["id", "city"]

    def test_bomb_is_refused_not_crashed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("datannurpy.compression._DECOMP_MIN_CAP", 1024)
        monkeypatch.setattr("datannurpy.compression._DECOMP_MAX_RATIO", 2)
        payload = "col\n" + "x" * 100_000 + "\n"
        (tmp_path / "bomb.csv.gz").write_bytes(gzip.compress(payload.encode()))
        catalog = Catalog(quiet=True)
        catalog.add_dataset(tmp_path / "bomb.csv.gz", depth="value")
        # Refused cleanly: no exception, and the oversized content was not ingested.
        assert catalog.dataset.all()[0].nb_row != 1

    def test_folder_scan_does_not_ignore_gzip(self, tmp_path: Path) -> None:
        (tmp_path / "sales.csv.gz").write_bytes(gzip.compress(_CSV.encode()))
        (tmp_path / "other.csv").write_text(_CSV)
        catalog = Catalog(quiet=True)
        catalog.add_folder(tmp_path, depth="value")
        datasets = catalog.dataset.all()
        assert len(datasets) == 2  # the .gz is not silently dropped
        assert [d.nb_row for d in datasets] == [3, 3]
        assert all(d.delivery_format == "csv" for d in datasets)


# --------------------------------------------------------------------------- #
# Remote integration (memory:// exercises the full remote code path)
# --------------------------------------------------------------------------- #
class TestRemoteGzip:
    @pytest.fixture
    def memory_root(self) -> str:
        return f"/{uuid.uuid4().hex}"

    @pytest.fixture
    def memory_fs(self, memory_root: str) -> fsspec.AbstractFileSystem:
        fs = fsspec.filesystem("memory")
        fs.mkdir(memory_root)
        return fs

    def test_remote_value_depth(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        memory_fs.pipe(f"{memory_root}/sales.csv.gz", gzip.compress(_CSV.encode()))
        catalog = Catalog(quiet=True)
        catalog.add_dataset(f"memory://{memory_root}/sales.csv.gz", depth="value")
        ds = catalog.dataset.all()[0]
        assert ds.delivery_format == "csv"
        assert ds.nb_row == 3

    def test_remote_variable_depth_streams_header(
        self, memory_fs: fsspec.AbstractFileSystem, memory_root: str
    ) -> None:
        memory_fs.pipe(f"{memory_root}/sales.csv.gz", gzip.compress(_CSV.encode()))
        catalog = Catalog(quiet=True)
        catalog.add_dataset(f"memory://{memory_root}/sales.csv.gz", depth="variable")
        assert [v.name for v in catalog.variable.all()] == ["id", "name", "score"]
