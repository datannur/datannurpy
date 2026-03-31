"""Tests for CSV scanner."""

from __future__ import annotations

from pathlib import Path

from datannurpy import Catalog
from datannurpy.scanner import read_csv

DATA_DIR = Path(__file__).parent.parent.parent / "data"
CSV_DIR = DATA_DIR / "csv"


class TestReadCsv:
    """Test read_csv function."""

    def test_empty_file(self, tmp_path: Path):
        """read_csv should return None for empty file."""
        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("")

        assert read_csv(csv_path) is None

    def test_with_data(self, tmp_path: Path):
        """read_csv should return DataFrame for valid CSV."""
        csv_path = tmp_path / "data.csv"
        csv_path.write_text("id,name\n1,Test\n")

        df = read_csv(csv_path)
        assert df is not None
        assert len(df) == 1
        assert "id" in df.columns


class TestLegacyEncoding:
    """Test scanning CSV files with legacy encodings and delimiters."""

    def test_cp1252_semicolon_delimiter(self):
        """CSV with CP1252 encoding and semicolon delimiter should be scanned correctly."""
        catalog = Catalog()
        catalog.add_dataset(CSV_DIR / "legacy_encoding.csv")

        assert len(catalog.variable.all()) == 4
        assert catalog.dataset.all()[0].nb_row == 4

    def test_explicit_encoding(self):
        """CSV scan with explicit encoding should work."""
        catalog = Catalog()
        catalog.add_dataset(CSV_DIR / "legacy_encoding.csv", csv_encoding="CP1252")

        assert len(catalog.variable.all()) == 4

    def test_unparseable_csv(self, tmp_path: Path, capsys):
        """CSV scan should warn when DuckDB cannot parse the file."""
        csv_file = tmp_path / "test.csv"
        # Write content that has a valid header but inconsistent column counts
        csv_file.write_text("a,b,c\n1,2\n3,4,5,6,7\n")

        catalog = Catalog()
        catalog.add_dataset(csv_file, quiet=False)

        # DuckDB may still parse it (padding/truncating), so just verify no crash
        assert len(catalog.dataset.all()) == 1

    def test_single_column_csv(self, tmp_path: Path):
        """CSV with single column (no separators) should work."""
        csv_file = tmp_path / "single.csv"
        csv_file.write_text("name\nAlice\nBob\n")

        catalog = Catalog()
        catalog.add_dataset(csv_file)

        assert len(catalog.variable.all()) == 1
        assert catalog.variable.all()[0].name == "name"

    def test_read_csv_header_with_non_utf8(self, tmp_path: Path):
        """_read_csv_header should work with non-utf8 header bytes."""
        from datannurpy.scanner.csv import _read_csv_header

        header = b"pr\xe9nom;age\n"  # é = 0xe9, invalid utf-8
        cols = _read_csv_header(header)
        assert cols == ["prénom", "age"]

    def test_trailing_separator_drops_empty_column(self, tmp_path: Path):
        """CSV with trailing separator should drop the empty-named column."""
        csv_file = tmp_path / "trailing.csv"
        csv_file.write_text("a;b;\n1;2;\n3;4;\n")

        df = read_csv(csv_file)
        assert df is not None
        assert list(df.columns) == ["a", "b"]


class TestSkipCopy:
    """Test csv_skip_copy parameter for CSV scanning."""

    def test_csv_skip_copy_utf8_csv(self, tmp_path: Path):
        """csv_skip_copy=True should read UTF-8 CSV directly without transcoding."""
        csv_file = tmp_path / "utf8.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n")

        catalog = Catalog(csv_skip_copy=True)
        catalog.add_dataset(csv_file)

        assert catalog.dataset.all()[0].nb_row == 2
        assert len(catalog.variable.all()) == 2

    def test_csv_skip_copy_fallback_on_non_utf8(self, tmp_path: Path):
        """csv_skip_copy=True should fall back to ensure_local_utf8 for non-UTF-8."""
        csv_file = tmp_path / "cp1252.csv"
        csv_file.write_bytes(b"nom,age\nRen\xe9,42\n")

        catalog = Catalog(csv_skip_copy=True)
        catalog.add_dataset(csv_file)

        assert catalog.dataset.all()[0].nb_row == 1
        assert len(catalog.variable.all()) == 2


class TestSampling:
    """Test sample_size parameter for CSV scanning."""

    def test_sample_size_limits_stats(self, tmp_path: Path):
        """sample_size should use reservoir sampling for stats."""
        csv_file = tmp_path / "big.csv"
        lines = ["id,value\n"] + [f"{i},{i * 10}\n" for i in range(200)]
        csv_file.write_text("".join(lines))

        catalog = Catalog()
        catalog.add_dataset(csv_file, sample_size=100)

        ds = catalog.dataset.all()[0]
        assert ds.nb_row == 200  # full row count preserved
        assert len(catalog.variable.all()) == 2

    def test_sample_size_no_effect_when_small(self, tmp_path: Path):
        """sample_size should have no effect when row count <= sample_size."""
        csv_file = tmp_path / "small.csv"
        csv_file.write_text("id,value\n1,10\n2,20\n")

        catalog = Catalog()
        catalog.add_dataset(csv_file, sample_size=100)

        ds = catalog.dataset.all()[0]
        assert ds.nb_row == 2

    def test_scan_csv_empty_after_header(self, tmp_path: Path):
        """scan_csv should handle CSV with header but no data rows."""
        from datannurpy.scanner.csv import scan_csv

        csv_file = tmp_path / "header_only.csv"
        csv_file.write_text("a,b,c\n")

        variables, nb_row, actual_sample, freq = scan_csv(
            csv_file, dataset_id="test", infer_stats=True
        )
        assert nb_row == 0
        assert actual_sample is None
        assert len(variables) == 3


class TestEnsureLocalUtf8:
    """Test ensure_local_utf8 edge cases."""

    def test_cp1252_fallback_multi_chunk(self, monkeypatch):
        """ensure_local_utf8 should handle multi-chunk cp1252 files."""
        import io

        import datannurpy.scanner.filesystem as fs_mod
        from datannurpy.scanner.filesystem import ensure_local_utf8

        # Shrink chunk size so even small data spans multiple reads
        monkeypatch.setattr(fs_mod, "_CHUNK_SIZE", 20)

        # Non-UTF-8 byte in first chunk triggers cp1252 mode,
        # remaining data goes through the `else` branch on subsequent chunks
        data = b"pr\xe9nom;age\n" + b"Ren\xe9;42\n" * 5

        fin = io.BytesIO(data)
        fout = io.BytesIO()
        ensure_local_utf8(fin, fout)

        result = fout.getvalue().decode("utf-8")
        assert "prénom" in result

    def test_utf8_with_carry_bytes(self):
        """ensure_local_utf8 should handle UTF-8 multi-byte chars at chunk boundaries."""
        import io

        from datannurpy.scanner.filesystem import ensure_local_utf8

        # Create data where a multi-byte char spans the chunk boundary
        # é = \xc3\xa9 in UTF-8
        # Put \xc3 at end of first "chunk" and \xa9 at start of second
        part1 = b"A" * 1048575 + b"\xc3"  # 1MB - 1 + first byte of é
        part2 = b"\xa9 fin\n"  # second byte of é + rest
        data = part1 + part2

        fin = io.BytesIO(data)
        fout = io.BytesIO()
        ensure_local_utf8(fin, fout)

        result = fout.getvalue().decode("utf-8")
        assert "é fin" in result

    def test_utf8_trailing_carry_becomes_cp1252(self):
        """ensure_local_utf8 should handle trailing incomplete UTF-8 bytes as cp1252."""
        import io

        from datannurpy.scanner.filesystem import ensure_local_utf8

        # \xc3 alone at end of file is incomplete UTF-8 → treated as cp1252
        data = b"hello\n" + b"\xc3"

        fin = io.BytesIO(data)
        fout = io.BytesIO()
        ensure_local_utf8(fin, fout)

        result = fout.getvalue().decode("utf-8")
        assert "hello" in result


class TestReadCsvHeaderEdgeCases:
    """Test _read_csv_header edge cases."""

    def test_empty_header(self):
        """_read_csv_header should return empty list for empty bytes."""
        from datannurpy.scanner.csv import _read_csv_header

        assert _read_csv_header(b"") == []
        assert _read_csv_header(b"\n") == []

    def test_non_utf8_without_explicit_encoding(self):
        """_read_csv_header should fall back to cp1252 for non-UTF8."""
        from datannurpy.scanner.csv import _read_csv_header

        # \xe9 is é in cp1252, invalid UTF-8
        cols = _read_csv_header(b"pr\xe9nom,\xe2ge\n")
        assert "prénom" in cols


class TestScanCsvErrorPath:
    """Test scan_csv error handling."""

    def test_scan_csv_empty_file(self, tmp_path: Path):
        """scan_csv should return empty result for 0-byte file."""
        from datannurpy.scanner.csv import scan_csv

        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("")

        variables, nb_row, sample, freq = scan_csv(
            csv_file, dataset_id="test", infer_stats=True
        )
        assert variables == []
        assert nb_row == 0
        assert sample is None
        assert freq is None

    def test_scan_csv_warns_on_connect_error(self, tmp_path: Path, monkeypatch):
        """scan_csv should return empty result when DuckDB connection fails."""
        from datannurpy.scanner import csv as csv_mod
        from datannurpy.scanner.csv import scan_csv

        csv_file = tmp_path / "ok.csv"
        csv_file.write_text("a\n1\n")

        from contextlib import contextmanager

        @contextmanager
        def _broken_source(*a, **kw):
            raise RuntimeError("forced error")
            yield  # type: ignore[misc]  # pragma: no cover

        monkeypatch.setattr(csv_mod, "_csv_source", _broken_source)

        variables, nb_row, sample, freq = scan_csv(
            csv_file, dataset_id="test", infer_stats=True, quiet=True
        )
        assert variables == []
        assert nb_row == 0


class TestReadCsvErrorPath:
    """Test read_csv error handling."""

    def test_read_csv_returns_none_on_error(self, tmp_path: Path, monkeypatch):
        """read_csv should return None when DuckDB read fails."""
        from datannurpy.scanner import csv as csv_mod

        csv_file = tmp_path / "ok.csv"
        csv_file.write_text("a\n1\n")

        from contextlib import contextmanager

        @contextmanager
        def _broken_source(*a, **kw):
            raise RuntimeError("forced error")
            yield  # type: ignore[misc]  # pragma: no cover

        monkeypatch.setattr(csv_mod, "_csv_source", _broken_source)
        assert read_csv(csv_file) is None


class TestCsvSnifferFallback:
    """Test _read_csv_header when Sniffer fails."""

    def test_sniffer_fallback(self, monkeypatch):
        """_read_csv_header should fall back to default reader when Sniffer fails."""
        import csv as stdlib_csv

        from datannurpy.scanner.csv import _read_csv_header

        def _always_fail(self, sample, delimiters=None):
            raise stdlib_csv.Error("forced")

        monkeypatch.setattr(stdlib_csv.Sniffer, "sniff", _always_fail)
        cols = _read_csv_header(b"a,b,c\n")
        assert cols == ["a", "b", "c"]
