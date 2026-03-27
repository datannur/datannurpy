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

    def test_all_encodings_fail(self, tmp_path: Path, monkeypatch, capsys):
        """CSV scan should warn when all encodings fail."""
        import polars as pl

        csv_file = tmp_path / "test.csv"
        csv_file.write_text("col\n1")

        # Mock pl.read_csv to always fail (simulates all encodings failing)
        def mock_read(*args, **kwargs):
            raise pl.exceptions.ComputeError("Invalid UTF-8 sequence")

        monkeypatch.setattr(pl, "read_csv", mock_read)

        catalog = Catalog()
        catalog.add_dataset(csv_file, quiet=False)

        captured = capsys.readouterr()
        assert "Could not parse CSV file" in captured.err

        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 0

    def test_single_column_csv(self, tmp_path: Path):
        """CSV with single column (no separators) should work."""
        csv_file = tmp_path / "single.csv"
        csv_file.write_text("name\nAlice\nBob\n")

        catalog = Catalog()
        catalog.add_dataset(csv_file)

        assert len(catalog.variable.all()) == 1
        assert catalog.variable.all()[0].name == "name"

    def test_detect_separator_with_non_utf8(self, tmp_path: Path):
        """_detect_separator should work with non-utf8 files."""
        from datannurpy.scanner.csv import _detect_separator

        csv_file = tmp_path / "test.csv"
        csv_file.write_bytes(b"pr\xe9nom;age\n")  # é = 0xe9, invalid utf-8

        sep = _detect_separator(csv_file)
        assert sep == ";"

    def test_trailing_separator_drops_empty_column(self, tmp_path: Path):
        """CSV with trailing separator should drop the empty-named column."""
        csv_file = tmp_path / "trailing.csv"
        csv_file.write_text("a;b;\n1;2;\n3;4;\n")

        df = read_csv(csv_file)
        assert df is not None
        assert list(df.columns) == ["a", "b"]
