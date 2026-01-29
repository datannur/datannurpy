"""Tests for CSV scanner."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

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

    def test_latin1_semicolon_delimiter(self):
        """CSV with latin1 encoding and semicolon delimiter should be scanned correctly."""
        catalog = Catalog()
        catalog.add_dataset(CSV_DIR / "legacy_encoding.csv")

        assert len(catalog.variables) == 4
        assert catalog.datasets[0].nb_row == 3

    def test_explicit_encoding(self):
        """CSV scan with explicit encoding should work."""
        catalog = Catalog()
        catalog.add_dataset(CSV_DIR / "legacy_encoding.csv", csv_encoding="CP1252")

        assert len(catalog.variables) == 4

    def test_all_encodings_fail(self, tmp_path: Path, monkeypatch):
        """CSV scan should warn when all encodings fail."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("col\n1")

        def mock_read_csv(*args, **kwargs):
            raise duckdb.InvalidInputException("Mocked encoding error")

        monkeypatch.setattr("ibis.backends.duckdb.Backend.read_csv", mock_read_csv)

        catalog = Catalog()
        with pytest.warns(UserWarning, match="Could not parse CSV file"):
            catalog.add_dataset(csv_file, quiet=True)

        assert len(catalog.datasets) == 1
        assert len(catalog.variables) == 0
