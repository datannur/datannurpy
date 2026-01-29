"""Tests for statistical file scanner (SAS, SPSS, Stata)."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pyreadstat
import pytest

from datannurpy import Catalog, Folder
from datannurpy.scanner import read_statistical

DATA_DIR = Path(__file__).parent.parent.parent / "data"


class TestReadStatistical:
    """Test read_statistical function."""

    @pytest.mark.skipif(
        not (DATA_DIR / "cars.sas7bdat").exists(),
        reason="SAS test file not available",
    )
    def test_read_statistical_sas(self):
        """read_statistical should return DataFrame for SAS file."""
        df = read_statistical(DATA_DIR / "cars.sas7bdat")
        assert df is not None
        assert len(df) > 0

    def test_read_statistical_unsupported_extension(self, tmp_path: Path):
        """read_statistical should return None for unsupported extension."""
        txt_path = tmp_path / "data.txt"
        txt_path.write_text("hello")

        assert read_statistical(txt_path) is None

    def test_read_statistical_pyreadstat_not_installed(
        self, monkeypatch, tmp_path: Path
    ):
        """read_statistical should return None and warn when pyreadstat not installed."""
        import datannurpy.scanner.statistical as stat_module

        saved_pyreadstat = sys.modules.get("pyreadstat")

        if "pyreadstat" in sys.modules:
            del sys.modules["pyreadstat"]

        original_import = __builtins__["__import__"]

        def mock_import(name, *args, **kwargs):
            if name == "pyreadstat":
                raise ImportError("No module named 'pyreadstat'")
            return original_import(name, *args, **kwargs)

        monkeypatch.setattr("builtins.__import__", mock_import)

        sas_path = tmp_path / "test.sas7bdat"
        sas_path.write_bytes(b"dummy")

        with pytest.warns(UserWarning, match="pyreadstat is required"):
            result = stat_module.read_statistical(sas_path)
        assert result is None

        if saved_pyreadstat:
            sys.modules["pyreadstat"] = saved_pyreadstat

    def test_read_statistical_corrupted_file(self, tmp_path: Path):
        """read_statistical should return None and warn for corrupted files."""
        sas_path = tmp_path / "corrupted.sas7bdat"
        sas_path.write_bytes(b"not a valid sas file")

        with pytest.warns(UserWarning, match="Could not read statistical file"):
            result = read_statistical(sas_path)
        assert result is None


class TestScanStatisticalExceptions:
    """Test scan_statistical exception handling."""

    def test_pyreadstat_not_installed(self, monkeypatch, tmp_path: Path):
        """Statistical scan should raise ImportError if pyreadstat is not installed."""
        sas_file = tmp_path / "test.sas7bdat"
        sas_file.write_bytes(b"")

        monkeypatch.setitem(sys.modules, "pyreadstat", None)

        catalog = Catalog()
        with pytest.raises(ImportError, match="pyreadstat is required"):
            catalog.add_dataset(sas_file, quiet=True)

    def test_corrupted_file(self, tmp_path: Path):
        """Statistical scan should warn and return empty for corrupted files."""
        sas_file = tmp_path / "corrupted.sas7bdat"
        sas_file.write_bytes(b"not a valid sas file")

        catalog = Catalog()
        with pytest.warns(UserWarning, match="Could not read statistical file"):
            catalog.add_dataset(sas_file, quiet=True)

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].nb_row == 0
        assert len(catalog.variables) == 0

    def test_column_without_label(self, tmp_path: Path):
        """Statistical scan should handle columns without labels."""
        df = pd.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
        spss_file = tmp_path / "no_labels.sav"
        pyreadstat.write_sav(df, spss_file)

        catalog = Catalog()
        catalog.add_dataset(spss_file)

        assert len(catalog.variables) == 2
        assert all(v.description is None for v in catalog.variables)


class TestScanSasFiles:
    """Test SAS file scanning."""

    def test_add_folder_scans_sas(self):
        """add_folder should scan SAS files (.sas7bdat extension)."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["cars.sas7bdat"]
        )
        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].delivery_format == "sas"
        assert len(catalog.variables) == 4

    def test_add_folder_extracts_sas_metadata(self):
        """add_folder should extract metadata from SAS files."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["cars.sas7bdat"]
        )
        assert catalog.datasets[0].description == "Written by SAS"
        var_by_name = {v.name: v for v in catalog.variables}
        assert var_by_name["MPG"].description == "miles per gallon"
        assert var_by_name["CYL"].description == "number of cylinders"

    def test_add_folder_sas_integer_conversion(self):
        """add_folder should convert SAS float columns with integer values to integer type."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["cars.sas7bdat"]
        )
        var_by_name = {v.name: v for v in catalog.variables}
        # CYL contains only integers (3, 4, 5, 6, 8)
        assert var_by_name["CYL"].type == "integer"
        # MPG contains decimals (14.5, 16.2, etc.)
        assert var_by_name["MPG"].type == "float"
