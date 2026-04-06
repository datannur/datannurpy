"""Tests for statistical file scanner (SAS, SPSS, Stata)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pyreadstat
import pytest

from datannurpy import Catalog, Folder
from datannurpy.scanner import read_statistical
from datannurpy.scanner.statistical import scan_statistical

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
        self, monkeypatch, tmp_path: Path, capsys
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

        result = stat_module.read_statistical(sas_path, quiet=False)
        captured = capsys.readouterr()
        assert "pyreadstat is required" in captured.err
        assert result is None

        if saved_pyreadstat:
            sys.modules["pyreadstat"] = saved_pyreadstat

    def test_read_statistical_corrupted_file(self, tmp_path: Path, capsys):
        """read_statistical should return None and warn for corrupted files."""
        sas_path = tmp_path / "corrupted.sas7bdat"
        sas_path.write_bytes(b"not a valid sas file")

        result = read_statistical(sas_path, quiet=False)
        captured = capsys.readouterr()
        assert "✗ corrupted.sas7bdat" in captured.err
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

    def test_corrupted_file(self, tmp_path: Path, capsys):
        """Statistical scan should warn and return empty for corrupted files."""
        sas_file = tmp_path / "corrupted.sas7bdat"
        sas_file.write_bytes(b"not a valid sas file")

        catalog = Catalog()
        catalog.add_dataset(sas_file, quiet=False)

        captured = capsys.readouterr()
        assert "✗ corrupted.sas7bdat" in captured.err

        assert len(catalog.dataset.all()) == 1
        assert catalog.dataset.all()[0].nb_row == 0
        assert len(catalog.variable.all()) == 0

    def test_column_without_label(self, tmp_path: Path):
        """Statistical scan should handle columns without labels."""
        df = pd.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
        spss_file = tmp_path / "no_labels.sav"
        pyreadstat.write_sav(df, spss_file)

        catalog = Catalog()
        catalog.add_dataset(spss_file)

        assert len(catalog.variable.all()) == 2
        assert all(v.description is None for v in catalog.variable.all())


class TestScanSasFiles:
    """Test SAS file scanning."""

    def test_add_folder_scans_sas(self):
        """add_folder should scan SAS files (.sas7bdat extension)."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["cars.sas7bdat"]
        )
        assert len(catalog.dataset.all()) == 1
        assert catalog.dataset.all()[0].delivery_format == "sas"
        assert len(catalog.variable.all()) == 4

    def test_add_folder_extracts_sas_metadata(self):
        """add_folder should extract metadata from SAS files."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["cars.sas7bdat"]
        )
        assert catalog.dataset.all()[0].description == "Written by SAS"
        var_by_name = {v.name: v for v in catalog.variable.all()}
        assert var_by_name["MPG"].description == "miles per gallon"
        assert var_by_name["CYL"].description == "number of cylinders"

    def test_add_folder_sas_integer_conversion(self):
        """add_folder should convert SAS float columns with integer values to integer type."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["cars.sas7bdat"]
        )
        var_by_name = {v.name: v for v in catalog.variable.all()}
        # CYL contains only integers (3, 4, 5, 6, 8)
        assert var_by_name["CYL"].type == "integer"
        # MPG contains decimals (14.5, 16.2, etc.)
        assert var_by_name["MPG"].type == "float"


class TestScanStatisticalEdgeCases:
    """Test edge cases: empty files, sampling, pipeline errors."""

    def test_empty_file_returns_empty_variables(self, tmp_path: Path):
        """scan_statistical should handle an empty SPSS file (0 rows)."""
        df = pd.DataFrame({"a": pd.Series([], dtype="float64")})
        spss_file = tmp_path / "empty.sav"
        pyreadstat.write_sav(df, spss_file)

        variables, row_count, actual_sample_size, freq_table, metadata = (
            scan_statistical(spss_file, dataset_id="ds")
        )
        assert row_count == 0
        assert actual_sample_size is None
        assert freq_table is None
        assert len(variables) == 1
        assert variables[0].name == "a"

    def test_sampling_when_rows_exceed_sample_size(self, tmp_path: Path):
        """scan_statistical should sample when row_count > sample_size."""
        df = pd.DataFrame({"x": list(range(100)), "y": [float(i) for i in range(100)]})
        spss_file = tmp_path / "big.sav"
        pyreadstat.write_sav(df, spss_file)

        variables, row_count, actual_sample_size, freq_table, metadata = (
            scan_statistical(spss_file, dataset_id="ds", sample_size=10)
        )
        assert row_count == 100
        assert actual_sample_size is not None
        assert actual_sample_size <= 100
        assert len(variables) == 2

    def test_pipeline_error_returns_empty(self, tmp_path: Path, capsys):
        """scan_statistical should catch exceptions from the Parquet pipeline."""
        df = pd.DataFrame({"a": [1, 2]})
        spss_file = tmp_path / "ok.sav"
        pyreadstat.write_sav(df, spss_file)

        with patch(
            "datannurpy.scanner.statistical._build_from_parquet",
            side_effect=RuntimeError("boom"),
        ):
            variables, row_count, _, _, _ = scan_statistical(
                spss_file, dataset_id="ds", quiet=False
            )
        captured = capsys.readouterr()
        assert "✗ ok.sav" in captured.err
        assert variables == []
        assert row_count == 0

    def test_string_only_file_no_fix_needed(self, tmp_path: Path):
        """_fix_parquet_types should return early when no float or time columns."""
        df = pd.DataFrame({"name": ["alice", "bob"], "city": ["paris", "lyon"]})
        spss_file = tmp_path / "strings.sav"
        pyreadstat.write_sav(df, spss_file)

        variables, row_count, _, _, _ = scan_statistical(spss_file, dataset_id="ds")
        assert row_count == 2
        assert all(v.type == "string" for v in variables)

    def test_float_column_not_all_int(self, tmp_path: Path):
        """_fix_parquet_types should keep float columns that have non-integer values."""
        df = pd.DataFrame({"val": [1.5, 2.7, 3.1]})
        spss_file = tmp_path / "floats.sav"
        pyreadstat.write_sav(df, spss_file)

        variables, _, _, _, _ = scan_statistical(spss_file, dataset_id="ds")
        var = variables[0]
        assert var.type == "float"

    def test_apply_labels_skips_none_labels(self, tmp_path: Path):
        """scan_statistical should not set description when label is None."""
        df = pd.DataFrame({"col_a": [1, 2]})
        spss_file = tmp_path / "no_label.sav"
        pyreadstat.write_sav(df, spss_file)

        variables, _, _, _, _ = scan_statistical(
            spss_file, dataset_id="ds", infer_stats=False
        )
        assert len(variables) == 1
        assert variables[0].description is None

    def test_schema_mode_sets_variable_types(self, tmp_path: Path):
        """scan_statistical with infer_stats=False should set variable types from readstat metadata."""
        df = pd.DataFrame({"num": [1.0, 2.0], "txt": ["a", "b"]})
        spss_file = tmp_path / "typed.sav"
        pyreadstat.write_sav(df, spss_file)

        variables, _, _, _, _ = scan_statistical(
            spss_file, dataset_id="ds", infer_stats=False
        )
        var_by_name = {v.name: v for v in variables}
        assert var_by_name["num"].type == "float"
        assert var_by_name["txt"].type == "string"

    def test_apply_types_unknown_type_passes_through(self):
        """_apply_types should pass through unknown readstat types as-is."""
        from datannurpy.scanner.statistical import _apply_types

        from datannurpy.schema import Variable

        var = Variable(id="ds---col", name="col", dataset_id="ds")
        _apply_types([var], {"col": "int32"})
        assert var.type == "int32"

    def test_apply_types_skips_missing_name(self):
        """_apply_types should skip variables not present in meta_types."""
        from datannurpy.scanner.statistical import _apply_types

        from datannurpy.schema import Variable

        var = Variable(id="ds---col", name="col", dataset_id="ds")
        _apply_types([var], {})
        assert var.type is None

    def test_read_statistical_mixed_types(self, tmp_path: Path):
        """convert_float_to_int should skip non-float columns."""
        df = pd.DataFrame({"name": ["alice", "bob"], "age": [30.0, 25.0]})
        spss_file = tmp_path / "mixed.sav"
        pyreadstat.write_sav(df, spss_file)

        result = read_statistical(spss_file)
        assert result is not None
        assert result["name"].dtype == object
        assert result["age"].dtype == "Int64"

    def test_time_column_without_float(self, tmp_path: Path):
        """_fix_parquet_types should handle time columns when no float columns exist."""
        import datetime

        df = pd.DataFrame(
            {
                "label": ["a", "b"],
                "t": [datetime.time(10, 30), datetime.time(14, 0)],
            }
        )
        spss_file = tmp_path / "time_only.sav"
        pyreadstat.write_sav(df, spss_file)

        variables, row_count, _, _, _ = scan_statistical(spss_file, dataset_id="ds")
        assert row_count == 2
        var_by_name = {v.name: v for v in variables}
        assert "label" in var_by_name
        assert "t" in var_by_name

    def test_fix_parquet_types_timetz_cast(self, tmp_path: Path):
        """_fix_parquet_types casts TIMETZ→TIME regardless of Python/DuckDB version."""
        import duckdb
        import ibis

        from datannurpy.scanner.statistical import _fix_parquet_types

        parquet_path = tmp_path / "timetz.parquet"
        dcon = duckdb.connect()
        dcon.execute(
            f"COPY (SELECT TIMETZ '10:30:00+00' AS t, 'a' AS label) "
            f"TO '{parquet_path}' (FORMAT PARQUET)"
        )
        dcon.close()

        con = ibis.duckdb.connect()
        table = con.read_parquet(str(parquet_path))
        result = _fix_parquet_types(con, table, parquet_path)
        assert result is not table
        assert result.schema()["t"].is_time()

    def test_multiple_chunks(self, tmp_path: Path):
        """_stat_to_parquet should write multiple chunks to Parquet."""
        from datannurpy.scanner.statistical import _stat_to_parquet

        df = pd.DataFrame({"x": list(range(10))})
        spss_file = tmp_path / "multi_chunk.sav"
        pyreadstat.write_sav(df, spss_file)

        with _stat_to_parquet(pyreadstat.read_sav, spss_file, chunksize=3) as pq_path:
            import pyarrow.parquet as pq_mod

            table = pq_mod.read_table(pq_path)
            assert len(table) == 10
