"""Tests for scanner utility functions."""

from __future__ import annotations

from unittest.mock import MagicMock

import ibis
import ibis.expr.datatypes as dt
from ibis.expr.datatypes.core import IntervalUnit
import pytest

from datannurpy.scanner.utils import (
    build_variables,
    get_dir_data_size,
    ibis_type_to_str,
    _to_float,
    _round6,
)


class TestToFloat:
    """Test _to_float helper."""

    def test_none(self):
        assert _to_float(None) is None

    def test_value(self):
        assert _to_float(42) == 42.0


class TestRound6:
    """Test _round6 helper."""

    def test_none(self):
        assert _round6(None) is None

    def test_value(self):
        assert _round6(3.14159265) == 3.141593


class TestIbisTypeToStr:
    """Test ibis_type_to_str function."""

    def test_unsigned_integer(self):
        """Unsigned integers should map to 'integer'."""
        assert ibis_type_to_str(dt.UInt8()) == "integer"
        assert ibis_type_to_str(dt.UInt64()) == "integer"

    def test_boolean(self):
        """Boolean should map to 'boolean'."""
        assert ibis_type_to_str(dt.Boolean()) == "boolean"

    def test_interval(self):
        """Interval should map to 'duration'."""
        assert ibis_type_to_str(dt.Interval(unit=IntervalUnit.SECOND)) == "duration"

    def test_null(self):
        """Null should map to 'null'."""
        assert ibis_type_to_str(dt.Null()) == "null"

    def test_geospatial(self):
        """GeoSpatial types should map to 'geometry'."""
        assert ibis_type_to_str(dt.GeoSpatial()) == "geometry"
        assert ibis_type_to_str(dt.Point()) == "geometry"
        assert ibis_type_to_str(dt.Polygon()) == "geometry"

    def test_unknown_geometry(self):
        """Unknown types with geometry raw_type should map to 'geometry'."""
        mock_raw = MagicMock(**{"__str__.return_value": "point"})
        assert ibis_type_to_str(dt.Unknown(raw_type=mock_raw)) == "geometry"

    def test_unknown_non_geometry(self):
        """Unknown types without geometry raw_type should stay 'unknown'."""
        mock_raw = MagicMock(**{"__str__.return_value": "sometype"})
        assert ibis_type_to_str(dt.Unknown(raw_type=mock_raw)) == "unknown"


class TestBuildVariables:
    """Test build_variables function."""

    def test_all_columns_skipped(self):
        """build_variables should handle tables where all columns are skipped."""
        table = ibis.memtable({"blob": [b"data"]})
        variables, freq_table = build_variables(
            table,
            nb_rows=1,
            dataset_id="test",
            infer_stats=True,
            skip_stats_columns={"blob"},
        )
        assert len(variables) == 1
        assert variables[0].nb_distinct is None  # No stats computed

    def test_oracle_clob_error_handled(self, monkeypatch):
        """build_variables should handle Oracle ORA-22849 error gracefully."""
        table = ibis.memtable({"col": ["a", "b"]})

        def mock_aggregate(*args, **kwargs):
            raise Exception("ORA-22849: cannot use CLOB in COUNT DISTINCT")

        monkeypatch.setattr(
            "ibis.expr.types.relations.Table.aggregate",
            MagicMock(side_effect=mock_aggregate),
        )
        variables, freq_table = build_variables(
            table, nb_rows=2, dataset_id="test", infer_stats=True
        )

        assert len(variables) == 1
        assert variables[0].nb_distinct is None  # Stats skipped due to error

    def test_other_exception_reraised(self, monkeypatch):
        """build_variables should reraise non-Oracle exceptions."""
        table = ibis.memtable({"col": ["a", "b"]})

        def mock_aggregate(*args, **kwargs):
            raise ValueError("Some other error")

        monkeypatch.setattr(
            "ibis.expr.types.relations.Table.aggregate",
            MagicMock(side_effect=mock_aggregate),
        )

        with pytest.raises(ValueError, match="Some other error"):
            build_variables(table, nb_rows=2, dataset_id="test", infer_stats=True)

    def test_numeric_extra_stats(self):
        """build_variables should compute min/max/mean/std for numeric columns."""
        table = ibis.memtable({"val": [10, 20, 30, 40, 50]})
        variables, _ = build_variables(
            table, nb_rows=5, dataset_id="test", infer_stats=True
        )
        v = variables[0]
        assert v.min == pytest.approx(10.0)
        assert v.max == pytest.approx(50.0)
        assert v.mean == pytest.approx(30.0)
        assert v.std is not None

    def test_float_extra_stats(self):
        """build_variables should compute min/max/mean/std for float columns."""
        table = ibis.memtable({"val": [1.5, 2.5, 3.5]})
        variables, _ = build_variables(
            table, nb_rows=3, dataset_id="test", infer_stats=True
        )
        v = variables[0]
        assert v.min == pytest.approx(1.5)
        assert v.max == pytest.approx(3.5)
        assert v.mean == pytest.approx(2.5)

    def test_string_extra_stats_on_length(self):
        """build_variables should compute min/max/mean/std on string length."""
        table = ibis.memtable({"name": ["ab", "abcd", "abcdef"]})
        variables, _ = build_variables(
            table, nb_rows=3, dataset_id="test", infer_stats=True
        )
        v = variables[0]
        assert v.min == pytest.approx(2.0)
        assert v.max == pytest.approx(6.0)
        assert v.mean == pytest.approx(4.0)

    def test_extra_stats_with_nulls(self):
        """build_variables should exclude nulls from min/max/mean/std."""
        table = ibis.memtable({"val": [10, None, 30, None, 50]})
        variables, _ = build_variables(
            table, nb_rows=5, dataset_id="test", infer_stats=True
        )
        v = variables[0]
        assert v.min == pytest.approx(10.0)
        assert v.max == pytest.approx(50.0)
        assert v.mean == pytest.approx(30.0)
        assert v.nb_missing == 2

    def test_extra_stats_none_when_no_infer(self):
        """Extra stats should be None when infer_stats=False."""
        table = ibis.memtable({"val": [10, 20, 30]})
        variables, _ = build_variables(
            table, nb_rows=3, dataset_id="test", infer_stats=False
        )
        v = variables[0]
        assert v.min is None
        assert v.max is None
        assert v.mean is None
        assert v.std is None

    def test_extra_stats_none_for_boolean(self):
        """Boolean columns should not have extra stats."""
        table = ibis.memtable({"flag": [True, False, True]})
        variables, _ = build_variables(
            table, nb_rows=3, dataset_id="test", infer_stats=True
        )
        v = variables[0]
        assert v.min is None
        assert v.max is None

    def test_date_extra_stats(self):
        """build_variables should compute min/max/mean/std on date columns as epoch seconds."""
        import datetime

        table = ibis.memtable(
            {
                "dt": [
                    datetime.date(2020, 1, 1),
                    datetime.date(2020, 7, 1),
                    datetime.date(2021, 1, 1),
                ]
            }
        )
        variables, _ = build_variables(
            table, nb_rows=3, dataset_id="test", infer_stats=True
        )
        v = variables[0]
        assert v.min is not None
        assert v.max is not None
        assert v.mean is not None
        assert v.std is not None
        assert v.min < v.max

    def test_single_row_skips_std(self):
        """build_variables should skip std when table has only 1 row."""
        import datetime

        table = ibis.memtable({"dt": [datetime.date(2020, 1, 1)], "val": [42]})
        variables, _ = build_variables(
            table, nb_rows=1, dataset_id="test", infer_stats=True
        )
        for v in variables:
            assert v.std is None
            assert v.min is not None

    def test_arrow_invalid_fallback(self):
        """build_variables falls back to execute() when to_pyarrow() raises ArrowInvalid."""
        from unittest.mock import patch

        import pyarrow as pa

        table = ibis.memtable({"val": [1, 2, 3]})
        orig_to_pyarrow = type(table.aggregate([])).to_pyarrow

        call_count = 0

        def failing_to_pyarrow(self_expr, **kw):  # type: ignore[no-untyped-def]
            nonlocal call_count
            call_count += 1
            # Fail on the first call (stats aggregation)
            if call_count == 1:
                raise pa.ArrowInvalid("Could not convert Decimal('1')")
            return orig_to_pyarrow(self_expr, **kw)

        with patch.object(type(table.aggregate([])), "to_pyarrow", failing_to_pyarrow):
            variables, _ = build_variables(
                table, nb_rows=3, dataset_id="test", infer_stats=True
            )
        v = variables[0]
        assert v.min is not None
        assert v.max is not None

    def test_single_distinct_value_no_nan(self):
        """std should be None when all values are identical (single distinct)."""
        table = ibis.memtable({"val": [42, 42, 42]})
        variables, _ = build_variables(
            table, nb_rows=3, dataset_id="test", infer_stats=True
        )
        v = variables[0]
        assert v.min == pytest.approx(42.0)
        assert v.max == pytest.approx(42.0)
        assert v.std is None

    def test_full_table_sampling_mode(self):
        """build_variables with full_table splits streaming and cardinality queries."""
        full = ibis.memtable(
            {"val": [10, 20, 30, 40, 50], "flag": [True, False, True, False, True]}
        )
        sample = ibis.memtable({"val": [10, 30], "flag": [True, True]})
        variables, _ = build_variables(
            sample,
            nb_rows=2,
            dataset_id="test",
            infer_stats=True,
            full_table=full,
            full_nb_rows=5,
        )
        var_by_name = {v.name: v for v in variables}
        # min/max/mean from full table
        assert var_by_name["val"].min == pytest.approx(10.0)
        assert var_by_name["val"].max == pytest.approx(50.0)
        assert var_by_name["val"].mean == pytest.approx(30.0)
        # nb_missing exact from full table
        assert var_by_name["val"].nb_missing == 0
        # nb_distinct from full table (approx_nunique)
        assert var_by_name["val"].nb_distinct == 5
        # boolean column has no extra stats
        assert var_by_name["flag"].min is None


class TestGetDirDataSize:
    """Test get_dir_data_size with remote filesystem."""

    def test_remote_fs(self):
        fs = MagicMock()
        fs.glob.side_effect = [
            ["bucket/dir/a.parquet", "bucket/dir/sub/b.parquet"],
            [],
        ]
        fs.info.side_effect = [
            {"size": 1000},
            {"size": 2000},
        ]
        from pathlib import PurePosixPath

        result = get_dir_data_size(PurePosixPath("bucket/dir"), fs=fs)
        assert result == 3000
        fs.glob.assert_any_call("bucket/dir/**/*.parquet")
        fs.glob.assert_any_call("bucket/dir/**/*.pq")
