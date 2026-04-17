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

    def test_binary(self):
        """Binary should map to 'binary'."""
        assert ibis_type_to_str(dt.Binary()) == "binary"

    def test_unknown_geometry(self):
        """Unknown types with geometry raw_type should map to 'geometry'."""
        mock_raw = MagicMock(**{"__str__.return_value": "point"})
        assert ibis_type_to_str(dt.Unknown(raw_type=mock_raw)) == "geometry"

    def test_unknown_non_geometry(self):
        """Unknown types without geometry raw_type should stay 'unknown'."""
        mock_raw = MagicMock(**{"__str__.return_value": "sometype"})
        assert ibis_type_to_str(dt.Unknown(raw_type=mock_raw)) == "unknown"

    def test_unknown_double(self):
        """Unknown types with raw_type 'double' should map to 'float'."""
        assert ibis_type_to_str(dt.Unknown(raw_type="double")) == "float"

    def test_unknown_double_with_precision(self):
        """Unknown types with raw_type 'double(17,6)' should map to 'float'."""
        assert ibis_type_to_str(dt.Unknown(raw_type="double(17,6)")) == "float"

    def test_unknown_udouble(self):
        """Unknown types with raw_type 'UDOUBLE(17, 6)' should map to 'float'."""
        assert ibis_type_to_str(dt.Unknown(raw_type="UDOUBLE(17, 6)")) == "float"

    def test_unknown_float(self):
        """Unknown types with raw_type 'float' should map to 'float'."""
        assert ibis_type_to_str(dt.Unknown(raw_type="float")) == "float"

    def test_unknown_integer_raw_types(self):
        """Unknown types with integer raw_types should map to 'integer'."""
        for raw in ("tinyint", "smallint", "mediumint", "int", "bigint"):
            assert ibis_type_to_str(dt.Unknown(raw_type=raw)) == "integer"

    def test_unknown_unsigned_integer_raw_types(self):
        """Unsigned integer raw_types should map to 'integer'."""
        for raw in ("utinyint", "usmallint", "umediumint", "uint", "ubigint"):
            assert ibis_type_to_str(dt.Unknown(raw_type=raw)) == "integer"

    def test_unmapped_type_returns_unknown(self):
        """Non-Unknown unmapped types should return 'unknown'."""
        assert ibis_type_to_str(dt.Array(value_type=dt.String())) == "unknown"


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

    def test_empty_string_treated_as_missing(self):
        """Empty strings should be treated as missing values."""
        table = ibis.memtable({"val": ["a", "", None, "b", ""]})
        variables, freq = build_variables(
            table, nb_rows=5, dataset_id="test", freq_threshold=100
        )
        v = variables[0]
        assert v.nb_missing == 3  # 1 NULL + 2 ""
        assert v.nb_distinct == 2  # "a", "b"
        assert freq is not None
        assert "" not in freq.column("value").to_pylist()

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

    def test_full_table_schema_used_for_types(self):
        """build_variables uses full_table schema for Variable.type, not sample memtable."""
        # full_table has int column → type="integer"
        full = ibis.memtable({"val": [1, 2, 3, 4, 5]})
        assert str(full.schema()["val"]) == "int64"
        # sample memtable has float64 (simulates degradation through Arrow round-trip)
        sample = ibis.memtable({"val": [1.0, 3.0]})
        assert str(sample.schema()["val"]) == "float64"
        variables, _ = build_variables(
            sample,
            nb_rows=2,
            dataset_id="test",
            infer_stats=True,
            full_table=full,
            full_nb_rows=5,
        )
        # Type must come from full_table (int64 → "integer"), not sample (float64 → "float")
        assert variables[0].type == "integer"

    def test_freq_union_fallback_on_error(self):
        """build_variables falls back to pa.concat_tables when ibis.union fails."""
        from unittest.mock import patch

        table = ibis.memtable({"cat": ["a", "b", "a"], "num": [1, 2, 3]})

        def failing_union(*args, **kwargs):
            raise Exception("Illegal mix of collations for operation 'UNION'")

        with patch("datannurpy.scanner.utils.ibis.union", side_effect=failing_union):
            variables, freq_table = build_variables(
                table,
                nb_rows=3,
                dataset_id="test",
                infer_stats=True,
                freq_threshold=10,
            )

        assert freq_table is not None
        assert len(freq_table) == 5  # 2 cat values + 3 num values

    def test_unknown_mappable_raw_type_not_skipped(self):
        """Unknown columns with mappable raw_type should not be skipped for stats."""
        table = ibis.memtable({"val": [1.5, 2.5, 3.5]})
        # Patch schema to report val as Unknown(raw_type="double")
        fake_schema = ibis.Schema.from_tuples([("val", dt.Unknown(raw_type="double"))])
        original_schema = type(table).schema

        def patched_schema(self):  # type: ignore[no-untyped-def]
            if self is table:
                return fake_schema
            return original_schema(self)

        from unittest.mock import patch

        with patch.object(type(table), "schema", patched_schema):
            variables, _ = build_variables(
                table, nb_rows=3, dataset_id="test", infer_stats=True
            )
        v = variables[0]
        assert v.type == "float"
        assert v.nb_distinct is not None


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


class TestPatternFreqIntegration:
    """Test pattern frequency integration in build_variables."""

    def test_high_cardinality_string_gets_pattern(self):
        """String column with nb_distinct > freq_threshold should get pattern freq."""
        values = [f"AB-{i:04d}" for i in range(50)]
        table = ibis.memtable({"code": values, "num": list(range(50))})
        variables, freq_table = build_variables(
            table, nb_rows=50, dataset_id="test", infer_stats=True, freq_threshold=10
        )
        var_by_name = {v.name: v for v in variables}
        assert var_by_name["code"].is_pattern is True
        assert "auto---structured" in var_by_name["code"].tag_ids
        # Integer column should not be a pattern
        assert var_by_name["num"].is_pattern is False
        assert var_by_name["num"].tag_ids == []
        # freq_table should contain pattern entries
        assert freq_table is not None
        code_freqs = [r for r in freq_table.to_pylist() if r["variable_id"] == "code"]
        assert len(code_freqs) > 0
        assert code_freqs[0]["value"] == "aa-9999"

    def test_low_cardinality_string_no_pattern(self):
        """String column with nb_distinct <= freq_threshold should not get pattern."""
        table = ibis.memtable({"cat": ["a", "b", "c", "a", "b"]})
        variables, freq_table = build_variables(
            table, nb_rows=5, dataset_id="test", infer_stats=True, freq_threshold=10
        )
        v = variables[0]
        assert v.is_pattern is False
        assert v.tag_ids == []
        assert freq_table is not None

    def test_no_freq_threshold_no_pattern(self):
        """Without freq_threshold, no pattern should be computed."""
        values = [f"AB-{i:04d}" for i in range(50)]
        table = ibis.memtable({"code": values})
        variables, freq_table = build_variables(
            table, nb_rows=50, dataset_id="test", infer_stats=True
        )
        assert variables[0].is_pattern is False
        assert variables[0].tag_ids == []

    def test_mixed_columns_freq_and_pattern(self):
        """Low-card and high-card string columns produce combined freq_table."""
        low = ["a", "b"] * 25
        high = [f"X-{i:04d}" for i in range(50)]
        table = ibis.memtable({"low": low, "high": high})
        variables, freq_table = build_variables(
            table, nb_rows=50, dataset_id="test", infer_stats=True, freq_threshold=5
        )
        var_by_name = {v.name: v for v in variables}
        assert var_by_name["low"].is_pattern is False
        assert var_by_name["high"].is_pattern is True
        # Both should have entries in freq_table
        assert freq_table is not None
        var_ids = set(r["variable_id"] for r in freq_table.to_pylist())
        assert "low" in var_ids
        assert "high" in var_ids

    def test_free_text_classification(self):
        """Diverse strings with many patterns should get free_text classification."""
        import random

        rng = random.Random(42)
        chars = "abcdefghijklmnopqrstuvwxyz0123456789 -_."
        values = ["".join(rng.choices(chars, k=rng.randint(5, 30))) for _ in range(200)]
        table = ibis.memtable({"desc": values})
        variables, _ = build_variables(
            table, nb_rows=200, dataset_id="test", infer_stats=True, freq_threshold=10
        )
        assert variables[0].is_pattern is True
        assert "auto---free-text" in variables[0].tag_ids

    def test_pattern_works_with_non_regex_backend(self):
        """Pattern computation should materialize to memtable if backend lacks regex."""
        from unittest.mock import patch

        values = [f"AB-{i:04d}" for i in range(50)]
        table = ibis.memtable({"code": values})

        # Simulate a backend where _prepare_table materializes
        def fake_prepare(t, cols):
            arrow = t.select(*cols).to_pyarrow()
            return ibis.memtable(arrow)

        with patch(
            "datannurpy.scanner.pattern._prepare_table",
            side_effect=fake_prepare,
        ):
            variables, freq_table = build_variables(
                table,
                nb_rows=50,
                dataset_id="test",
                infer_stats=True,
                freq_threshold=10,
            )
        # Even with a "non-regex" backend, patterns should still be computed
        var = variables[0]
        assert var.is_pattern is True
        assert var.tag_ids != []
        assert freq_table is not None

    def test_security_column_uses_pattern_not_raw_freq(self):
        """Security-tagged columns should never expose raw values in freq."""
        hashes = [f"$2a$10$salt{i:040d}hashvalue" for i in range(10)]
        table = ibis.memtable({"password": hashes, "name": ["alice", "bob"] * 5})
        variables, freq_table = build_variables(
            table, nb_rows=10, dataset_id="test", infer_stats=True, freq_threshold=20
        )
        var_by_name = {v.name: v for v in variables}
        # password should be detected as bcrypt → pattern mode, no raw hashes
        assert "auto---bcrypt" in var_by_name["password"].tag_ids
        assert var_by_name["password"].is_pattern is True
        # freq_table should not contain any raw hash values
        assert freq_table is not None
        pw_freqs = [r for r in freq_table.to_pylist() if r["variable_id"] == "password"]
        for row in pw_freqs:
            assert not row["value"].startswith("$2a$")
