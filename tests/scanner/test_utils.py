"""Tests for scanner utility functions."""

from __future__ import annotations

from unittest.mock import MagicMock

import ibis
import ibis.expr.datatypes as dt
from ibis.expr.datatypes.core import IntervalUnit
import pytest

from datannurpy.scanner.utils import build_variables, ibis_type_to_str


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
