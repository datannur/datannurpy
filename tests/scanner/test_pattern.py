"""Tests for pattern frequency analysis."""

from __future__ import annotations

import ibis

from datannurpy.scanner.pattern import (
    _build_pattern_expr,
    _classify_string,
    _prepare_table,
    compute_pattern_freqs,
)


class TestBuildPatternExpr:
    """Test _build_pattern_expr transformation."""

    def test_phone_number(self):
        t = ibis.memtable({"v": ["022 832 55 33"]})
        r = t.select(_build_pattern_expr(t.v).name("p")).to_pyarrow().to_pylist()
        assert r[0]["p"] == "999 999 99 99"

    def test_code_with_letters_and_digits(self):
        t = ibis.memtable({"v": ["GE-1234"]})
        r = t.select(_build_pattern_expr(t.v).name("p")).to_pyarrow().to_pylist()
        assert r[0]["p"] == "aa-9999"

    def test_email(self):
        t = ibis.memtable({"v": ["john.doe@gmail.com"]})
        r = t.select(_build_pattern_expr(t.v).name("p")).to_pyarrow().to_pylist()
        assert r[0]["p"] == "aaaa.aaa@aaaaa.aaa"

    def test_name_with_space(self):
        t = ibis.memtable({"v": ["Jean Dupont"]})
        r = t.select(_build_pattern_expr(t.v).name("p")).to_pyarrow().to_pylist()
        assert r[0]["p"] == "aaaa aaaaaa"

    def test_unicode_letters(self):
        t = ibis.memtable({"v": ["café", "Zürich", "日本語"]})
        r = t.select(_build_pattern_expr(t.v).name("p")).to_pyarrow().to_pylist()
        assert r[0]["p"] == "aaaa"
        assert r[1]["p"] == "aaaaaa"
        assert r[2]["p"] == "aaa"

    def test_preserved_separators(self):
        t = ibis.memtable({"v": ["a/b_c-d.e @f"]})
        r = t.select(_build_pattern_expr(t.v).name("p")).to_pyarrow().to_pylist()
        assert r[0]["p"] == "a/a_a-a.a @a"

    def test_unknown_chars_become_question_mark(self):
        t = ibis.memtable({"v": ["#$%&"]})
        r = t.select(_build_pattern_expr(t.v).name("p")).to_pyarrow().to_pylist()
        assert r[0]["p"] == "????"

    def test_ascii_fallback(self):
        t = ibis.memtable({"v": ["café"]})
        from datannurpy.scanner.pattern import _LETTER_ASCII

        r = (
            t.select(_build_pattern_expr(t.v, letter_re=_LETTER_ASCII).name("p"))
            .to_pyarrow()
            .to_pylist()
        )
        assert r[0]["p"] == "aaa?"


class TestClassifyString:
    """Test _classify_string classification logic."""

    def test_structured_single_dominant(self):
        assert _classify_string([600, 100, 50], 1000) == "structured"

    def test_structured_at_boundary(self):
        assert _classify_string([500], 1000) == "structured"

    def test_semi_structured(self):
        assert _classify_string([200, 200, 200, 50], 1000) == "semi_structured"

    def test_semi_structured_top3_at_boundary(self):
        assert _classify_string([200, 150, 150], 1000) == "semi_structured"

    def test_free_text(self):
        assert _classify_string([50, 40, 30, 20], 1000) == "free_text"

    def test_empty_freqs(self):
        assert _classify_string([], 100) == "free_text"

    def test_zero_total(self):
        assert _classify_string([10], 0) == "free_text"


class TestPrepareTable:
    """Test _prepare_table probe + materialization."""

    def test_duckdb_returns_same_table_with_unicode(self):
        t = ibis.memtable({"a": ["hello"], "b": [1]})
        from datannurpy.scanner.pattern import _LETTER_UNICODE

        result, letter_re = _prepare_table(t, ["a"])
        assert result is t
        assert letter_re == _LETTER_UNICODE

    def test_ascii_fallback_when_unicode_unsupported(self, monkeypatch):
        from datannurpy.scanner.pattern import _LETTER_ASCII

        t = ibis.memtable({"a": ["hello"]})
        orig_select = ibis.expr.types.relations.Table.select
        call_num = {"n": 0}

        def selective_select(self, *args, **kwargs):
            result = orig_select(self, *args, **kwargs)
            call_num["n"] += 1
            if call_num["n"] == 1:
                # First call (Unicode probe) fails
                raise Exception("unsupported Unicode regex")
            return result

        monkeypatch.setattr("ibis.expr.types.relations.Table.select", selective_select)
        result, letter_re = _prepare_table(t, ["a"])
        assert result is t
        assert letter_re == _LETTER_ASCII

    def test_materializes_when_no_regex(self, monkeypatch):
        t = ibis.memtable({"a": ["hello", "world"], "b": [1, 2]})
        orig_select = ibis.expr.types.relations.Table.select

        def failing_select(self, *args, **kwargs):
            # Check if this is a probe call (has _t in the args)
            str_args = str(args)
            if "_t" in str_args:
                raise Exception("no regex support")
            return orig_select(self, *args, **kwargs)

        monkeypatch.setattr("ibis.expr.types.relations.Table.select", failing_select)
        result, letter_re = _prepare_table(t, ["a"])
        assert result is not t
        assert list(result.columns) == ["a"]
        assert result.count().execute() == 2


class TestComputePatternFreqs:
    """Test compute_pattern_freqs end-to-end."""

    def test_empty_cols_returns_none(self):
        t = ibis.memtable({"x": [1]})
        freq_table, classes = compute_pattern_freqs(t, [])
        assert freq_table is None
        assert classes == {}

    def test_structured_phone_column(self):
        values = ["022 832 55 33"] * 40 + ["044 123 45 67"] * 30 + [None] * 5
        t = ibis.memtable({"phone": values})
        freq_table, classes = compute_pattern_freqs(t, ["phone"])

        assert classes["phone"] == "structured"
        assert freq_table is not None
        rows = freq_table.to_pylist()
        assert all(r["variable_id"] == "phone" for r in rows)
        assert rows[0]["value"] == "999 999 99 99"
        assert rows[0]["freq"] == 70

    def test_semi_structured_mixed_codes(self):
        values = (
            ["AB-1234"] * 30
            + ["X-99"] * 25
            + ["CODE5678"] * 20
            + [f"misc{i}" for i in range(200)]
        )
        t = ibis.memtable({"code": values})
        freq_table, classes = compute_pattern_freqs(t, ["code"])
        assert classes["code"] == "semi_structured"

    def test_free_text_column(self):
        values = ["x" * i for i in range(1, 201)]
        t = ibis.memtable({"desc": values})
        freq_table, classes = compute_pattern_freqs(t, ["desc"])
        assert classes["desc"] == "free_text"

    def test_all_null_column(self):
        t = ibis.memtable({"val": [None, None, None]}).cast({"val": "string"})
        freq_table, classes = compute_pattern_freqs(t, ["val"])
        assert classes["val"] == "free_text"
        assert freq_table is None

    def test_top_n_limit(self):
        values = [f"type{chr(65 + i)}-{i:04d}" for i in range(20)] * 5
        t = ibis.memtable({"code": values})
        freq_table, _ = compute_pattern_freqs(t, ["code"], top_n=3)
        assert freq_table is not None
        assert len(freq_table) <= 3

    def test_multiple_columns(self):
        t = ibis.memtable(
            {
                "phone": ["022 832 55 33"] * 50,
                "email": ["a@b.com"] * 50,
            }
        )
        freq_table, classes = compute_pattern_freqs(t, ["phone", "email"])
        assert "phone" in classes
        assert "email" in classes
        assert freq_table is not None
        var_ids = set(freq_table.column("variable_id").to_pylist())
        assert var_ids == {"phone", "email"}

    def test_freq_table_schema(self):
        t = ibis.memtable({"code": ["AB-1234"] * 10})
        freq_table, _ = compute_pattern_freqs(t, ["code"])
        assert freq_table is not None
        assert set(freq_table.column_names) == {"variable_id", "value", "freq"}
