"""Tests for pattern frequency analysis."""

from __future__ import annotations

import ibis
import pyarrow as pa

from datannurpy.scanner.pattern import (
    _build_pattern_array,
    _classify_string,
    compute_pattern_freqs,
)


def _pattern(value: str) -> str:
    out = _build_pattern_array(pa.array([value], type=pa.string()))
    return out[0].as_py()


class TestBuildPatternArray:
    """Test _build_pattern_array transformation."""

    def test_phone_number(self):
        assert _pattern("022 832 55 33") == "999 999 99 99"

    def test_code_with_letters_and_digits(self):
        assert _pattern("GE-1234") == "aa-9999"

    def test_email(self):
        assert _pattern("john.doe@gmail.com") == "aaaa.aaa@aaaaa.aaa"

    def test_name_with_space(self):
        assert _pattern("Jean Dupont") == "aaaa aaaaaa"

    def test_unicode_letters(self):
        assert _pattern("café") == "aaaa"
        assert _pattern("Zürich") == "aaaaaa"
        assert _pattern("日本語") == "aaa"

    def test_preserved_separators(self):
        assert _pattern("a/b_c-d.e @f") == "a/a_a-a.a @a"

    def test_unknown_chars_become_question_mark(self):
        assert _pattern("#$%&") == "????"

    def test_accented_letters(self):
        assert _pattern("café") == "aaaa"

    def test_hyphen_preserved(self):
        assert _pattern("a-b") == "a-a"
        assert _pattern("1-2") == "9-9"
        assert _pattern("-") == "-"

    def test_null_bytes_stripped(self):
        assert _pattern("MARLY\x00\x00\x00\x00\x00") == "aaaaa"

    def test_cast_from_non_string(self):
        out = _build_pattern_array(pa.array([42, 7], type=pa.int64()))
        assert out.to_pylist() == ["99", "9"]


class TestClassifyString:
    """Test _classify_string classification logic."""

    def test_structured_single_dominant(self):
        assert _classify_string([600, 100, 50], 1000) == "auto---structured"

    def test_structured_at_boundary(self):
        assert _classify_string([500], 1000) == "auto---structured"

    def test_semi_structured(self):
        assert _classify_string([200, 200, 200, 50], 1000) == "auto---semi-structured"

    def test_semi_structured_top3_at_boundary(self):
        assert _classify_string([200, 150, 150], 1000) == "auto---semi-structured"

    def test_free_text(self):
        assert _classify_string([50, 40, 30, 20], 1000) == "auto---free-text"

    def test_empty_freqs(self):
        assert _classify_string([], 100) == "auto---free-text"

    def test_zero_total(self):
        assert _classify_string([10], 0) == "auto---free-text"


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

        assert classes["phone"] == "auto---structured"
        assert freq_table is not None
        rows = freq_table.to_pylist()
        assert all(r["variable_id"] == "phone" for r in rows)
        assert rows[0]["value"] == "999 999 99 99"
        assert rows[0]["frequency"] == 70

    def test_semi_structured_mixed_codes(self):
        values = (
            ["AB-1234"] * 30
            + ["X-99"] * 25
            + ["CODE5678"] * 20
            + [f"misc{i}" for i in range(200)]
        )
        t = ibis.memtable({"code": values})
        freq_table, classes = compute_pattern_freqs(t, ["code"])
        assert classes["code"] == "auto---semi-structured"
        assert freq_table is not None

    def test_free_text_column(self):
        values = ["x" * i for i in range(1, 201)]
        t = ibis.memtable({"desc": values})
        freq_table, classes = compute_pattern_freqs(t, ["desc"])
        assert classes["desc"] == "auto---free-text"
        assert freq_table is not None

    def test_all_null_column(self):
        t = ibis.memtable({"val": [None, None, None]}).cast({"val": "string"})
        freq_table, classes = compute_pattern_freqs(t, ["val"])
        assert classes["val"] == "auto---free-text"
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

    def test_frequency_table_schema(self):
        t = ibis.memtable({"code": ["AB-1234"] * 10})
        frequency_table, _ = compute_pattern_freqs(t, ["code"])
        assert frequency_table is not None
        assert set(frequency_table.column_names) == {
            "variable_id",
            "value",
            "frequency",
        }
