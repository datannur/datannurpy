"""Pattern frequency analysis for high-cardinality string columns."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.compute as pc

if TYPE_CHECKING:
    import ibis


def _build_pattern_array(arr: pa.Array) -> pa.Array:
    """Transform a string array into its abstract pattern form."""
    repl = pc.replace_substring_regex  # pyright: ignore[reportAttributeAccessIssue]
    out = arr.cast(pa.string())
    out = repl(out, r"\x00", "")
    out = repl(out, r"[\p{L}]", "a")
    out = repl(out, r"[0-9]", "9")
    out = repl(out, r"[^a9@._/ -]", "?")
    return out


def _classify_string(top_freqs: list[int], total: int) -> str:
    """Classify a string column based on pattern frequency distribution."""
    if total == 0 or not top_freqs:
        return "auto---free-text"
    if top_freqs[0] / total >= 0.50:
        return "auto---structured"
    if sum(top_freqs[:3]) / total >= 0.50:
        return "auto---semi-structured"
    return "auto---free-text"


def compute_pattern_freqs(
    table: ibis.Table,
    pattern_cols: list[str],
    top_n: int = 10,
) -> tuple[pa.Table | None, dict[str, str]]:
    """Compute pattern frequencies for high-cardinality string columns."""
    if not pattern_cols:
        return None, {}

    # Single materialization; PyArrow's RE2 supports \p{L} uniformly so no
    # backend probing is needed.
    arrow = table.select(*pattern_cols).to_pyarrow()

    freq_tables: list[pa.Table] = []
    string_classes: dict[str, str] = {}

    for col in pattern_cols:
        non_null = arrow.column(col).combine_chunks().drop_null()
        total = len(non_null)
        if total == 0:
            string_classes[col] = "auto---free-text"
            continue

        patterned = _build_pattern_array(non_null)
        vc = patterned.value_counts()
        values = vc.field("values")
        counts = vc.field("counts")
        order = pc.sort_indices(  # pyright: ignore[reportAttributeAccessIssue]
            pa.table({"counts": counts}), sort_keys=[("counts", "descending")]
        )
        top_idx = order[:top_n]
        top_values = values.take(top_idx)
        top_counts = counts.take(top_idx)

        top_freqs = top_counts.to_pylist()
        string_classes[col] = _classify_string(top_freqs, total)

        n = len(top_freqs)
        freq_tables.append(
            pa.table(
                {
                    "variable_id": pa.array([col] * n, type=pa.string()),
                    "value": top_values.cast(pa.string()),
                    "frequency": top_counts.cast(pa.int64()),
                }
            )
        )

    freq_table = pa.concat_tables(freq_tables) if freq_tables else None
    return freq_table, string_classes
