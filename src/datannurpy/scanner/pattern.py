"""Pattern frequency analysis for high-cardinality string columns."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import ibis

if TYPE_CHECKING:
    import pyarrow as pa

_LETTER_UNICODE = r"[\p{L}]"


def _build_pattern_expr(col: Any) -> Any:
    """Transform a string expression into its abstract pattern form."""
    return (
        col.cast("string")
        .re_replace(r"\x00", "")
        .re_replace(_LETTER_UNICODE, "a")
        .re_replace(r"[0-9]", "9")
        .re_replace(r"[^a9@._/ -]", "?")
    )


def _classify_string(top_freqs: list[int], total: int) -> str:
    """Classify a string column based on pattern frequency distribution."""
    if total == 0 or not top_freqs:
        return "auto---free-text"
    if top_freqs[0] / total >= 0.50:
        return "auto---structured"
    if sum(top_freqs[:3]) / total >= 0.50:
        return "auto---semi-structured"
    return "auto---free-text"


def _prepare_table(table: ibis.Table, cols: list[str]) -> ibis.Table:
    """Ensure DuckDB-compatible regex support, materializing if needed."""
    try:
        test_expr: Any = ibis.literal("é")
        result = (
            table.select(test_expr.re_replace(_LETTER_UNICODE, "a").name("_t"))
            .limit(1)
            .to_pyarrow()
        )
        # Validate the probe output: some backends (e.g. Oracle) accept \p{L}
        # syntactically but only treat ASCII letters as letters, leaving "é"
        # unchanged. Fall back to DuckDB materialization in that case.
        if result.num_rows > 0 and result.column("_t")[0].as_py() == "a":
            return table
    except Exception:
        pass
    arrow = table.select(*cols).to_pyarrow()
    return ibis.memtable(arrow)


def compute_pattern_freqs(
    table: ibis.Table,
    pattern_cols: list[str],
    top_n: int = 10,
) -> tuple[pa.Table | None, dict[str, str]]:
    """Compute pattern frequencies for high-cardinality string columns."""
    if not pattern_cols:
        return None, {}

    import pyarrow as pa

    # Ensure DuckDB-compatible regex; materializes only if backend lacks \p{L}
    table = _prepare_table(table, pattern_cols)

    # Batch non-null counts: 1 query instead of N
    count_aggs: list[Any] = [
        table[col].count().name(f"{col}__count") for col in pattern_cols
    ]
    counts_row: dict[str, Any] = table.aggregate(count_aggs).to_pyarrow().to_pylist()[0]

    freq_tables: list[pa.Table] = []
    string_classes: dict[str, str] = {}

    for col in pattern_cols:
        total: int = int(counts_row[f"{col}__count"])
        if total == 0:
            string_classes[col] = "auto---free-text"
            continue

        non_null = table.filter(table[col].notnull())
        pattern_expr = _build_pattern_expr(non_null[col])
        patterned = non_null.select(pattern_expr.name("pattern"))
        grouped = patterned.group_by("pattern").agg(frequency=patterned.count())
        order_key: Any = ibis.desc("frequency")
        top = grouped.order_by(order_key).limit(top_n).to_pyarrow()

        top_rows = top.to_pylist()
        top_freqs = [r["frequency"] for r in top_rows]
        string_classes[col] = _classify_string(top_freqs, total)

        freq_tables.append(
            pa.table(
                {
                    "variable_id": pa.array([col] * len(top_rows), type=pa.string()),
                    "value": top.column("pattern"),
                    "frequency": top.column("frequency"),
                }
            )
        )

    freq_table = pa.concat_tables(freq_tables) if freq_tables else None
    return freq_table, string_classes
