"""Ensure dataclass-defined columns exist on Polars DataFrames."""

from __future__ import annotations

from dataclasses import MISSING, fields
from typing import Any

import polars as pl

_DTYPES: dict[str, Any] = {
    "int": pl.Int64,
    "float": pl.Float64,
    "bool": pl.Boolean,
}


def ensure_schema_columns(
    df: pl.DataFrame, entity_type: type | None, *, skip: set[str] | None = None
) -> pl.DataFrame:
    """Add any dataclass-defined columns missing from df."""
    if df.is_empty() or entity_type is None:
        return df

    skip = skip or set()
    new_cols: list[pl.Series] = []
    for f in fields(entity_type):
        if f.name in df.columns or f.name in skip:
            continue
        type_str = str(f.type)
        if type_str.startswith("list["):
            values: list[Any] = [[] for _ in range(df.height)]
            dtype: Any = pl.List(pl.Utf8)
        else:
            default = f.default if f.default is not MISSING else None
            values = [default] * df.height
            dtype = next((d for k, d in _DTYPES.items() if k in type_str), pl.Utf8)
        new_cols.append(pl.Series(f.name, values, dtype=dtype))

    if not new_cols:
        return df
    return df.with_columns(new_cols)
