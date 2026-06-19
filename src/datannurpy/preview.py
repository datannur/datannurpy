"""Dataset preview collection and export helpers."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Union, cast

import ibis
import polars as pl
import pyarrow as pa
from jsonjsdb.writer import write_table_json, write_table_jsonjs

from .errors import ConfigError
from .utils.log import log_warn

if TYPE_CHECKING:
    import pandas as pd

    from .catalog import Catalog
    from .schema import Dataset

PreviewRows = Union[int, Literal[False], None]


def validate_preview_rows(value: PreviewRows, *, allow_none: bool) -> int | None:
    """Validate preview_rows and normalize false to 0."""
    if value is None:
        if allow_none:
            return None
        msg = "preview_rows cannot be None"
        raise ConfigError(msg)
    if value is False:
        return 0
    if value is True:
        msg = "preview_rows=true is ambiguous; use an explicit row count"
        raise ConfigError(msg)
    if not isinstance(value, int):
        msg = "preview_rows must be a non-negative integer or false"
        raise ConfigError(msg)
    if value < 0:
        msg = f"preview_rows must be >= 0, got {value}"
        raise ConfigError(msg)
    return value


def resolve_preview_rows(value: PreviewRows, default: int) -> int:
    """Resolve an add_* preview_rows override against catalog default."""
    resolved = validate_preview_rows(value, allow_none=True)
    return default if resolved is None else resolved


def effective_preview_rows(rows: int, depth: str) -> int:
    """Return the row limit that should be persisted for a dataset."""
    return rows if rows > 0 and depth in {"stat", "value"} else 0


def preview_from_arrow(
    table: pa.Table,
    preview_rows: int,
    *,
    label: str,
    quiet: bool,
) -> pl.DataFrame | None:
    """Build a preview from an Arrow table, logging and skipping on failure."""
    if preview_rows <= 0:
        return None
    try:
        df = cast(pl.DataFrame, pl.from_arrow(table.slice(0, preview_rows)))
        return normalize_preview_df(df)
    except Exception as exc:  # pragma: no cover - defensive per-source skip
        log_warn(f"{label}: preview skipped ({exc})", quiet)
        return None


def preview_from_ibis(
    table: ibis.Table,
    preview_rows: int,
    *,
    label: str,
    quiet: bool,
) -> pl.DataFrame | None:
    """Build a preview from an Ibis table using a bounded limit."""
    if preview_rows <= 0:
        return None
    try:
        limited = table.limit(preview_rows)
        try:
            arrow_table = limited.to_pyarrow()
        except Exception:
            result = limited.execute()
            if isinstance(result, pa.Table):
                arrow_table = result
            else:
                arrow_table = pa.Table.from_pandas(result, preserve_index=False)
        df = cast(pl.DataFrame, pl.from_arrow(arrow_table))
        return normalize_preview_df(df)
    except Exception as exc:  # pragma: no cover - backend-dependent
        log_warn(f"{label}: preview skipped ({exc})", quiet)
        return None


def preview_from_pandas(
    df: pd.DataFrame,
    preview_rows: int,
    *,
    label: str,
    quiet: bool,
) -> pl.DataFrame | None:
    """Build a preview from a pandas DataFrame."""
    if preview_rows <= 0:
        return None
    try:
        return normalize_preview_df(pl.from_pandas(df.head(preview_rows)))
    except Exception as exc:  # pragma: no cover - defensive per-source skip
        log_warn(f"{label}: preview skipped ({exc})", quiet)
        return None


def normalize_preview_df(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize preview values before passing them to jsonjsdb writers."""
    transforms: list[pl.Expr] = []
    for column_name, dtype in df.schema.items():
        expr = pl.col(column_name)
        if dtype.is_temporal() or isinstance(dtype, pl.Decimal):
            expr = expr.cast(pl.Utf8)
        elif dtype.is_float():
            expr = expr.fill_nan(None)
        elif dtype in (pl.Binary, pl.Object):
            expr = expr.map_elements(_json_safe_object, return_dtype=pl.Utf8)
        transforms.append(expr.alias(column_name))
    return df.select(transforms)


def _json_safe_object(value: Any) -> Any:
    """Convert object values that JSON cannot serialize safely."""
    if value is None:
        return None
    return str(value)


def remember_preview(
    catalog: Catalog,
    dataset_id: str,
    preview: pl.DataFrame | None,
    *,
    label: str,
) -> None:
    """Store runtime preview payload and log label on the catalog."""
    catalog._dataset_preview_labels[dataset_id] = label
    if preview is not None:
        catalog._dataset_previews[dataset_id] = preview


def sync_preview_exports(catalog: Catalog, output_dir: str | Path) -> set[str]:
    """Synchronize preview JSON/JSON-JS files for the final exported datasets."""
    output_path = Path(output_dir)
    preview_dir = output_path / "preview"
    datasets = list(catalog.dataset.all())
    try:
        preview_paths = list(preview_dir.iterdir())
        preview_dir_exists = True
    except FileNotFoundError:
        preview_paths = []
        preview_dir_exists = False
    existing_preview_ids = _existing_preview_ids_from_paths(preview_paths)
    if not preview_dir_exists and not any(
        (dataset.preview_rows or 0) > 0 for dataset in datasets
    ):
        return set()
    datasets_by_id = {dataset.id: dataset for dataset in datasets}
    eligible_ids = {
        dataset.id
        for dataset in datasets
        if (dataset.preview_rows or 0) > 0 or dataset.id in existing_preview_ids
    }

    remaining_existing_ids = (
        _remove_stale_preview_files(preview_paths, eligible_ids)
        if preview_dir_exists
        else set()
    )

    preview_ids: set[str] = set()
    for dataset_id in sorted(eligible_ids):
        preview = catalog._dataset_previews.get(dataset_id)
        if preview is not None:
            if not preview_dir_exists:
                preview_dir.mkdir(parents=True, exist_ok=True)
                preview_dir_exists = True
            dataset = datasets_by_id[dataset_id]
            try:
                write_table_json(preview, preview_dir / f"{dataset_id}.json")
                write_table_jsonjs(
                    preview, dataset_id, preview_dir / f"{dataset_id}.json.js"
                )
                preview_ids.add(dataset_id)
            except Exception as exc:  # pragma: no cover - filesystem/json edge cases
                label = _preview_label(catalog, dataset)
                log_warn(f"{label}: preview export skipped ({exc})", catalog.quiet)
                (preview_dir / f"{dataset_id}.json").unlink(missing_ok=True)
                (preview_dir / f"{dataset_id}.json.js").unlink(missing_ok=True)
        elif dataset_id in remaining_existing_ids:
            preview_ids.add(dataset_id)

    if preview_dir_exists and not preview_ids:
        preview_dir.rmdir()
    return preview_ids


def apply_preview_flags(catalog: Catalog, preview_ids: set[str]) -> None:
    """Mark datasets that have synchronized preview files."""
    df = catalog.dataset.df
    if df.is_empty() or "id" not in df.columns:
        return
    # Batch the two outcomes instead of an O(N) rebuild per dataset.
    all_ids = df["id"].to_list()
    with_preview = [i for i in all_ids if i in preview_ids]
    without_preview = [i for i in all_ids if i not in preview_ids]
    if with_preview:
        catalog.dataset.update_many(with_preview, has_preview=1)
    if without_preview:
        catalog.dataset.update_many(without_preview, has_preview=None)


def _existing_preview_ids_from_paths(paths: list[Path]) -> set[str]:
    """Return dataset ids that have both preview export formats from paths."""
    json_ids: set[str] = set()
    jsonjs_ids: set[str] = set()
    for path in paths:
        if path.is_dir():
            continue
        name = path.name
        if name.endswith(".json.js"):
            jsonjs_ids.add(name[: -len(".json.js")])
        elif name.endswith(".json"):
            json_ids.add(name[: -len(".json")])
    return json_ids & jsonjs_ids


def _preview_label(catalog: Catalog, dataset: Dataset) -> str:
    """Return the preferred label for preview warnings."""
    return (
        catalog._dataset_preview_labels.get(dataset.id)
        or dataset.data_path
        or dataset.name
        or dataset.id
    )


def _remove_stale_preview_files(paths: list[Path], eligible_ids: set[str]) -> set[str]:
    """Remove stale preview files and return dataset ids still present."""
    remaining_json_ids: set[str] = set()
    remaining_jsonjs_ids: set[str] = set()
    for path in paths:
        if path.is_dir():
            shutil.rmtree(path)
            continue
        dataset_id = _dataset_id_from_preview_file(path)
        if dataset_id is None or dataset_id not in eligible_ids:
            path.unlink(missing_ok=True)
        elif path.name.endswith(".json.js"):
            remaining_jsonjs_ids.add(dataset_id)
        else:
            remaining_json_ids.add(dataset_id)
    return remaining_json_ids & remaining_jsonjs_ids


def _dataset_id_from_preview_file(path: Path) -> str | None:
    """Extract dataset id from a preview .json or .json.js filename."""
    name = path.name
    if name.endswith(".json.js"):
        return name[: -len(".json.js")]
    if name.endswith(".json"):
        return name[: -len(".json")]
    return None
