"""Shared helpers for adding scanned datasets incrementally.

Used by both the file scanner (``add_dataset``) and the File Geodatabase scanner
(``add_geodatabase``): skip a dataset whose source is unchanged, and persist a
freshly scanned dataset together with its variables, enumerations and preview.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .finalize import remove_dataset_cascade
from .preview import remember_preview
from .utils import build_variable_ids, iso_to_timestamp, log_skip

if TYPE_CHECKING:
    from .catalog import Catalog
    from .schema import Dataset, Variable


def skip_unchanged(
    catalog: Catalog,
    match_path: str,
    data_path: str,
    current_mtime: int,
    *,
    refresh: bool,
    preview_rows: int,
    quiet: bool,
    label: str,
) -> bool:
    """Skip-and-mark an unchanged dataset, or cascade-remove a stale one.

    Returns True when the existing dataset is unchanged (caller should skip it).
    """
    existing = catalog.dataset.get_by("_match_path", match_path) or (
        catalog.dataset.get_by("_match_path", data_path)
    )
    if existing is None:
        return False
    if not refresh and iso_to_timestamp(existing.last_update_date) == current_mtime:
        catalog.dataset.update(
            existing.id, _seen=True, _match_path=match_path, preview_rows=preview_rows
        )
        catalog.enumeration_manager.mark_dataset_seen(existing.id)
        log_skip(label, quiet)
        return True
    remove_dataset_cascade(catalog, existing)
    return False


def finalize_scanned_dataset(
    catalog: Catalog,
    dataset: Dataset,
    *,
    variables: list[Variable],
    freq_table: Any,
    preview: Any,
    label: str,
    auto_enumerations: bool,
) -> None:
    """Add a scanned dataset together with its variables, enumerations and preview."""
    catalog.dataset.add(dataset)
    remember_preview(catalog, dataset.id, preview, label=label)
    var_id_mapping = build_variable_ids(variables, dataset.id)
    if freq_table is not None:
        catalog.enumeration_manager.assign_from_freq(
            variables, freq_table, var_id_mapping, auto_enumerations=auto_enumerations
        )
    catalog.variable.add_all(variables)
