"""Shared helpers for adding scanned datasets incrementally.

Used by both the file scanner (``add_dataset``) and the File Geodatabase scanner
(``add_geodatabase``): skip a dataset whose source is unchanged, and persist a
freshly scanned dataset together with its variables, enumerations and preview.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .finalize import remove_dataset_cascade
from .preview import remember_preview
from .schema import Dataset
from .utils import build_variable_ids, error_count, iso_to_timestamp, log_skip
from .utils.version import is_stale_failure, scanner_version

if TYPE_CHECKING:
    from .catalog import Catalog
    from .schema import Variable


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
    current_signature: str | None = None,
) -> bool:
    """Skip-and-mark an unchanged dataset, or cascade-remove a stale one.

    Returns True when the existing dataset is unchanged (caller should skip it).
    """
    existing = catalog.dataset.get_by("_match_path", match_path) or (
        catalog.dataset.get_by("_match_path", data_path)
    )
    if existing is None:
        return False
    # Skip only when *every* freshness signal the source exposes is unchanged; re-scan
    # if any changed (a stale Last-Modified — 1s granularity — is caught by the ETag,
    # and vice versa). Signals: the modification time (mtime 0 = none, e.g. an HTTP
    # endpoint with no Last-Modified) and a content signature/ETag. A source exposing
    # neither can't be judged, so always re-scan rather than keep a stale scan.
    signals: list[bool] = []
    if current_mtime:
        signals.append(iso_to_timestamp(existing.last_update_date) == current_mtime)
    if current_signature is not None:
        signals.append(existing.schema_signature == current_signature)
    if is_stale_failure(existing.scan_failed_version):
        signals.append(False)
    if not refresh and signals and all(signals):
        catalog.dataset.update(
            existing.id, _seen=True, _match_path=match_path, preview_rows=preview_rows
        )
        catalog.enumeration_manager.mark_dataset_seen(existing.id)
        log_skip(label, quiet)
        return True
    remove_dataset_cascade(catalog, existing)
    return False


def scan_gdb_layer_dataset(
    catalog: Catalog,
    source: str,
    layer: str,
    *,
    dataset_id: str,
    folder_id: str | None,
    label: str,
    match_path: str,
    data_path: str,
    last_update: str | None,
    freq_threshold: int | None,
    preview_rows: int,
    auto_enumerations: bool,
    quiet: bool,
) -> tuple[int | None, int]:
    """Scan one File Geodatabase layer and persist its dataset; the shared core
    of ``add_geodatabase`` and the zipped-``.gdb`` folder-scan path (the callers
    own skip, per-layer logging and tallies). A ✗ logged during the scan stamps
    the dataset for the versioned retry. Returns ``(nb_row, nb_vars)``."""
    from .scanner.geo_vector import scan_geo_vector

    errors_before = error_count()
    variables, nb_row, freq_table, geo, preview = scan_geo_vector(
        source,
        dataset_id=dataset_id,
        layer=layer,
        freq_threshold=freq_threshold,
        preview_rows=preview_rows,
        return_preview=True,
        quiet=quiet,
        path_label=label,
    )
    layer_errors = min(1, error_count() - errors_before)
    geo = geo or {}
    dataset = Dataset(
        id=dataset_id,
        name=layer,
        folder_id=folder_id,
        data_path=data_path,
        last_update_date=last_update,
        delivery_format="geodatabase",
        nb_row=nb_row,
        preview_rows=preview_rows,
        crs=geo.get("crs"),
        geometry_type=geo.get("geometry_type"),
        bbox=geo.get("bbox"),
        scan_failed_version=scanner_version() if layer_errors else None,
        _seen=True,
        _match_path=match_path,
    )
    finalize_scanned_dataset(
        catalog,
        dataset,
        variables=variables,
        freq_table=freq_table,
        preview=preview,
        label=label,
        auto_enumerations=auto_enumerations,
    )
    return nb_row, len(variables)


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
    remember_preview(catalog, dataset.id, preview, label=label, variables=variables)
    var_id_mapping = build_variable_ids(variables, dataset.id)
    if freq_table is not None:
        catalog.enumeration_manager.assign_from_freq(
            variables, freq_table, var_id_mapping, auto_enumerations=auto_enumerations
        )
    catalog.variable.add_all(variables)
