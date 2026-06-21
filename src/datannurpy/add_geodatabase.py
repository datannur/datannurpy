"""Add an ESRI File Geodatabase (.gdb) to the catalog as one dataset per layer.

A File Geodatabase is a multi-layer vector container (a directory on disk). Like
``add_database`` turns each table of a database into a dataset under a container
folder, ``add_geodatabase`` turns each layer of a ``.gdb`` into a dataset — but the
layers are read through GDAL/OGR (pyogrio), not a SQL connection.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

from .dataset_scan import finalize_scanned_dataset, skip_unchanged
from .errors import ConfigError
from .preview import (
    PreviewRows,
    effective_preview_rows,
    resolve_preview_rows,
)
from .schema import Dataset, EntityMetadata, Folder, folder_from_metadata
from .scanner.utils import get_mtime_timestamp
from .utils import (
    log_done,
    log_error,
    log_section,
    make_id,
    sanitize_id,
    timestamp_to_iso,
)
from .utils.folder import upsert_folder
from .utils.params import validate_params

if TYPE_CHECKING:
    from .catalog import Catalog, Depth


@validate_params
def add_geodatabase(
    catalog: Catalog,
    path: str | Path,
    metadata: EntityMetadata | None = None,
    *,
    depth: Depth | None = None,
    auto_enumerations: bool | None = None,
    preview_rows: PreviewRows = None,
    quiet: bool | None = None,
    refresh: bool | None = None,
) -> None:
    """Add an ESRI File Geodatabase (``.gdb``): one dataset per layer, in a folder."""
    from .scanner.geo_vector import list_geo_layers, scan_geo_vector

    catalog._has_scanned = True
    resolved_depth: Depth = depth if depth is not None else cast("Depth", catalog.depth)
    if resolved_depth == "value":
        from .scanner.autotag import ensure_auto_tags

        ensure_auto_tags(catalog)
    q = quiet if quiet is not None else catalog.quiet
    do_refresh = refresh if refresh is not None else catalog.refresh
    resolved_auto_enumerations = (
        auto_enumerations
        if auto_enumerations is not None
        else catalog.auto_enumerations
    )
    preview_limit = effective_preview_rows(
        resolve_preview_rows(preview_rows, catalog.preview_rows), resolved_depth
    )

    gdb_path = Path(path).resolve()
    if gdb_path.suffix.lower() != ".gdb" or not gdb_path.is_dir():
        raise ConfigError(f"Not a File Geodatabase (.gdb directory): {path}")
    gdb_name = gdb_path.name
    source = str(gdb_path)

    start_time = log_section("add_geodatabase", gdb_name, q)
    try:
        layers = list_geo_layers(source)
    except Exception as e:
        log_error(gdb_name, e, q)
        return

    # One stat for the whole geodatabase, reused for the folder and every layer.
    current_mtime = get_mtime_timestamp(gdb_path)
    last_update = timestamp_to_iso(current_mtime)

    # Container folder, mirroring how add_database nests tables under the database.
    folder = (
        folder_from_metadata(
            metadata, default_id=sanitize_id(gdb_path.stem), default_name=gdb_path.stem
        )
        if metadata is not None
        else Folder(id=sanitize_id(gdb_path.stem), name=gdb_path.stem)
    )
    folder.data_path = gdb_name
    folder.last_update_date = last_update
    folder.type = folder.type or "geodatabase"
    upsert_folder(catalog, folder)

    freq_threshold = catalog.freq_threshold if resolved_depth == "value" else None
    for layer in layers:
        label = f"{gdb_name}/{layer}"
        dataset_id = make_id(folder.id, sanitize_id(layer))
        data_path = f"{gdb_name}/{layer}"
        match_path = f"{source}::{layer}"
        if skip_unchanged(
            catalog,
            match_path,
            data_path,
            current_mtime,
            refresh=do_refresh,
            preview_rows=preview_limit,
            quiet=q,
            label=label,
        ):
            continue
        variables, nb_row, freq_table, geo, preview = scan_geo_vector(
            source,
            dataset_id=dataset_id,
            layer=layer,
            freq_threshold=freq_threshold,
            preview_rows=preview_limit,
            return_preview=True,
            quiet=q,
            path_label=label,
        )
        geo = geo or {}
        dataset = Dataset(
            id=dataset_id,
            name=layer,
            folder_id=folder.id,
            data_path=data_path,
            last_update_date=last_update,
            delivery_format="geodatabase",
            nb_row=nb_row,
            preview_rows=preview_limit,
            crs=geo.get("crs"),
            geometry_type=geo.get("geometry_type"),
            bbox=geo.get("bbox"),
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
            auto_enumerations=resolved_auto_enumerations,
        )
    log_done(f"{gdb_name} ({len(layers)} layers)", q, start_time)
