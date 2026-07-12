"""Add an ESRI File Geodatabase (.gdb) to the catalog as one dataset per layer.

A File Geodatabase is a multi-layer vector container (a directory on disk). Like
``add_database`` turns each table of a database into a dataset under a container
folder, ``add_geodatabase`` turns each layer of a ``.gdb`` into a dataset — but the
layers are read through GDAL/OGR (pyogrio), not a SQL connection.
"""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import nullcontext
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, cast

from .dataset_scan import finalize_scanned_dataset, skip_unchanged
from .errors import ConfigError
from .scanner.database import filter_by_patterns
from .preview import (
    PreviewRows,
    effective_preview_rows,
    resolve_preview_rows,
)
from .schema import Dataset, EntityMetadata, Folder, folder_from_metadata
from .scanner.filesystem import FileSystem, is_remote_url
from .scanner.utils import get_mtime_timestamp
from .utils import (
    error_count,
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
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    auto_enumerations: bool | None = None,
    preview_rows: PreviewRows = None,
    quiet: bool | None = None,
    refresh: bool | None = None,
    storage_options: dict[str, str] | None = None,
) -> None:
    """Add an ESRI File Geodatabase (``.gdb``): one dataset per layer, in a folder.

    Works on a local path or a remote URL (the ``.gdb`` directory is downloaded
    once and scanned locally, like ``add_database`` does for a remote SQLite file).
    """
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

    # Resolve the .gdb identity (id/mtime from the remote/local source) and a context
    # yielding a local directory to read from (downloaded temp for remote).
    if is_remote_url(path) or storage_options:
        fs = FileSystem(path, storage_options)
        gdb_name = fs.root.rstrip("/").rsplit("/", 1)[-1]
        if not gdb_name.lower().endswith(".gdb"):
            raise ConfigError(f"Not a File Geodatabase (.gdb directory): {path}")
        source_id = str(path)
        current_mtime = get_mtime_timestamp(PurePosixPath(fs.root), fs=fs)
        scan_ctx = fs.ensure_local_dir(fs.root)
    else:
        gdb_path = Path(path).resolve()
        if gdb_path.suffix.lower() != ".gdb" or not gdb_path.is_dir():
            raise ConfigError(f"Not a File Geodatabase (.gdb directory): {path}")
        gdb_name = gdb_path.name
        source_id = str(gdb_path)
        current_mtime = get_mtime_timestamp(gdb_path)
        scan_ctx = nullcontext(gdb_path)

    stem = Path(gdb_name).stem
    last_update = timestamp_to_iso(current_mtime)
    start_time = log_section("add_geodatabase", gdb_name, q)
    with scan_ctx as scan_dir:
        source = str(scan_dir)
        try:
            layers = filter_by_patterns(list_geo_layers(source), include, exclude)
        except Exception as e:
            log_error(gdb_name, e, q)
            catalog._tally_scan(0, 0, 1)
            return

        # Container folder, mirroring how add_database nests tables under the database.
        folder = (
            folder_from_metadata(
                metadata, default_id=sanitize_id(stem), default_name=stem
            )
            if metadata is not None
            else Folder(id=sanitize_id(stem), name=stem)
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
            match_path = f"{source_id}::{layer}"
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
                catalog._tally_scan(0, 1)
                continue
            errors_before = error_count()
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
            # Internally-handled layer errors still count for the run tally —
            # at most one per layer dataset.
            catalog._tally_scan(1, 0, min(1, error_count() - errors_before))
    log_done(f"{gdb_name} ({len(layers)} layers)", q, start_time)
