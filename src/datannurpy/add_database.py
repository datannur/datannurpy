"""Add database to catalog."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import ibis

from .scanner.database import _encode_uri_credentials
from .utils import (
    build_variable_ids,
    get_prefix_folders,
    get_table_prefix,
    log_done,
    log_error,
    log_folder,
    log_section,
    log_skip,
    log_start,
    log_summary,
    make_id,
    sanitize_id,
    timestamp_to_iso,
    upsert_folder,
)
from .utils.params import _UNSET, validate_params
from .scanner.filesystem import FileSystem
from .scanner.utils import get_mtime_iso
from .finalize import remove_dataset_cascade
from .preview import (
    PreviewRows,
    effective_preview_rows,
    remember_preview,
    resolve_preview_rows,
)
from .schema import Dataset, EntityMetadata, Folder, Variable, folder_from_metadata
from .scanner.database import (
    batch_table_data_size,
    batch_table_row_count,
    build_table_data_path,
    close_connection,
    compute_schema_signature,
    connect,
    get_database_name,
    get_database_path,
    get_schemas_to_scan,
    get_table_data_size,
    get_table_row_count,
    is_remote_database_file,
    list_tables,
    open_ssh_tunnel,
    sanitize_connection_url,
    scan_table,
)

from .scanner.db_introspect import TableMetadata, introspect_schema
from .scanner.timeseries import (
    _build_series_dataset_id_with_suffix,
    PERIOD_PLACEHOLDER,
    TableSeriesGroup,
    build_series_dataset_name,
    compute_variable_periods,
    group_table_time_series,
)
from .utils.db_enrich import (
    apply_metadata_to_new_vars,
    collect_cached_var_changes,
    collect_fk_refs,
    ensure_db_tags,
    resolve_foreign_keys,
)

if TYPE_CHECKING:
    from .catalog import Catalog, Depth


@validate_params
def add_database(
    catalog: Catalog,
    connection: str | ibis.BaseBackend,
    metadata: EntityMetadata | None = None,
    *,
    depth: Depth | None = None,
    schema: str | Sequence[str] | None = None,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    sample_size: int | None = _UNSET,
    auto_enumerations: bool | None = None,
    preview_rows: PreviewRows = None,
    group_by_prefix: bool | str = True,
    prefix_min_tables: int = 2,
    time_series: bool = True,
    quiet: bool | None = None,
    refresh: bool | None = None,
    storage_options: dict[str, str] | None = None,
    oracle_client_path: str | None = None,
    ssh_tunnel: dict[str, str | int] | None = None,
) -> None:
    """Scan a database and add its tables to the catalog."""
    if isinstance(schema, list):
        kwargs = {k: v for k, v in locals().items() if k not in ("catalog", "schema")}
        for s in schema:
            add_database(catalog, schema=s, **kwargs)
        return
    assert not isinstance(schema, Sequence) or isinstance(schema, str)

    resolved_sample_size = (
        sample_size if sample_size is not _UNSET else catalog.sample_size
    )
    resolved_depth = depth if depth is not None else catalog.depth
    resolved_auto_enumerations = (
        auto_enumerations
        if auto_enumerations is not None
        else catalog.auto_enumerations
    )
    preview_limit = effective_preview_rows(
        resolve_preview_rows(preview_rows, catalog.preview_rows), resolved_depth
    )

    # Handle remote SQLite files (sftp://, s3://, etc.)
    if isinstance(connection, str) and is_remote_database_file(connection):
        remote_path = urlparse(connection).path
        fs = FileSystem(connection, storage_options)
        with fs.ensure_local(remote_path) as local_path:
            _add_database_impl(
                catalog,
                f"sqlite:///{local_path}",
                metadata,
                depth=depth,
                schema=schema,
                include=include,
                exclude=exclude,
                sample_size=resolved_sample_size,
                auto_enumerations=resolved_auto_enumerations,
                preview_rows=preview_limit,
                group_by_prefix=group_by_prefix,
                prefix_min_tables=prefix_min_tables,
                time_series=time_series,
                quiet=quiet,
                refresh=refresh,
                remote_path=connection,
            )
        return

    # SSH tunnel: open tunnel, replace connection URI, then scan
    if ssh_tunnel and isinstance(connection, str):
        with open_ssh_tunnel(ssh_tunnel, connection) as tunneled_uri:
            _add_database_impl(
                catalog,
                tunneled_uri,
                metadata,
                depth=depth,
                schema=schema,
                include=include,
                exclude=exclude,
                sample_size=resolved_sample_size,
                auto_enumerations=resolved_auto_enumerations,
                preview_rows=preview_limit,
                group_by_prefix=group_by_prefix,
                prefix_min_tables=prefix_min_tables,
                time_series=time_series,
                quiet=quiet,
                refresh=refresh,
                remote_path=connection,
                oracle_client_path=oracle_client_path,
            )
        return

    _add_database_impl(
        catalog,
        connection,
        metadata,
        depth=depth,
        schema=schema,
        include=include,
        exclude=exclude,
        sample_size=resolved_sample_size,
        auto_enumerations=resolved_auto_enumerations,
        preview_rows=preview_limit,
        group_by_prefix=group_by_prefix,
        prefix_min_tables=prefix_min_tables,
        time_series=time_series,
        quiet=quiet,
        refresh=refresh,
        remote_path=None,
        oracle_client_path=oracle_client_path,
    )


@dataclass
class _DbScan:
    """Shared context and accumulators for one add_database run."""

    catalog: Catalog
    con: ibis.BaseBackend
    backend_name: str
    db_name: str
    depth: Depth
    quiet: bool
    refresh: bool
    introspect: bool
    now_iso: str
    freq_threshold: int | None
    sample_size: int | None
    auto_enumerations: bool
    preview_rows: int
    raw_fk_refs: list[tuple[str, str | None, str, str]] = field(default_factory=list)
    table_to_dataset_id: dict[tuple[str | None, str], str] = field(default_factory=dict)
    # Variables mutated by `collect_cached_var_changes` are accumulated and
    # flushed once per schema to avoid per-table rebuilds.
    cached_changed_vars: list[Variable] = field(default_factory=list)
    scanned: int = 0
    unchanged: int = 0
    scan_errors: int = 0
    resource_count: int = 0


@dataclass
class _SchemaCtx:
    """Per-schema lookup tables shared by the table-scanning helpers."""

    name: str | None
    folder_id: str
    meta: dict[str, TableMetadata]
    size_cache: dict[str, int]
    count_cache: dict[str, int]
    prefix_folder_ids: dict[str, str]
    valid_prefixes: set[str]
    prefix_sep: str
    seen_ids: list[str] = field(default_factory=list)


def _upsert_database_root_folder(
    catalog: Catalog,
    metadata: EntityMetadata | None,
    *,
    db_name: str,
    backend_name: str,
    connection: str | ibis.BaseBackend,
    remote_path: str | None,
) -> str:
    """Create or refresh the root folder for the database; returns its id."""
    if metadata is None:
        folder = Folder(id=sanitize_id(db_name), name=db_name)
    else:
        folder = folder_from_metadata(
            metadata,
            default_id=sanitize_id(db_name),
            default_name=db_name,
        )

    # Set public data_path while keeping local file paths internal-only.
    if remote_path:
        folder.data_path = sanitize_connection_url(remote_path)
        folder.last_update_date = None  # Can't get mtime from remote
    else:
        local_database_path = (
            get_database_path(connection, backend_name)
            if isinstance(connection, str)
            else None
        )
        folder.data_path = f"{backend_name}://{db_name}"
        # Use mtime of database file for last_update_date (null for non-file connections)
        if local_database_path:
            folder.last_update_date = get_mtime_iso(Path(local_database_path))
        else:
            folder.last_update_date = None
    folder.type = folder.type or backend_name

    upsert_folder(catalog, folder)
    return folder.id


def _create_prefix_folders(
    catalog: Catalog,
    tables: list[str],
    series_table_names: set[str],
    series_groups: list[TableSeriesGroup],
    *,
    group_by_prefix: bool | str,
    prefix_sep: str,
    prefix_min_tables: int,
    parent_folder_id: str,
) -> tuple[dict[str, str], set[str]]:
    """Create folders for grouped table-name prefixes.

    Returns (prefix -> folder_id, valid prefixes)."""
    if not group_by_prefix:
        return {}, set()

    # Use effective table list: exclude series tables, add one
    # representative per series so prefixes reflect grouped names
    effective_tables = [t for t in tables if t not in series_table_names]
    effective_tables.extend(
        group.normalized_name.replace(PERIOD_PLACEHOLDER, "PERIOD")
        for group in series_groups
    )
    prefix_folders = get_prefix_folders(
        effective_tables, sep=prefix_sep, min_count=prefix_min_tables
    )

    prefix_folder_ids: dict[str, str] = {}  # prefix → folder_id
    for pf in prefix_folders:
        if pf.parent_prefix is not None:
            parent_id = prefix_folder_ids[pf.parent_prefix]
        else:
            parent_id = parent_folder_id

        folder_id = make_id(parent_id, sanitize_id(pf.prefix))
        prefix_folder_ids[pf.prefix] = folder_id

        upsert_folder(
            catalog,
            Folder(
                id=folder_id,
                name=pf.prefix,
                parent_id=parent_id,
                type="table_prefix",
            ),
        )
    return prefix_folder_ids, {pf.prefix for pf in prefix_folders}


def _table_folder_id(sc: _SchemaCtx, table_name: str) -> str:
    """Folder for a table: its prefix folder when grouped, else the schema folder."""
    table_prefix: str | None = None
    if sc.valid_prefixes:
        table_prefix = get_table_prefix(
            table_name, sc.valid_prefixes, sep=sc.prefix_sep
        )
    return sc.prefix_folder_ids[table_prefix] if table_prefix else sc.folder_id


def _record_unchanged(
    run: _DbScan, sc: _SchemaCtx, existing_id: str, table_name: str
) -> None:
    """Bookkeeping for a cache hit: keep the dataset, refresh cached metadata."""
    sc.seen_ids.append(existing_id)
    if run.introspect:
        meta = sc.meta[table_name]
        run.cached_changed_vars.extend(
            collect_cached_var_changes(run.catalog, existing_id, meta)
        )
        run.table_to_dataset_id[(sc.name, table_name)] = existing_id
        collect_fk_refs(meta.fks, existing_id, run.raw_fk_refs)
    run.unchanged += 1
    log_skip(table_name, run.quiet)


def _scan_table_structure(
    run: _DbScan,
    sc: _SchemaCtx,
    table_name: str,
    table_data_path: str,
    existing_dataset: Any,
    t0: float,
) -> None:
    """Dataset/variable depth: no signature, no row count, no incremental check."""
    if existing_dataset is not None and not run.refresh:
        _record_unchanged(run, sc, existing_dataset.id, table_name)
        return

    table_folder_id = _table_folder_id(sc, table_name)
    dataset_id = make_id(table_folder_id, sanitize_id(table_name))

    # Variable mode: scan columns
    table_vars = []
    if run.depth == "variable":
        try:
            table_vars, _, _, _ = scan_table(
                run.con,
                table_name,
                schema=sc.name,
                dataset_id=dataset_id,
                infer_stats=False,
            )
        except Exception as exc:
            log_error(table_name, exc, run.quiet)
            run.scan_errors += 1
            return

    is_change = existing_dataset is not None
    if is_change:
        remove_dataset_cascade(run.catalog, existing_dataset)
    dataset = Dataset(
        id=dataset_id,
        name=table_name,
        folder_id=table_folder_id,
        delivery_format=run.backend_name,
        last_update_date=run.now_iso if is_change else None,
        data_path=table_data_path,
        data_size=get_table_data_size(run.con, table_name, sc.name),
        preview_rows=0,
        _seen=True,
        _match_path=table_data_path,
    )
    if run.introspect:
        meta = sc.meta[table_name]
        apply_metadata_to_new_vars(table_vars, dataset, meta)
        run.table_to_dataset_id[(sc.name, table_name)] = dataset_id
        collect_fk_refs(meta.fks, dataset_id, run.raw_fk_refs)
    run.catalog.dataset.add(dataset)
    run.scanned += 1
    if table_vars:
        build_variable_ids(table_vars, dataset.id)
        run.catalog.variable.add_all(table_vars)
        log_done(f"{table_name} ({len(table_vars)} vars)", run.quiet, t0)
    else:
        log_done(table_name, run.quiet, t0)


def _scan_table_stats(
    run: _DbScan,
    sc: _SchemaCtx,
    table_name: str,
    table_data_path: str,
    existing_dataset: Any,
    t0: float,
) -> None:
    """Stat/value depth: full scan with an incremental check on signature + rows."""
    # Compute signature and exact row count for incremental check
    try:
        current_signature = compute_schema_signature(run.con, table_name, sc.name)
        current_nb_row = (
            sc.count_cache[table_name]
            if table_name in sc.count_cache
            else get_table_row_count(run.con, table_name, sc.name)
        )
    except Exception as exc:
        log_error(table_name, exc, run.quiet)
        run.scan_errors += 1
        return

    # Preserve timestamp if data unchanged (for stable evolution tracking)
    preserved_date: str | None = None

    if existing_dataset is not None:
        data_unchanged = (
            existing_dataset.schema_signature == current_signature
            and existing_dataset.nb_row == current_nb_row
        )

        if not run.refresh and data_unchanged:
            _record_unchanged(run, sc, existing_dataset.id, table_name)
            return

        if data_unchanged:
            preserved_date = existing_dataset.last_update_date

    table_folder_id = _table_folder_id(sc, table_name)
    dataset_id = make_id(table_folder_id, sanitize_id(table_name))

    # Timestamps
    if existing_dataset is None:
        effective_date = None
    elif preserved_date is not None:
        effective_date = preserved_date
    else:
        effective_date = run.now_iso

    try:
        table_vars, nb_row, actual_sample_size, freq_table, preview = scan_table(
            run.con,
            table_name,
            schema=sc.name,
            dataset_id=dataset_id,
            infer_stats=True,
            freq_threshold=run.freq_threshold,
            sample_size=run.sample_size,
            preview_rows=run.preview_rows,
            return_preview=True,
            quiet=run.quiet,
            row_count=current_nb_row,
        )
    except Exception as exc:
        log_error(table_name, exc, run.quiet)
        run.scan_errors += 1
        return

    if existing_dataset is not None:
        remove_dataset_cascade(run.catalog, existing_dataset)

    dataset = Dataset(
        id=dataset_id,
        name=table_name,
        folder_id=table_folder_id,
        delivery_format=run.backend_name,
        last_update_date=effective_date,
        data_path=table_data_path,
        nb_row=nb_row,
        data_size=sc.size_cache.get(table_name)
        or get_table_data_size(run.con, table_name, sc.name),
        sample_size=actual_sample_size,
        preview_rows=run.preview_rows,
        schema_signature=current_signature,
        _seen=True,
        _match_path=table_data_path,
    )
    meta = sc.meta[table_name]
    apply_metadata_to_new_vars(table_vars, dataset, meta)
    run.table_to_dataset_id[(sc.name, table_name)] = dataset_id
    collect_fk_refs(meta.fks, dataset_id, run.raw_fk_refs)
    run.catalog.dataset.add(dataset)
    run.scanned += 1
    remember_preview(
        run.catalog, dataset.id, preview, label=table_name, variables=table_vars
    )

    var_id_mapping = build_variable_ids(table_vars, dataset.id)
    if freq_table is not None:
        run.catalog.enumeration_manager.assign_from_freq(
            table_vars,
            freq_table,
            var_id_mapping,
            auto_enumerations=run.auto_enumerations,
        )
    run.catalog.variable.add_all(table_vars)

    log_done(f"{table_name} ({nb_row:,} rows, {len(table_vars)} vars)", run.quiet, t0)


def _scan_schema(
    run: _DbScan,
    schema_name: str | None,
    *,
    root_folder_id: str,
    include: Sequence[str] | None,
    exclude: Sequence[str] | None,
    time_series: bool,
    group_by_prefix: bool | str,
    prefix_min_tables: int,
) -> None:
    """Scan one schema: its tables, series groups and per-schema bookkeeping."""
    if schema_name is not None:
        log_folder(f"{schema_name} (schema)", run.quiet)
        # Multiple schemas: create sub-folder for each
        current_folder_id = make_id(root_folder_id, sanitize_id(schema_name))
        upsert_folder(
            run.catalog,
            Folder(
                id=current_folder_id,
                name=schema_name,
                parent_id=root_folder_id,
                type="schema",
            ),
        )
    else:
        current_folder_id = root_folder_id

    tables = list_tables(run.con, schema_name, include, exclude, run.backend_name)
    run.resource_count += len(tables)

    # Group tables by time series if enabled
    series_table_names: set[str] = set()
    series_groups: list[TableSeriesGroup] = []
    if time_series:
        series_groups, _singles = group_table_time_series(tables)
        for group in series_groups:
            for _, tname in group.tables:
                series_table_names.add(tname)

    # Batch introspection: one pass per schema instead of per table
    if run.introspect:
        schema_meta = introspect_schema(run.con, run.backend_name, schema_name, tables)
    else:
        schema_meta = {t: TableMetadata() for t in tables}

    # Batch data size and row counts in bulk queries
    size_cache: dict[str, int] = {}
    count_cache: dict[str, int] = {}
    if run.depth in ("stat", "value"):
        size_cache = batch_table_data_size(run.con, tables, schema_name)
        count_cache = batch_table_row_count(run.con, tables, schema_name)

    prefix_sep = "_" if group_by_prefix is True else group_by_prefix or "_"
    prefix_folder_ids, valid_prefixes = _create_prefix_folders(
        run.catalog,
        tables,
        series_table_names,
        series_groups,
        group_by_prefix=group_by_prefix,
        prefix_sep=prefix_sep,
        prefix_min_tables=prefix_min_tables,
        parent_folder_id=current_folder_id,
    )

    sc = _SchemaCtx(
        name=schema_name,
        folder_id=current_folder_id,
        meta=schema_meta,
        size_cache=size_cache,
        count_cache=count_cache,
        prefix_folder_ids=prefix_folder_ids,
        valid_prefixes=valid_prefixes,
        prefix_sep=prefix_sep,
    )

    existing_by_path: dict[str, Any] = {
        ds._match_path: ds for ds in run.catalog.dataset.all() if ds._match_path
    }
    for table_name in tables:
        if table_name in series_table_names:
            continue
        t0 = log_start(table_name, run.quiet)

        # Build data_path for incremental lookup
        table_data_path = build_table_data_path(
            run.backend_name, run.db_name, schema_name, table_name
        )
        existing_dataset = existing_by_path.get(table_data_path)

        if run.depth in ("stat", "value"):
            _scan_table_stats(
                run, sc, table_name, table_data_path, existing_dataset, t0
            )
        else:
            _scan_table_structure(
                run, sc, table_name, table_data_path, existing_dataset, t0
            )

    if sc.seen_ids:
        run.catalog.dataset.update_many(
            sc.seen_ids, _seen=True, preview_rows=run.preview_rows
        )
        run.catalog.enumeration_manager.mark_datasets_seen(sc.seen_ids)

    # Process time series groups
    for group in series_groups:
        rep = group.normalized_name.replace(PERIOD_PLACEHOLDER, "PERIOD")
        series_error = _scan_table_series(
            run.catalog,
            run.con,
            group,
            folder_id=_table_folder_id(sc, rep),
            schema_name=schema_name,
            backend_name=run.backend_name,
            db_name=run.db_name,
            depth=run.depth,
            freq_threshold=run.freq_threshold,
            sample_size=run.sample_size,
            auto_enumerations=run.auto_enumerations,
            preview_rows=run.preview_rows,
            quiet=run.quiet,
        )
        run.scan_errors += series_error
        if series_error == 0:
            # A series always rescans (no incremental skip), so success
            # means exactly one dataset was (re)scanned.
            run.scanned += 1

    # Flush batched cached-metadata updates (single rebuild)
    if run.cached_changed_vars:
        run.catalog.variable.remove_all([v.id for v in run.cached_changed_vars])
        run.catalog.variable.add_all(run.cached_changed_vars)
        run.cached_changed_vars.clear()

    # Resolve FK refs
    resolve_foreign_keys(run.catalog, run.raw_fk_refs, run.table_to_dataset_id)


def _add_database_impl(
    catalog: Catalog,
    connection: str | ibis.BaseBackend,
    metadata: EntityMetadata | None,
    *,
    depth: Depth | None,
    schema: str | None,
    include: Sequence[str] | None,
    exclude: Sequence[str] | None,
    sample_size: int | None,
    auto_enumerations: bool,
    preview_rows: int,
    group_by_prefix: bool | str,
    prefix_min_tables: int,
    time_series: bool,
    quiet: bool | None,
    refresh: bool | None,
    remote_path: str | None,
    oracle_client_path: str | None = None,
) -> None:
    """Implementation of add_database (local or already-downloaded remote)."""
    catalog._has_scanned = True
    q = quiet if quiet is not None else catalog.quiet
    resolved_depth: Depth = depth if depth is not None else catalog.depth
    # Connect to database
    con, backend_name = connect(connection, oracle_client_path=oracle_client_path)

    # Determine database name for folder
    if remote_path:
        # Strip query string before extracting stem (e.g. ?ssl_mode=DISABLED)
        db_name = PurePosixPath(
            urlparse(_encode_uri_credentials(remote_path)).path
        ).stem
    else:
        db_name = get_database_name(connection, con, backend_name)

    start_time = log_section("add_database", f"{backend_name}://{db_name}", q)
    vars_before = catalog.variable.count

    # Determine schemas to scan
    schemas_to_scan = get_schemas_to_scan(con, schema, backend_name)

    root_folder_id = _upsert_database_root_folder(
        catalog,
        metadata,
        db_name=db_name,
        backend_name=backend_name,
        connection=connection,
        remote_path=remote_path,
    )

    # DB introspection setup (active for depth >= "variable")
    do_introspect = resolved_depth != "dataset"
    if do_introspect:
        ensure_db_tags(catalog)
    if resolved_depth == "value":
        from .scanner.autotag import ensure_auto_tags

        ensure_auto_tags(catalog)

    run = _DbScan(
        catalog=catalog,
        con=con,
        backend_name=backend_name,
        db_name=db_name,
        depth=resolved_depth,
        quiet=q,
        refresh=refresh if refresh is not None else catalog.refresh,
        introspect=do_introspect,
        now_iso=timestamp_to_iso(catalog._now),
        freq_threshold=catalog.freq_threshold if resolved_depth == "value" else None,
        sample_size=sample_size if resolved_depth == "value" else None,
        auto_enumerations=auto_enumerations,
        preview_rows=preview_rows,
    )

    for schema_name in schemas_to_scan:
        _scan_schema(
            run,
            schema_name,
            root_folder_id=root_folder_id,
            include=include,
            exclude=exclude,
            time_series=time_series,
            group_by_prefix=group_by_prefix,
            prefix_min_tables=prefix_min_tables,
        )

    # GeoPackage: enrich datasets with CRS / geometry type from gpkg_* tables.
    # Reads plain SQL on the open SQLite connection; no-op for non-GeoPackage DBs.
    if backend_name == "sqlite":
        from .scanner.geopackage import apply_geopackage_geo

        apply_geopackage_geo(catalog, con, backend_name, db_name)

    # Close connection if we created it (string connection)
    if isinstance(connection, str):
        close_connection(con)

    vars_added = catalog.variable.count - vars_before
    catalog._tally_scan(run.scanned, run.unchanged, run.scan_errors)
    log_summary(
        run.scanned,
        None if resolved_depth == "dataset" else vars_added,
        q,
        start_time,
        run.scan_errors,
        resource_count=run.resource_count,
        resource_label="tables",
        unchanged=run.unchanged,
    )


def _scan_table_series(
    catalog: Catalog,
    con: ibis.BaseBackend,
    group: TableSeriesGroup,
    *,
    folder_id: str,
    schema_name: str | None,
    backend_name: str,
    db_name: str,
    depth: Depth,
    freq_threshold: int | None,
    sample_size: int | None,
    auto_enumerations: bool,
    preview_rows: int,
    quiet: bool,
) -> int:
    """Scan a time series of database tables. Returns 1 on error, 0 on success."""
    tables = group.tables  # [(period_str, table_name), ...]
    periods = [p for p, _ in tables]
    first_period = periods[0]
    last_period, last_table = tables[-1]
    normalized = group.normalized_name

    dataset_name = build_series_dataset_name(normalized, periods)
    dataset_id = _build_series_dataset_id_with_suffix(
        normalized,
        folder_id,
        group.id_suffix,
    )
    data_path = build_table_data_path(backend_name, db_name, schema_name, last_table)

    # Remove existing dataset if present
    existing = catalog.dataset.get_by("_match_path", data_path)
    if existing:
        remove_dataset_cascade(catalog, existing)

    t0 = log_start(f"{dataset_name} ({len(tables)} tables)", quiet)

    table_vars: list[Variable] = []
    nb_row: int | None = None
    actual_sample_size: int | None = None
    freq_table = None
    preview = None

    if depth == "dataset":
        pass  # No scanning needed
    else:
        # Schema scan all tables for columns_by_period
        columns_by_period: dict[str, list[str]] = {}
        last_schema_vars = None
        for period, tname in tables:
            try:
                tvars, _, _, _ = scan_table(
                    con,
                    tname,
                    schema=schema_name,
                    dataset_id=dataset_id,
                    infer_stats=False,
                )
                columns_by_period[period] = [v.name for v in tvars]
                if tname == last_table:
                    last_schema_vars = tvars
            except Exception:
                continue

        var_periods = compute_variable_periods(columns_by_period)

        if depth in ("stat", "value"):
            # Stat/Value: scan latest table for stats
            try:
                row_count = get_table_row_count(con, last_table, schema_name)
                table_vars, nb_row, actual_sample_size, freq_table, preview = (
                    scan_table(
                        con,
                        last_table,
                        schema=schema_name,
                        dataset_id=dataset_id,
                        infer_stats=True,
                        freq_threshold=freq_threshold,
                        sample_size=sample_size,
                        preview_rows=preview_rows,
                        return_preview=True,
                        quiet=quiet,
                        row_count=row_count,
                    )
                )
            except Exception as exc:
                log_error(dataset_name, exc, quiet)
                return 1
        else:
            # Variable mode: reuse schema scan from columns_by_period loop
            if last_schema_vars is not None:
                table_vars = last_schema_vars
            else:
                try:
                    table_vars, _, _, _ = scan_table(
                        con,
                        last_table,
                        schema=schema_name,
                        dataset_id=dataset_id,
                        infer_stats=False,
                    )
                except Exception as exc:
                    log_error(dataset_name, exc, quiet)
                    return 1

        # Apply variable periods (start_date/end_date)
        vars_in_last = {v.name for v in table_vars}
        for var in table_vars:
            start, end = var_periods.get(var.name, (None, None))
            var.start_date = start
            var.end_date = end

        # Add skeleton variables removed from latest but present in older tables
        for var_name in set(var_periods.keys()) - vars_in_last:
            start, end = var_periods.get(var_name, (None, None))
            table_vars.append(
                Variable(
                    id="",
                    name=var_name,
                    dataset_id=dataset_id,
                    start_date=start,
                    end_date=end,
                )
            )

    dataset = Dataset(
        id=dataset_id,
        name=dataset_name,
        folder_id=folder_id,
        delivery_format=backend_name,
        data_path=data_path,
        nb_row=nb_row,
        nb_resources=len(tables),
        start_date=first_period,
        end_date=last_period,
        sample_size=actual_sample_size,
        preview_rows=preview_rows,
        _seen=True,
        _match_path=data_path,
    )
    catalog.dataset.add(dataset)
    remember_preview(
        catalog,
        dataset.id,
        preview if depth != "variable" else None,
        label=dataset_name,
        variables=table_vars,
    )

    if table_vars:
        var_id_mapping = build_variable_ids(table_vars, dataset.id)
        if freq_table is not None:
            catalog.enumeration_manager.assign_from_freq(
                table_vars,
                freq_table,
                var_id_mapping,
                auto_enumerations=auto_enumerations,
            )
        catalog.variable.add_all(table_vars)

    if nb_row is not None and nb_row > 0:
        log_done(
            f"{dataset_name} ({nb_row:,} rows, {len(table_vars)} vars, "
            f"{len(tables)} tables)",
            quiet,
            t0,
        )
    elif table_vars:
        log_done(
            f"{dataset_name} ({len(table_vars)} vars, {len(tables)} tables)",
            quiet,
            t0,
        )
    else:
        log_done(f"{dataset_name} ({len(tables)} tables)", quiet, t0)

    return 0
