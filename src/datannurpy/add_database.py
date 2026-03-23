"""Add database to catalog."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Literal
from urllib.parse import urlparse

import ibis

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
from .utils.params import validate_params
from .scanner.filesystem import FileSystem
from .scanner.utils import get_mtime_iso
from .finalize import remove_dataset_cascade
from .schema import Dataset, Folder
from .scanner.database import (
    build_table_data_path,
    close_connection,
    compute_schema_signature,
    connect,
    get_database_name,
    get_database_path,
    get_schemas_to_scan,
    get_table_row_count,
    list_tables,
    scan_table,
)

if TYPE_CHECKING:
    from .catalog import Catalog


_DATABASE_SCHEMES = {
    "sqlite",
    "postgresql",
    "postgres",
    "mysql",
    "oracle",
    "mssql",
    "duckdb",
}


def _is_remote_database_file(connection: str) -> bool:
    """Check if connection is a remote file URL (sftp://, s3://, etc.), not a database URL."""
    if "://" not in connection:
        return False
    scheme = urlparse(connection).scheme.lower()
    return scheme not in _DATABASE_SCHEMES and scheme != "file"


@validate_params
def add_database(
    catalog: Catalog,
    connection: str | ibis.BaseBackend,
    folder: Folder | None = None,
    *,
    depth: Literal["structure", "schema", "full"] | None = None,
    schema: str | Sequence[str] | None = None,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    infer_stats: bool = True,
    sample_size: int | None = None,
    group_by_prefix: bool | str = True,
    prefix_min_tables: int = 2,
    quiet: bool | None = None,
    refresh: bool | None = None,
    storage_options: dict[str, str] | None = None,
    oracle_client_path: str | None = None,
) -> None:
    """Scan a database and add its tables to the catalog."""
    if isinstance(schema, list):
        kwargs = {k: v for k, v in locals().items() if k not in ("catalog", "schema")}
        for s in schema:
            add_database(catalog, schema=s, **kwargs)
        return
    assert not isinstance(schema, Sequence) or isinstance(schema, str)

    # Handle remote SQLite files (sftp://, s3://, etc.)
    if isinstance(connection, str) and _is_remote_database_file(connection):
        remote_path = urlparse(connection).path
        fs = FileSystem(connection, storage_options)
        with fs.ensure_local(remote_path) as local_path:
            _add_database_impl(
                catalog,
                f"sqlite:///{local_path}",
                folder,
                depth=depth,
                schema=schema,
                include=include,
                exclude=exclude,
                infer_stats=infer_stats,
                sample_size=sample_size,
                group_by_prefix=group_by_prefix,
                prefix_min_tables=prefix_min_tables,
                quiet=quiet,
                refresh=refresh,
                remote_path=connection,
            )
        return

    _add_database_impl(
        catalog,
        connection,
        folder,
        depth=depth,
        schema=schema,
        include=include,
        exclude=exclude,
        infer_stats=infer_stats,
        sample_size=sample_size,
        group_by_prefix=group_by_prefix,
        prefix_min_tables=prefix_min_tables,
        quiet=quiet,
        refresh=refresh,
        remote_path=None,
        oracle_client_path=oracle_client_path,
    )


def _add_database_impl(
    catalog: Catalog,
    connection: str | ibis.BaseBackend,
    folder: Folder | None,
    *,
    depth: Literal["structure", "schema", "full"] | None,
    schema: str | None,
    include: Sequence[str] | None,
    exclude: Sequence[str] | None,
    infer_stats: bool,
    sample_size: int | None,
    group_by_prefix: bool | str,
    prefix_min_tables: int,
    quiet: bool | None,
    refresh: bool | None,
    remote_path: str | None,
    oracle_client_path: str | None = None,
) -> None:
    """Implementation of add_database (local or already-downloaded remote)."""
    catalog._has_scanned = True
    q = quiet if quiet is not None else catalog.quiet
    do_refresh = refresh if refresh is not None else catalog.refresh
    resolved_depth = depth if depth is not None else catalog.depth
    # Connect to database
    con, backend_name = connect(connection, oracle_client_path=oracle_client_path)

    # Determine database name for folder
    if remote_path:
        db_name = PurePosixPath(remote_path).stem
    else:
        db_name = get_database_name(connection, con, backend_name)

    start_time = log_section("add_database", f"{backend_name}://{db_name}", q)
    datasets_before = catalog.dataset.count
    vars_before = catalog.variable.count

    # Get timestamp for folder/dataset
    now_iso = timestamp_to_iso(catalog._now)

    # Determine schemas to scan
    schemas_to_scan = get_schemas_to_scan(con, schema, backend_name)

    # Create root folder for database
    if folder is None:
        root_folder_id = sanitize_id(db_name)
        folder = Folder(id=root_folder_id, name=db_name)
    else:
        root_folder_id = folder.id

    # Set data_path: use remote_path if remote, otherwise local path
    if remote_path:
        folder.data_path = remote_path
        folder.last_update_date = None  # Can't get mtime from remote
    else:
        folder.data_path = (
            get_database_path(connection, backend_name)
            if isinstance(connection, str)
            else None
        )
        # Use mtime of database file for last_update_date (null for non-file connections)
        if folder.data_path:
            folder.last_update_date = get_mtime_iso(Path(folder.data_path))
        else:
            folder.last_update_date = None
    folder.type = backend_name

    # Add or update root folder
    upsert_folder(catalog, folder)

    freq_threshold = catalog.freq_threshold if catalog.freq_threshold else None

    # Process each schema
    scan_errors = 0
    for schema_name in schemas_to_scan:
        # Determine folder for this schema
        if schema_name is not None and len(schemas_to_scan) > 1:
            log_folder(f"{schema_name} (schema)", q)
            # Multiple schemas: create sub-folder for each
            schema_folder_id = make_id(root_folder_id, sanitize_id(schema_name))

            upsert_folder(
                catalog,
                Folder(
                    id=schema_folder_id,
                    name=schema_name,
                    parent_id=root_folder_id,
                    type="schema",
                ),
            )
            current_folder_id = schema_folder_id
        else:
            current_folder_id = root_folder_id

        # Get tables
        tables = list_tables(con, schema_name, include, exclude, backend_name)

        # Group tables by prefix if enabled
        prefix_folder_ids: dict[str, str] = {}  # prefix → folder_id
        valid_prefixes: set[str] = set()
        prefix_sep = "_" if group_by_prefix is True else group_by_prefix or "_"

        if group_by_prefix:
            prefix_folders = get_prefix_folders(
                tables, sep=prefix_sep, min_count=prefix_min_tables
            )
            valid_prefixes = {pf.prefix for pf in prefix_folders}

            # Create prefix folders
            for pf in prefix_folders:
                if pf.parent_prefix is not None:
                    parent_id = prefix_folder_ids[pf.parent_prefix]
                else:
                    parent_id = current_folder_id

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

        for table_name in tables:
            log_start(table_name, q)

            # Build data_path for incremental lookup
            table_data_path = build_table_data_path(
                backend_name, db_name, schema_name, table_name
            )

            # Check if table exists in cache
            existing_dataset = catalog.dataset.get_by("data_path", table_data_path)

            # Structure mode: just enumerate tables, no queries
            if resolved_depth == "structure":
                if existing_dataset is not None and not do_refresh:
                    catalog.dataset.update(existing_dataset.id, _seen=True)
                    log_skip(table_name, q)
                    continue

                # Determine folder for this table
                table_prefix: str | None = None
                if valid_prefixes:
                    table_prefix = get_table_prefix(
                        table_name, valid_prefixes, sep=prefix_sep
                    )
                if table_prefix:
                    table_folder_id = prefix_folder_ids[table_prefix]
                else:
                    table_folder_id = current_folder_id

                dataset_id = make_id(table_folder_id, sanitize_id(table_name))

                is_change = existing_dataset is not None
                if is_change:
                    remove_dataset_cascade(catalog, existing_dataset)
                dataset = Dataset(
                    id=dataset_id,
                    name=table_name,
                    folder_id=table_folder_id,
                    delivery_format=backend_name,
                    last_update_date=now_iso if is_change else None,
                    data_path=table_data_path,
                    last_update_timestamp=catalog._now if is_change else None,
                    _seen=True,
                )
                catalog.dataset.add(dataset)
                log_done(table_name, q)
                continue

            # Compute signature and row count for comparison/storage
            try:
                current_signature = compute_schema_signature(
                    con, table_name, schema_name
                )
                current_nb_row = get_table_row_count(con, table_name, schema_name)
            except Exception as exc:
                log_error(table_name, exc, q)
                scan_errors += 1
                continue

            # Preserve timestamp if data unchanged (for stable evolution tracking)
            preserved_timestamp: int | None = None

            if existing_dataset is not None:
                # Check if data actually changed
                data_unchanged = (
                    existing_dataset.schema_signature == current_signature
                    and existing_dataset.nb_row == current_nb_row
                )

                if not do_refresh and data_unchanged:
                    # Unchanged, skip
                    catalog.dataset.update(existing_dataset.id, _seen=True)
                    catalog.modality_manager.mark_dataset_seen(existing_dataset.id)
                    log_skip(table_name, q)
                    continue

                # Preserve timestamp if data unchanged (even with refresh)
                if data_unchanged:
                    preserved_timestamp = existing_dataset.last_update_timestamp

            # Determine folder for this table
            table_prefix: str | None = None
            if valid_prefixes:
                table_prefix = get_table_prefix(
                    table_name, valid_prefixes, sep=prefix_sep
                )

            if table_prefix:
                table_folder_id = prefix_folder_ids[table_prefix]
            else:
                table_folder_id = current_folder_id

            # Build dataset ID
            dataset_id = make_id(table_folder_id, sanitize_id(table_name))

            # First scan → None; rescan with change → now; unchanged → preserved
            if existing_dataset is None:
                effective_timestamp = None
                effective_date = None
            elif preserved_timestamp is not None:
                effective_timestamp = preserved_timestamp
                effective_date = timestamp_to_iso(preserved_timestamp)
            else:
                effective_timestamp = catalog._now
                effective_date = now_iso

            # Schema/Full mode: scan table
            schema_only = resolved_depth == "schema"
            try:
                table_vars, nb_row, freq_table = scan_table(
                    con,
                    table_name,
                    schema=schema_name,
                    dataset_id=dataset_id,
                    infer_stats=infer_stats and not schema_only,
                    freq_threshold=freq_threshold,
                    sample_size=sample_size,
                )
            except Exception as exc:
                log_error(table_name, exc, q)
                scan_errors += 1
                continue

            # Remove old dataset only after successful scan
            if existing_dataset is not None:
                remove_dataset_cascade(catalog, existing_dataset)

            # Create dataset with incremental fields
            dataset = Dataset(
                id=dataset_id,
                name=table_name,
                folder_id=table_folder_id,
                delivery_format=backend_name,
                last_update_date=effective_date,
                data_path=table_data_path,
                nb_row=nb_row,
                schema_signature=current_signature,
                last_update_timestamp=effective_timestamp,
                _seen=True,
            )
            catalog.dataset.add(dataset)

            var_id_mapping = build_variable_ids(table_vars, dataset.id)
            if not schema_only:
                catalog.modality_manager.assign_from_freq(
                    table_vars, freq_table, var_id_mapping
                )
            catalog.variable.add_all(table_vars)

            if schema_only:
                log_done(f"{table_name} ({len(table_vars)} vars)", q)
            else:
                log_done(f"{table_name} ({nb_row:,} rows, {len(table_vars)} vars)", q)

    # Close connection if we created it (string connection)
    if isinstance(connection, str):
        close_connection(con)

    datasets_added = catalog.dataset.count - datasets_before
    vars_added = catalog.variable.count - vars_before
    log_summary(datasets_added, vars_added, q, start_time, scan_errors)
