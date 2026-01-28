"""Add database to catalog."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import ibis

from .utils import (
    build_variable_ids,
    get_prefix_folders,
    get_table_prefix,
    log_done,
    log_folder,
    log_section,
    log_start,
    log_summary,
    make_id,
    sanitize_id,
)
from .entities import Dataset, Folder
from .scanner.database import (
    connect,
    get_database_name,
    get_database_path,
    get_schemas_to_scan,
    list_tables,
    scan_table,
)

if TYPE_CHECKING:
    from .catalog import Catalog


def add_database(
    catalog: Catalog,
    connection: str | ibis.BaseBackend,
    folder: Folder | None = None,
    *,
    schema: str | None = None,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    infer_stats: bool = True,
    sample_size: int | None = None,
    group_by_prefix: bool | str = True,
    prefix_min_tables: int = 2,
    quiet: bool | None = None,
) -> None:
    """Scan a database and add its tables to the catalog."""
    q = quiet if quiet is not None else catalog.quiet
    # Connect to database
    con, backend_name = connect(connection)

    # Determine database name for folder
    db_name = get_database_name(connection, con, backend_name)

    start_time = log_section("add_database", f"{backend_name}://{db_name}", q)
    datasets_before = len(catalog.datasets)
    vars_before = len(catalog.variables)

    # Get timestamp for folder/dataset
    now_iso = datetime.now(tz=timezone.utc).strftime("%Y/%m/%d")

    # Determine schemas to scan
    schemas_to_scan = get_schemas_to_scan(con, schema, backend_name)

    # Create root folder for database
    if folder is None:
        root_folder_id = sanitize_id(db_name)
        folder = Folder(id=root_folder_id, name=db_name)
    else:
        root_folder_id = folder.id

    folder.last_update_date = now_iso
    folder.data_path = (
        get_database_path(connection, backend_name)
        if isinstance(connection, str)
        else None
    )
    folder.type = backend_name
    catalog.folders.append(folder)

    freq_threshold = catalog.freq_threshold if catalog.freq_threshold else None

    # Process each schema
    for schema_name in schemas_to_scan:
        # Determine folder for this schema
        if schema_name is not None and len(schemas_to_scan) > 1:
            log_folder(f"{schema_name} (schema)", q)
            # Multiple schemas: create sub-folder for each
            schema_folder_id = make_id(root_folder_id, sanitize_id(schema_name))
            schema_folder = Folder(
                id=schema_folder_id,
                name=schema_name,
                parent_id=root_folder_id,
                type="schema",
                last_update_date=now_iso,
            )
            catalog.folders.append(schema_folder)
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

                prefix_folder = Folder(
                    id=folder_id,
                    name=pf.prefix,
                    parent_id=parent_id,
                    type="table_prefix",
                    last_update_date=now_iso,
                )
                catalog.folders.append(prefix_folder)

        for table_name in tables:
            log_start(table_name, q)
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

            # Create dataset
            dataset = Dataset(
                id=dataset_id,
                name=table_name,
                folder_id=table_folder_id,
                delivery_format=backend_name,
                last_update_date=now_iso,
            )

            # Scan table
            table_vars, nb_row, freq_table = scan_table(
                con,
                table_name,
                schema=schema_name,
                infer_stats=infer_stats,
                freq_threshold=freq_threshold,
                sample_size=sample_size,
            )

            dataset.nb_row = nb_row
            catalog.datasets.append(dataset)
            var_id_mapping = build_variable_ids(table_vars, dataset.id)
            catalog.modality_manager.assign_from_freq(
                table_vars, freq_table, var_id_mapping
            )
            catalog.variables.extend(table_vars)
            log_done(f"{table_name} ({nb_row:,} rows, {len(table_vars)} vars)", q)

    datasets_added = len(catalog.datasets) - datasets_before
    vars_added = len(catalog.variables) - vars_before
    log_summary(datasets_added, vars_added, q, start_time)
