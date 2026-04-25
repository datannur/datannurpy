# Python API



All YAML features are also available programmatically via the Python API.

### `Catalog`

```python
Catalog(app_path=None, metadata_path=None, depth="value", refresh=False, freq_threshold=100, csv_encoding=None, sample_size=100_000, csv_skip_copy=False, app_config=None, quiet=False, verbose=False, log_file=None)
```

| Attribute      | Type                              | Description                                        |
| -------------- | --------------------------------- | -------------------------------------------------- |
| app_path       | str \| Path \| None               | Load existing catalog for incremental scan         |
| metadata_path  | str \| Path \| list \| None       | Metadata source folder, database URI, or list of sources |
| depth          | "dataset" \| "variable" \| "stat" \| "value" | Default scan depth (default: "value")              |
| refresh        | bool                              | Force full rescan ignoring cache (default: False)  |
| freq_threshold | int                               | Max distinct values for frequency/modality detection. Strings above this threshold get pattern frequencies instead |
| csv_encoding   | str \| None                       | Default CSV encoding (utf-8, cp1252, etc.)         |
| sample_size    | int \| None                       | Default sample size for stats (default: 100_000)   |
| csv_skip_copy      | bool                              | Skip UTF-8 temp copy for local CSV (default: False)|
| app_config     | dict[str, str] \| None            | Key-value config for the web app                   |
| quiet          | bool                              | Suppress progress logging (default: False)         |
| verbose        | bool                              | Show full tracebacks on errors (default: False)    |
| log_file       | str \| Path \| None               | Write full scan log to file (truncated each run)   |
| folder         | Table[Folder]                     | Folder table (`.all()`, `.count`, `.get_by(...)`)  |
| dataset        | Table[Dataset]                    | Dataset table                                      |
| variable       | Table[Variable]                   | Variable table                                     |
| modality       | Table[Modality]                   | Modality table                                     |
| value          | Table[Value]                      | Modality value table                               |
| freq           | Table[Freq]                       | Frequency table (computed)                         |
| institution    | Table[Institution]                | Institution table                                  |
| tag            | Table[Tag]                        | Tag table                                          |
| doc            | Table[Doc]                        | Document table                                     |
| concept        | Table[Concept]                    | Business glossary concept table                    |
| config         | Table[Config]                     | Web app config key-value table                     |

### `Catalog.add_folder()`

```python
catalog.add_folder(
    path,
    folder=None,
    *,
    depth=None,
    include=None,
    exclude=None,
    recursive=True,
    csv_encoding=None,
    sample_size=None,
    csv_skip_copy=None,
    storage_options=None,
    refresh=None,
    quiet=None,
    time_series=True,
    create_folders=True,
    on_unmatched="warn",
    id=None,
    name=None,
    description=None,
    manager_id=None,
    owner_id=None,
)
```

| Parameter       | Type                                      | Default  | Description                                   |
| --------------- | ----------------------------------------- | -------- | --------------------------------------------- |
| path            | str \| Path \| list[str \| Path]           | required | Directory or list of directories to scan      |
| folder          | Folder \| None                            | None     | Custom folder metadata                        |
| depth           | "dataset" \| "variable" \| "stat" \| "value" \| None | None     | Scan depth (uses catalog.depth if None)       |
| include         | list[str] \| None                         | None     | Glob patterns to include                      |
| exclude         | list[str] \| None                         | None     | Glob patterns to exclude                      |
| recursive       | bool                                      | True     | Scan subdirectories                           |
| csv_encoding    | str \| None                               | None     | Override CSV encoding                         |
| sample_size     | int \| None                               | None     | Sample rows for stats (overrides catalog)     |
| csv_skip_copy       | bool \| None                              | None     | Skip UTF-8 temp copy (overrides catalog)      |
| storage_options | dict \| None                              | None     | Options for remote storage (passed to fsspec) |
| refresh         | bool \| None                              | None     | Force rescan (overrides catalog setting)      |
| quiet           | bool \| None                              | None     | Override catalog quiet setting                |
| time_series     | bool                                      | True     | Group files with temporal patterns            |
| create_folders  | bool                                      | True     | If False, do not create folders from disk; rely on `metadata_path` for structure (metadata-first) |
| on_unmatched    | "skip" \| "warn" \| "error"               | "warn"   | Policy when a scanned file has no metadata match (only when `create_folders=False`) |
| id              | str \| None                               | None     | Override folder ID                            |
| name            | str \| None                               | None     | Override folder name                          |
| description     | str \| None                               | None     | Override folder description                   |
| manager_id      | str \| None                               | None     | Institution ID managing the folder            |
| owner_id        | str \| None                               | None     | Institution ID owning the folder              |

### `Catalog.add_dataset()`

```python
catalog.add_dataset(
    path,
    folder=None,
    *,
    folder_id=None,
    depth=None,
    csv_encoding=None,
    sample_size=None,
    csv_skip_copy=None,
    storage_options=None,
    refresh=None,
    quiet=None,
    name=None,
    description=None,
    ...,
)
```

| Parameter       | Type                                      | Default  | Description                                   |
| --------------- | ----------------------------------------- | -------- | --------------------------------------------- |
| path            | str \| Path \| list[str \| Path]           | required | File(s) or partitioned directory (local/remote) |
| folder          | Folder \| None                            | None     | Parent folder                                 |
| folder_id       | str \| None                               | None     | Parent folder ID (alternative to folder)      |
| depth           | "dataset" \| "variable" \| "stat" \| "value" \| None | None     | Scan depth (uses catalog.depth if None)       |
| csv_encoding    | str \| None                               | None     | Override CSV encoding                         |
| sample_size     | int \| None                               | None     | Sample rows for stats (overrides catalog)     |
| csv_skip_copy       | bool \| None                              | None     | Skip UTF-8 temp copy (overrides catalog)      |
| storage_options | dict \| None                              | None     | Options for remote storage (passed to fsspec) |
| refresh         | bool \| None                              | None     | Force rescan (overrides catalog setting)      |
| quiet           | bool \| None                              | None     | Override catalog quiet setting                |
| name            | str \| None                               | None     | Override dataset name                         |
| description     | str \| None                               | None     | Override dataset description                  |

Additional metadata parameters: `type`, `link`, `localisation`, `manager_id`, `owner_id`, `tag_ids`, `doc_ids`, `start_date`, `end_date`, `updating_each`, `no_more_update`

### `Catalog.add_database()`

```python
catalog.add_database(
    connection,
    folder=None,
    *,
    depth=None,
    schema=None,
    include=None,
    exclude=None,
    sample_size=None,
    group_by_prefix=True,
    prefix_min_tables=2,
    time_series=True,
    storage_options=None,
    refresh=None,
    quiet=None,
    oracle_client_path=None,
    ssh_tunnel=None,
    id=None,
    name=None,
    description=None,
    manager_id=None,
    owner_id=None,
)
```

| Parameter          | Type                                            | Default  | Description                                |
| ------------------ | ----------------------------------------------- | -------- | ------------------------------------------ |
| connection         | str \| ibis.BaseBackend                          | required | Connection string or ibis backend object   |
| folder             | Folder \| None                                  | None     | Custom root folder                         |
| depth              | \"dataset\" \| \"variable\" \| \"stat\" \| \"value\" \| None | None     | Scan depth (uses catalog.depth if None)    |
| schema             | str \| list[str] \| None                         | None     | Schema(s) to scan                          |
| include            | list[str] \| None                               | None     | Table name patterns to include             |
| exclude            | list[str] \| None                               | None     | Table name patterns to exclude             |
| sample_size        | int \| None                                     | None     | Sample rows for stats (overrides catalog)  |
| group_by_prefix    | bool \| str                                     | True     | Group tables by prefix into subfolders     |
| prefix_min_tables  | int                                             | 2        | Min tables to form a prefix group          |
| time_series        | bool                                            | True     | Detect temporal table patterns             |
| storage_options    | dict \| None                                    | None     | Options for remote SQLite/GeoPackage       |
| refresh            | bool \| None                                    | None     | Force rescan (overrides catalog setting)   |
| quiet              | bool \| None                                    | None     | Override catalog quiet setting             |
| oracle_client_path | str \| None                                     | None     | Path to Oracle Instant Client libraries    |
| ssh_tunnel         | dict \| None                                    | None     | SSH tunnel config (host, user, port, etc.) |
| id                 | str \| None                                     | None     | Override folder ID                         |
| name               | str \| None                                     | None     | Override folder name                       |
| description        | str \| None                                     | None     | Override folder description                |
| manager_id         | str \| None                                     | None     | Institution ID managing the folder         |
| owner_id           | str \| None                                     | None     | Institution ID owning the folder           |

### `Catalog.export_db()`

```python
catalog.export_db(output_dir=None, track_evolution=True, quiet=None)
```

| Parameter       | Type             | Default | Description                                |
| --------------- | ---------------- | ------- | ------------------------------------------ |
| output_dir      | str \| Path \| None | None    | Output directory (uses app_path if None)   |
| track_evolution | bool             | True    | Track changes between exports              |
| quiet           | bool \| None     | None    | Override catalog quiet setting             |

Exports JSON metadata files. Calls `finalize()` automatically when data has been scanned.

### `Catalog.finalize()`

```python
catalog.finalize()
```

Removes entities no longer seen during scan. Called automatically by `export_db()`/`export_app()`.

### `Catalog.export_app()`

```python
catalog.export_app(output_dir=None, open_browser=False, track_evolution=True, quiet=None)
```

| Parameter       | Type                | Default | Description                                |
| --------------- | ------------------- | ------- | ------------------------------------------ |
| output_dir      | str \| Path \| None | None    | Output directory (uses app_path if None)   |
| open_browser    | bool                | False   | Open app in browser after export           |
| track_evolution | bool                | True    | Track changes between exports              |
| quiet           | bool \| None        | None    | Override catalog quiet setting             |

Exports complete standalone datannur app with data. Uses `app_path` by default if set at init.

### `Folder`

```python
Folder(id, parent_id=None, tag_ids=[], doc_ids=[], name=None, description=None, type=None, data_path=None)
```

| Parameter   | Type        | Description                  |
| ----------- | ----------- | ---------------------------- |
| id          | str         | Unique identifier            |
| parent_id   | str \| None | Parent folder ID             |
| tag_ids     | list[str]   | Associated tag IDs           |
| doc_ids     | list[str]   | Associated document IDs      |
| name        | str \| None | Display name                 |
| description | str \| None | Description                  |
| type        | str \| None | Folder type                  |
| data_path   | str \| None | Path to the data source      |

### ID helpers

```python
from datannurpy import sanitize_id, build_dataset_id, build_variable_id
```

| Function                                        | Description                | Example                                                     |
| ----------------------------------------------- | -------------------------- | ----------------------------------------------------------- |
| sanitize_id(s)                                  | Clean string for use as ID | "My File (v2)" → "My File _v2_"                          |
| build_dataset_id(folder_id, dataset_name)       | Build dataset ID           | (\"src\", \"sales\") → \"src---sales\"                      |
| build_variable_id(folder_id, dataset_name, var) | Build variable ID          | (\"src\", \"sales\", \"amount\") → \"src---sales---amount\" |
