# Python API

## `Catalog`

```python
Catalog(
    *,
    app_path=None,
    metadata_path=None,
    depth="value",
    refresh=False,
    freq_threshold=100,
    csv_encoding=None,
    sample_size=100_000,
    preview_rows=100,
    csv_skip_copy=False,
    app_config=None,
    quiet=False,
    verbose=False,
    log_file=None,
)
```

| Attribute      | Type                              | Description                                        |
| -------------- | --------------------------------- | -------------------------------------------------- |
| app_path       | str \| Path \| None               | Load existing catalog for incremental scan         |
| metadata_path  | str \| Path \| list[str \| Path] \| None | Metadata source folder, database URI, or list of sources |
| depth          | "dataset" \| "variable" \| "stat" \| "value" | Default scan depth (default: "value")              |
| refresh        | bool                              | Force full rescan ignoring cache (default: False)  |
| freq_threshold | int                               | Max distinct values for frequency/enumeration detection. Strings above this threshold get pattern frequencies instead |
| csv_encoding   | str \| None                       | Default CSV encoding (utf-8, cp1252, etc.)         |
| sample_size    | int \| None                       | Default sample size for frequency/enumeration detection (default: 100_000) |
| preview_rows   | int \| Literal[False]             | Default max rows exported in dataset previews at `stat`/`value` depth (default: 100; 0 or false disables) |
| csv_skip_copy      | bool                              | Skip UTF-8 temp copy for local CSV (default: False)|
| app_config     | dict[str, str] \| None            | Key-value config for the web app                   |
| quiet          | bool                              | Suppress progress logging (default: False)         |
| verbose        | bool                              | Show full tracebacks on errors (default: False)    |
| log_file       | str \| Path \| None               | Write full scan log to file (truncated each run)   |
| folder         | Table[Folder]                     | Folder table (`.all()`, `.count`, `.get_by(...)`)  |
| dataset        | Table[Dataset]                    | Dataset table                                      |
| variable       | Table[Variable]                   | Variable table                                     |
| enumeration    | Table[Enumeration]                | Enumeration table                                  |
| value          | Table[Value]                      | Enumeration value table                            |
| frequency      | Table[Frequency]                  | Frequency table (computed)                         |
| organization   | Table[Organization]               | Organization table |
| tag            | Table[Tag]                        | Tag table                                          |
| doc            | Table[Doc]                        | Document table                                     |
| concept        | Table[Concept]                    | Business glossary concept table                    |
| config         | Table[Config]                     | Web app config key-value table                     |

## `Catalog.add_folder()`

```python
catalog.add_folder(
    path,
    metadata=None,
    *,
    depth=None,
    include=None,
    exclude=None,
    recursive=True,
    csv_encoding=None,
    sample_size=None,
    preview_rows=None,
    csv_skip_copy=None,
    storage_options=None,
    refresh=None,
    quiet=None,
    time_series=True,
    create_folders=True,
    on_unmatched="warn",
)
```

| Parameter       | Type                                      | Default  | Description                                   |
| --------------- | ----------------------------------------- | -------- | --------------------------------------------- |
| path            | str \| Path \| list[str \| Path]           | required | Directory or list of directories to scan      |
| metadata        | EntityMetadata \| None                    | None     | Identity, parent linkage, and metadata for the root folder |
| depth           | "dataset" \| "variable" \| "stat" \| "value" \| None | None     | Scan depth (uses catalog.depth if None)       |
| include         | list[str] \| None                         | None     | Glob patterns to include                      |
| exclude         | list[str] \| None                         | None     | Glob patterns to exclude                      |
| recursive       | bool                                      | True     | Scan subdirectories                           |
| csv_encoding    | str \| None                               | None     | Override CSV encoding                         |
| sample_size     | int \| None                               | None     | Sample rows for frequency/enumeration detection (overrides catalog) |
| preview_rows    | int \| Literal[False] \| None              | None     | Max preview rows for datasets found in this folder (overrides catalog; 0 or false disables) |
| csv_skip_copy       | bool \| None                              | None     | Skip UTF-8 temp copy (overrides catalog)      |
| storage_options | dict \| None                              | None     | Options for remote storage (passed to fsspec) |
| refresh         | bool \| None                              | None     | Force rescan (overrides catalog setting)      |
| quiet           | bool \| None                              | None     | Override catalog quiet setting                |
| time_series     | bool                                      | True     | Group files with temporal patterns            |
| create_folders  | bool                                      | True     | If False, do not create folders from disk; rely on `metadata_path` for structure (metadata-first) |
| on_unmatched    | "skip" \| "warn" \| "error"               | "warn"   | Policy when a scanned file has no metadata match (only when `create_folders=False`) |

## `Catalog.add_dataset()`

```python
catalog.add_dataset(
    path,
    *,
    metadata=None,
    depth=None,
    csv_encoding=None,
    sample_size=None,
    preview_rows=None,
    csv_skip_copy=None,
    storage_options=None,
    refresh=None,
    quiet=None,
)
```

| Parameter       | Type                                      | Default  | Description                                   |
| --------------- | ----------------------------------------- | -------- | --------------------------------------------- |
| path            | str \| Path \| list[str \| Path]           | required | File(s) or partitioned directory (local/remote) |
| metadata        | EntityMetadata \| None                    | None     | Dataset identity, parent linkage, and metadata |
| depth           | "dataset" \| "variable" \| "stat" \| "value" \| None | None     | Scan depth (uses catalog.depth if None)       |
| csv_encoding    | str \| None                               | None     | Override CSV encoding                         |
| sample_size     | int \| None                               | None     | Sample rows for frequency/enumeration detection (overrides catalog) |
| preview_rows    | int \| Literal[False] \| None              | None     | Max preview rows for this dataset (overrides catalog; 0 or false disables) |
| csv_skip_copy       | bool \| None                              | None     | Skip UTF-8 temp copy (overrides catalog)      |
| storage_options | dict \| None                              | None     | Options for remote storage (passed to fsspec) |
| refresh         | bool \| None                              | None     | Force rescan (overrides catalog setting)      |
| quiet           | bool \| None                              | None     | Override catalog quiet setting                |

## `Catalog.add_database()`

```python
catalog.add_database(
    connection,
    metadata=None,
    *,
    depth=None,
    schema=None,
    include=None,
    exclude=None,
    sample_size=None,
    preview_rows=None,
    group_by_prefix=True,
    prefix_min_tables=2,
    time_series=True,
    storage_options=None,
    refresh=None,
    quiet=None,
    oracle_client_path=None,
    ssh_tunnel=None,
)
```

| Parameter          | Type                                            | Default  | Description                                |
| ------------------ | ----------------------------------------------- | -------- | ------------------------------------------ |
| connection         | str \| ibis.BaseBackend                          | required | Connection string or ibis backend object   |
| metadata           | EntityMetadata \| None                          | None     | Identity, parent linkage, and metadata for the root folder |
| depth              | \"dataset\" \| \"variable\" \| \"stat\" \| \"value\" \| None | None     | Scan depth (uses catalog.depth if None)    |
| schema             | str \| list[str] \| None                         | None     | Schema(s) to scan                          |
| include            | list[str] \| None                               | None     | Glob patterns matched against table names to include |
| exclude            | list[str] \| None                               | None     | Glob patterns matched against table names to exclude |
| sample_size        | int \| None                                     | None     | Sample rows for frequency/enumeration detection (overrides catalog) |
| preview_rows       | int \| Literal[False] \| None                    | None     | Max preview rows for scanned table datasets (overrides catalog; 0 or false disables) |
| group_by_prefix    | bool \| str                                     | True     | Group tables by prefix into subfolders     |
| prefix_min_tables  | int                                             | 2        | Min tables to form a prefix group          |
| time_series        | bool                                            | True     | Detect temporal table patterns             |
| storage_options    | dict \| None                                    | None     | Options for remote SQLite/GeoPackage       |
| refresh            | bool \| None                                    | None     | Force rescan (overrides catalog setting)   |
| quiet              | bool \| None                                    | None     | Override catalog quiet setting             |
| oracle_client_path | str \| None                                     | None     | Path to Oracle Instant Client libraries    |
| ssh_tunnel         | dict \| None                                    | None     | SSH tunnel config (host, user, port, etc.) |


## `Catalog.export_db()`

```python
catalog.export_db(
    output_dir=None,
    *,
    track_evolution=True,
    copy_assets=None,
    base_dir=None,
    quiet=None,
)
```

| Parameter       | Type             | Default | Description                                |
| --------------- | ---------------- | ------- | ------------------------------------------ |
| output_dir      | str \| Path \| None | None    | Output directory (uses app_path if None)   |
| track_evolution | bool             | True    | Track changes between exports              |
| copy_assets     | dict \| list[dict] \| None | None    | Copy extra local files/directories into the export using the same rules as `copy_assets()` |
| base_dir        | str \| Path \| None | None    | Base directory for relative `copy_assets.from` paths (defaults to current working directory) |
| quiet           | bool \| None     | None    | Override catalog quiet setting             |

Exports JSON metadata files. Calls `finalize()` automatically when data has been scanned.

## `Catalog.export_app()`

```python
catalog.export_app(
    output_dir=None,
    *,
    open_browser=False,
    track_evolution=True,
    update_app=False,
    copy_assets=None,
    base_dir=None,
    quiet=None,
)
```

| Parameter       | Type                | Default | Description                                |
| --------------- | ------------------- | ------- | ------------------------------------------ |
| output_dir      | str \| Path \| None | None    | Output directory (uses app_path if None)   |
| open_browser    | bool                | False   | Open app in browser after export           |
| track_evolution | bool                | True    | Track changes between exports              |
| update_app      | bool                | False   | Refresh bundled front-end app files when the app already exists |
| copy_assets     | dict \| list[dict] \| None | None    | Copy extra local files/directories into the exported app using the same rules as `copy_assets()` |
| base_dir        | str \| Path \| None | None    | Base directory for relative `copy_assets.from` paths (defaults to current working directory) |
| quiet           | bool \| None        | None    | Override catalog quiet setting             |

Exports complete standalone datannur app with data. Uses `app_path` by default if set at init. Existing apps update `data/db` by default; pass `update_app=True` to refresh bundled front-end files.

## `Catalog.finalize()`

```python
catalog.finalize()
```

Advanced lifecycle method. Removes entities no longer seen during scan.

In normal usage, you usually do not need to call it directly: `export_db()` and `export_app()` call it automatically after scanning.


## `run_config()`

```python
from datannurpy import run_config

catalog = run_config(path)
```

| Parameter | Type       | Default  | Description |
| --------- | ---------- | -------- | ----------- |
| path      | str \| Path | required | YAML configuration file to load and execute |

Runs a `catalog.yml` workflow and returns the resulting `Catalog`.

## `copy_assets()`

```python
from datannurpy import copy_assets

copy_assets(output_dir, rules, *, base_dir=None, quiet=False)
```

| Parameter  | Type                          | Default | Description |
| ---------- | ----------------------------- | ------- | ----------- |
| output_dir | str \| Path                    | required | Export directory to populate |
| rules      | dict \| list[dict]             | required | Copy rules using the same shape as YAML `copy_assets` |
| base_dir   | str \| Path \| None           | None    | Base directory for relative `from` paths (defaults to current working directory) |
| quiet      | bool                           | False   | Suppress copy progress logging |

Each rule accepts `from`, `to`, optional `include`, and optional `clean`.

`Catalog.export_db()` and `Catalog.export_app()` also accept `copy_assets=` and `base_dir=` as convenience wrappers around this helper.

## `EntityMetadata`

```python
EntityMetadata(
    id=None,
    parent_id=None,
    manager_id=None,
    owner_id=None,
    tag_ids=None,
    doc_ids=None,
    name=None,
    description=None,
    license=None,
    type=None,
    link=None,
    localisation=None,
    start_date=None,
    end_date=None,
    updating_each=None,
    no_more_update=None,
)
```

| Parameter      | Type              | Description |
| -------------- | ----------------- | ----------- |
| id             | str \| None       | Explicit entity ID. If omitted, scan-derived defaults are used. |
| parent_id      | str \| None       | Parent folder ID (`Folder.parent_id` for folders, `Dataset.folder_id` for datasets). |
| manager_id     | str \| None       | Managing organization ID. |
| owner_id       | str \| None       | Owning organization ID. |
| tag_ids        | list[str] \| None | Related tag IDs. |
| doc_ids        | list[str] \| None | Related document IDs. |
| name           | str \| None       | Display name. |
| description    | str \| None       | Description text. |
| license        | str \| None       | License string. |
| type           | str \| None       | Entity type/category. |
| link           | str \| None       | External reference URL. |
| localisation   | str \| None       | Geographic coverage. |
| start_date     | str \| None       | Covered period start. |
| end_date       | str \| None       | Covered period end. |
| updating_each  | str \| None       | Update frequency. |
| no_more_update | str \| None       | Marker that no further updates are expected. |

In YAML configs, the same metadata is usually written as top-level keys on an `add` entry:

```yaml
add:
  - folder: ./data
    id: source
    name: Source data
    description: Curated files used by the analytics team.

  - dataset: ./data/sales.csv
    id: source---sales
    folder_id: source
    name: Sales
    description: Monthly sales by product and region.
```

`EntityMetadata` is the Python API equivalent of those YAML metadata keys.

## ID helpers

```python
from datannurpy import sanitize_id, build_dataset_id, build_variable_id
```

| Function                                        | Description                | Example                                                     |
| ----------------------------------------------- | -------------------------- | ----------------------------------------------------------- |
| sanitize_id(s)                                  | Clean string for use as ID | "My File (v2)" → "My File _v2_"                          |
| build_dataset_id(folder_id, dataset_name)       | Build dataset ID           | (\"src\", \"sales\") → \"src---sales\"                      |
| build_variable_id(folder_id, dataset_name, var) | Build variable ID          | (\"src\", \"sales\", \"amount\") → \"src---sales---amount\" |
