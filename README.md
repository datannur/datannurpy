# datannurpy

[![PyPI version](https://img.shields.io/pypi/v/datannurpy.svg)](https://pypi.org/project/datannurpy/)
[![Python](https://img.shields.io/badge/python-≥3.9-blue.svg)](https://pypi.org/project/datannurpy/)
[![CI](https://github.com/datannur/datannurpy/actions/workflows/ci.yml/badge.svg)](https://github.com/datannur/datannurpy/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/datannur/datannurpy/branch/main/graph/badge.svg)](https://codecov.io/gh/datannur/datannurpy)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Python library for [datannur](https://github.com/datannur/datannur) catalog metadata management.

## Supported formats

A lightweight catalog compatible with most data sources:

| Category        | Formats                                               |
| --------------- | ----------------------------------------------------- |
| **Flat files**  | CSV, Excel (.xlsx, .xls)                              |
| **Columnar**    | Parquet, Delta Lake, Apache Iceberg, Hive partitioned |
| **Statistical** | SAS (.sas7bdat), SPSS (.sav), Stata (.dta)            |
| **Databases**   | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |

All formats support automatic schema inference and statistics computation.

## Installation

```bash
pip install datannurpy
```

### Optional extras

```bash
# Databases
pip install datannurpy[postgres]  # PostgreSQL
pip install datannurpy[mysql]     # MySQL
pip install datannurpy[oracle]    # Oracle
pip install datannurpy[mssql]     # SQL Server

# File formats
pip install datannurpy[stat]      # SAS, SPSS, Stata
pip install datannurpy[delta]     # Delta Lake metadata extraction
pip install datannurpy[iceberg]   # Apache Iceberg metadata extraction

# Cloud storage
pip install datannurpy[s3]        # Amazon S3
pip install datannurpy[azure]     # Azure Blob Storage
pip install datannurpy[gcs]       # Google Cloud Storage
pip install datannurpy[cloud]     # All cloud providers

# Multiple extras
pip install datannurpy[postgres,stat,delta]
```

**SQL Server note:** Requires an ODBC driver on the system:

- macOS: `brew install unixodbc freetds`
- Linux: `apt install unixodbc-dev tdsodbc`
- Windows: [Microsoft ODBC Driver](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)

## Quick start

```python
from datannurpy import Catalog

catalog = Catalog()
catalog.add_folder("./data", include=["*.csv", "*.xlsx", "*.parquet"])
catalog.add_database("sqlite:///mydb.sqlite")
catalog.export_app("./my-catalog", open_browser=True)
```

## Configuration file

Alternative to Python scripts - define the catalog in YAML:

```yaml
# catalog.yml
app_path: ./my-catalog
refresh: true

add:
  - type: folder
    path: ./data
    include: ["*.csv", "*.parquet"]

  - type: database
    uri: sqlite:///mydb.sqlite

  - type: metadata
    path: ./metadata

export_app:
  open_browser: true
```

Run with:

```bash
python -m datannurpy catalog.yml
```

## Incremental scan

Re-run with the same `db_path` to only rescan changed files (compares mtime) or tables (compares schema + row count):

```python
catalog = Catalog(db_path="./my-catalog")
catalog.add_folder("./data")  # skips unchanged files
catalog.export_db()           # removes deleted entities, exports to db_path
```

Use `refresh=True` to force a full rescan.

## Evolution tracking

Changes between exports are automatically tracked in `evolution.json`:

- **add**: new folder, dataset, variable, modality, etc.
- **update**: modified field (shows old and new value)
- **delete**: removed entity

Cascade filtering: when a parent entity is added or deleted, its children are automatically filtered out to reduce noise. For example, adding a new dataset won't generate separate entries for each variable.

Disable tracking with `track_evolution=False`:

```python
catalog.export_db(track_evolution=False)
```

## Scanning files

```python
from datannurpy import Catalog, Folder

catalog = Catalog()

# Scan a folder (CSV, Excel, SAS)
catalog.add_folder("./data")

# With custom folder metadata
catalog.add_folder("./data", Folder(id="prod", name="Production"))

# With filtering options
catalog.add_folder(
    "./data",
    include=["*.csv", "*.xlsx"],
    exclude=["**/tmp/**"],
    recursive=True,
    infer_stats=True,
    csv_encoding="utf-8",  # or "cp1252", "iso-8859-1" (auto-detected by default)
)

# Add a single file
catalog.add_dataset("./data/sales.csv")
```

## Parquet formats

Supports simple Parquet files and partitioned datasets (Delta, Hive, Iceberg):

```python
# add_folder auto-detects all formats
catalog.add_folder("./data")  # scans *.parquet + Delta/Hive/Iceberg directories

# add_dataset for a single partitioned directory with metadata override
catalog.add_dataset(
    "./data/sales_delta",
    name="Sales Data",
    description="Monthly sales",
    folder=Folder(id="sales", name="Sales"),
)
```

With extras `[delta]` and `[iceberg]`, metadata (name, description, column docs) is extracted when available.

## Remote storage

Scan files on SFTP servers or cloud storage (S3, Azure, GCS):

```python
from datannurpy import Catalog, Folder

catalog = Catalog()

# SFTP (paramiko included by default)
catalog.add_folder(
    "sftp://user@host/path/to/data",
    storage_options={"password": "secret"},  # or key_filename="/path/to/key"
)

# Amazon S3 (requires: pip install datannurpy[s3])
catalog.add_folder(
    "s3://my-bucket/data",
    storage_options={"key": "...", "secret": "..."},
)

# Azure Blob (requires: pip install datannurpy[azure])
catalog.add_folder(
    "az://container/data",
    storage_options={"account_name": "...", "account_key": "..."},
)

# Google Cloud Storage (requires: pip install datannurpy[gcs])
catalog.add_folder(
    "gs://my-bucket/data",
    storage_options={"token": "/path/to/credentials.json"},
)

# Single remote file
catalog.add_dataset("s3://my-bucket/data/sales.parquet", storage_options={...})

# Remote SQLite / GeoPackage database
catalog.add_database("sftp://host/path/to/db.sqlite", storage_options={...})
catalog.add_database("s3://bucket/geodata.gpkg", storage_options={...})
```

The `storage_options` dict is passed directly to [fsspec](https://filesystem-spec.readthedocs.io/). See provider documentation for available options:

- [SFTP](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.sftp.SFTPFileSystem)
- [S3](https://s3fs.readthedocs.io/en/latest/)
- [Azure](https://github.com/fsspec/adlfs)
- [GCS](https://gcsfs.readthedocs.io/en/latest/)

## Scanning databases

```python
# SQLite / GeoPackage
catalog.add_database("sqlite:///path/to/db.sqlite")
catalog.add_database("sqlite:///path/to/geodata.gpkg")  # GeoPackage is SQLite

# PostgreSQL / MySQL / Oracle / SQL Server
catalog.add_database("postgresql://user:pass@host:5432/mydb")
catalog.add_database("mysql://user:pass@host:3306/mydb")
catalog.add_database("oracle://user:pass@host:1521/service_name")
catalog.add_database("mssql://user:pass@host:1433/mydb")

# SSL/TLS connections
catalog.add_database("postgresql://user:pass@host/db?sslmode=require")

# SQL Server with Windows auth (requires proper Kerberos setup)
catalog.add_database("mssql://host/db?TrustedConnection=yes")

# With options
catalog.add_database(
    "postgresql://localhost/mydb",
    schema="public",
    include=["sales_*"],
    exclude=["*_tmp"],
    sample_size=10000,  # limit rows for stats on large tables
    group_by_prefix=True,  # group tables by common prefix (default)
    prefix_min_tables=2,  # minimum tables to form a group
)
```

## Manual metadata

Load manually curated metadata from files or a database:

```python
# Load from a folder containing metadata files
catalog.add_metadata("./metadata")

# Load from a database
catalog.add_metadata("sqlite:///metadata.db")
```

Can be used alone or combined with auto-scanned metadata (`add_folder`, `add_database`).

**Expected structure:** One file/table per entity, named after the entity type:

```
metadata/
├── variable.csv      # Variables (descriptions, tags...)
├── dataset.xlsx      # Datasets
├── institution.json  # Institutions (owners, managers)
├── tag.csv           # Tags
├── modality.csv      # Modalities
├── value.csv         # Modality values
└── ...
```

**Supported formats:** CSV, Excel (.xlsx), JSON, SAS (.sas7bdat), or database tables.

**File format:** Standard tabular structure following [datannur schemas](https://github.com/datannur/datannur/tree/main/public/schemas). The `id` column is required for most entities (except `value` and `freq`).

```csv
# variable.csv
id,description,tag_ids
source---employees_csv---salary,"Monthly gross salary in euros","finance,hr"
source---employees_csv---department,"Department code","hr"
```

**Merge behavior:**

- Existing entities are updated (manual values override auto-scanned values)
- New entities are created
- List fields (`tag_ids`, `doc_ids`, etc.) are merged

**Helper functions** for building IDs in preprocessing scripts:

```python
from datannurpy import sanitize_id, build_dataset_id, build_variable_id

sanitize_id("My File (v2)")  # → "My_File_v2"
build_dataset_id("source", "employees_csv")  # → "source---employees_csv"
build_variable_id("source", "employees_csv", "salary")  # → "source---employees_csv---salary"
```

## Output

```python
# JSON metadata only (for existing datannur instance)
catalog.export_db("./output")

# Complete standalone app
catalog.export_app("./my-catalog", open_browser=True)
```

## API Reference

### `Catalog`

```python
Catalog(app_path=None, depth="full", refresh=False, freq_threshold=100, csv_encoding=None, quiet=False)
```

| Attribute      | Type                              | Description                                        |
| -------------- | --------------------------------- | -------------------------------------------------- |
| app_path       | str \| None                       | Load existing catalog for incremental scan         |
| depth          | "structure" \| "schema" \| "full" | Default scan depth for add_folder                  |
| refresh        | bool                              | Force full rescan ignoring cache (default: False)  |
| freq_threshold | int                               | Max distinct values for modality detection (0=off) |
| csv_encoding   | str \| None                       | Default CSV encoding (utf-8, cp1252, etc.)         |
| quiet          | bool                              | Suppress progress logging (default: False)         |
| folders        | list[Folder]                      | All folders in catalog                             |
| datasets       | list[Dataset]                     | All datasets in catalog                            |
| variables      | list[Variable]                    | All variables in catalog                           |
| modalities     | list[Modality]                    | All modalities in catalog                          |

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
    infer_stats=True,
    csv_encoding=None,
    storage_options=None,
    refresh=None,
    quiet=None,
    time_series=True,
)
```

| Parameter       | Type                                      | Default  | Description                                   |
| --------------- | ----------------------------------------- | -------- | --------------------------------------------- |
| path            | str \| Path                               | required | Directory to scan (local or remote URL)       |
| folder          | Folder \| None                            | None     | Custom folder metadata                        |
| depth           | "structure" \| "schema" \| "full" \| None | None     | Scan depth (uses catalog.depth if None)       |
| include         | list[str] \| None                         | None     | Glob patterns to include                      |
| exclude         | list[str] \| None                         | None     | Glob patterns to exclude                      |
| recursive       | bool                                      | True     | Scan subdirectories                           |
| infer_stats     | bool                                      | True     | Compute distinct/missing/duplicate counts     |
| csv_encoding    | str \| None                               | None     | Override CSV encoding                         |
| storage_options | dict \| None                              | None     | Options for remote storage (passed to fsspec) |
| refresh         | bool \| None                              | None     | Force rescan (overrides catalog setting)      |
| quiet           | bool \| None                              | None     | Override catalog quiet setting                |
| time_series     | bool                                      | True     | Group files with temporal patterns            |

**Time series detection:**

When `time_series=True` (default), files with temporal patterns in their names are automatically grouped into a single dataset:

```
data/
├── enquete_2020.csv    ─┐
├── enquete_2021.csv     ├─→ Single dataset "enquete" with nb_files=3
├── enquete_2022.csv    ─┘
└── reference.csv       ─→ Separate dataset "reference"
```

Detected patterns: year (`2024`), quarter (`2024Q1`, `2024T2`), month (`2024-03`, `202403`), date (`2024-03-15`).

The resulting dataset includes:

- `nb_files`: number of files in the series
- `start_date` / `end_date`: temporal coverage
- Variables track their own `start_date` / `end_date` based on presence across periods

Set `time_series=False` to treat each file as a separate dataset.

**Depth levels:**

| depth     | Output                                       |
| --------- | -------------------------------------------- |
| structure | Folders, datasets (format, mtime, path only) |
| schema    | + Variables (names, types)                   |
| full      | + Row count, stats, modalities               |

### `Catalog.add_dataset()`

```python
catalog.add_dataset(
    path,
    folder=None,
    *,
    folder_id=None,
    depth=None,
    infer_stats=True,
    csv_encoding=None,
    storage_options=None,
    refresh=None,
    quiet=None,
    id=None,
    name=None,
    description=None,
    ...,
)
```

| Parameter       | Type                                      | Default  | Description                                   |
| --------------- | ----------------------------------------- | -------- | --------------------------------------------- |
| path            | str \| Path                               | required | File or partitioned directory (local/remote)  |
| folder          | Folder \| None                            | None     | Parent folder                                 |
| folder_id       | str \| None                               | None     | Parent folder ID (alternative to folder)      |
| depth           | "structure" \| "schema" \| "full" \| None | None     | Scan depth (uses catalog.depth if None)       |
| infer_stats     | bool                                      | True     | Compute statistics                            |
| csv_encoding    | str \| None                               | None     | Override CSV encoding                         |
| storage_options | dict \| None                              | None     | Options for remote storage (passed to fsspec) |
| refresh         | bool \| None                              | None     | Force rescan (overrides catalog setting)      |
| quiet           | bool \| None                              | None     | Override catalog quiet setting                |
| id              | str \| None                               | None     | Override dataset ID                           |
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
    infer_stats=True,
    sample_size=None,
    group_by_prefix=True,
    prefix_min_tables=2,
    storage_options=None,
    refresh=None,
    quiet=None,
)
```

| Parameter         | Type                                            | Default  | Description                              |
| ----------------- | ----------------------------------------------- | -------- | ---------------------------------------- |
| connection        | str                                             | required | Connection string (see formats below)    |
| folder            | Folder \| None                                  | None     | Custom root folder                       |
| depth             | \"structure\" \| \"schema\" \| \"full\" \| None | None     | Scan depth (uses catalog.depth if None)  |
| schema            | str \| None                                     | None     | Specific schema to scan                  |
| include           | list[str] \| None                               | None     | Table name patterns to include           |
| exclude           | list[str] \| None                               | None     | Table name patterns to exclude           |
| infer_stats       | bool                                            | True     | Compute column statistics                |
| sample_size       | int \| None                                     | None     | Limit rows for stats (large tables)      |
| group_by_prefix   | bool \| str                                     | True     | Group tables by prefix into subfolders   |
| prefix_min_tables | int                                             | 2        | Min tables to form a prefix group        |
| storage_options   | dict \| None                                    | None     | Options for remote SQLite/GeoPackage     |
| refresh           | bool \| None                                    | None     | Force rescan (overrides catalog setting) |
| quiet             | bool \| None                                    | None     | Override catalog quiet setting           |

**Connection string formats:**

- SQLite: `sqlite:///path/to/db.sqlite` or `sftp://host/path/db.sqlite` (remote)
- PostgreSQL: `postgresql://user:pass@host:5432/database`
- MySQL: `mysql://user:pass@host:3306/database`
- Oracle: `oracle://user:pass@host:1521/service_name`
- SQL Server: `mssql://user:pass@host:1433/database`

### `Catalog.add_metadata()`

```python
catalog.add_metadata(path, depth=None, quiet=None)
```

| Parameter | Type                                      | Default  | Description                                  |
| --------- | ----------------------------------------- | -------- | -------------------------------------------- |
| path      | str \| Path                               | required | Folder or database containing metadata files |
| depth     | "structure" \| "schema" \| "full" \| None | None     | Filter which entities to load                |
| quiet     | bool \| None                              | None     | Override catalog quiet setting               |

**Supported entity files/tables:** `folder`, `dataset`, `variable`, `modality`, `value`, `freq`, `institution`, `tag`, `doc`

### `Catalog.export_db()`

```python
catalog.export_db(output_dir=None, quiet=None)
```

Exports JSON metadata files. Uses `db_path` by default if set at init.

### `Catalog.finalize()`

```python
catalog.finalize()
```

Removes entities no longer seen during scan. Called automatically by `export_db()`/`export_app()`.

### `Catalog.export_app()`

```python
catalog.export_app(output_dir=None, open_browser=False, quiet=None)
```

Exports complete standalone datannur app with data. Uses `db_path` by default if set at init.

### `Folder`

```python
Folder(id, name=None, description=None, parent_id=None, type=None, data_path=None)
```

| Parameter   | Type        | Description       |
| ----------- | ----------- | ----------------- |
| id          | str         | Unique identifier |
| name        | str \| None | Display name      |
| description | str \| None | Description       |
| parent_id   | str \| None | Parent folder ID  |

### ID helpers

```python
from datannurpy import sanitize_id, build_dataset_id, build_variable_id
```

| Function                                        | Description                | Example                                                     |
| ----------------------------------------------- | -------------------------- | ----------------------------------------------------------- |
| sanitize_id(s)                                  | Clean string for use as ID | \"My File (v2)\" → \"My_File_v2\"                           |
| build_dataset_id(folder_id, dataset_name)       | Build dataset ID           | (\"src\", \"sales\") → \"src---sales\"                      |
| build_variable_id(folder_id, dataset_name, var) | Build variable ID          | (\"src\", \"sales\", \"amount\") → \"src---sales---amount\" |

## License

MIT License - see the [LICENSE](LICENSE) file for details.
