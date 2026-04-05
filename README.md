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
verbose: true          # show full tracebacks on errors
log_file: ./errors.log  # write tracebacks to file

add:
  - type: folder
    path: ./data
    include: ["*.csv", "*.parquet"]

  - type: dataset
    path: ./data/sales.parquet
    name: Sales Data
    description: Monthly sales figures

  - type: database
    uri: sqlite:///mydb.sqlite

  - type: metadata
    path: ./metadata

export_app:
  open_browser: true
```

Environment variables (`$VAR` or `${VAR}`) are expanded in all values. Define reusable values with `env:`, load secrets from `env_file`, or place a `.env` next to the YAML:

```yaml
env:
  data_dir: /shared/data
  db_host: db.example.com
env_file: /secure/path/.env # secrets: DB_USER, DB_PASSWORD

add:
  - type: folder
    path: ${data_dir}/sales
  - type: folder
    path: ${data_dir}/hr
  - type: database
    uri: oracle://${DB_USER}:${DB_PASSWORD}@${db_host}:1521/ORCL
```

SSH tunnel support in YAML:

```yaml
add:
  - type: database
    uri: mysql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}/${DB_NAME}
    ssh_tunnel:
      host: ${SSH_HOST}
      user: ${SSH_USER}
```

Priority: system env vars > `env_file` / `.env` > `env:` section.

Run with:

```bash
python -m datannurpy catalog.yml
```

## Incremental scan

Re-run with the same `app_path` to only rescan changed files (compares mtime) or tables (compares schema + row count):

```python
catalog = Catalog(app_path="./my-catalog")
catalog.add_folder("./data")  # skips unchanged files
catalog.export_app()          # removes deleted entities, exports
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

# Multiple folders with shared options
catalog.add_folder(["./data/sales", "./data/hr"], include=["*.csv"])

# Add a single file
catalog.add_dataset("./data/sales.csv")

# Multiple files
catalog.add_dataset(["./data/sales.csv", "./data/products.csv"])
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
    sample_size=10000,         # override catalog default (100_000)
    group_by_prefix=True,  # group tables by common prefix (default)
    prefix_min_tables=2,  # minimum tables to form a group
)

# Multiple schemas with shared options
catalog.add_database(
    "postgresql://localhost/mydb",
    schema=["public", "sales", "hr"],
    infer_stats=True,
)

# SSH tunnel (for databases behind a firewall)
catalog.add_database(
    "mysql://user:pass@dbhost/mydb",
    ssh_tunnel={"host": "ssh.example.com", "user": "sshuser"},
)
# Also supports: port, password, key_file
catalog.add_database(
    "postgresql://user:pass@dbhost/mydb",
    ssh_tunnel={
        "host": "bastion.example.com",
        "port": 2222,
        "user": "admin",
        "key_file": "~/.ssh/id_rsa",
    },
)
```

**Database metadata enrichment:**

When `depth="schema"` or `"full"` (default), `add_database` automatically extracts structural metadata from system catalogs:

| Metadata                | Target field          | Backends           |
| ----------------------- | --------------------- | ------------------ |
| Primary keys            | `Variable.key`        | All 6              |
| Foreign keys            | `Variable.fk_var_id`  | All 6              |
| Table/column comments   | `description`         | All except SQLite  |
| NOT NULL, UNIQUE, INDEX | Auto tags (`db---*`)  | All 6              |
| Auto-increment          | Auto tag              | All 6              |

This metadata is always refreshed, even when table data is unchanged (cache hit).

## Sampling

By default, `Catalog` sets `sample_size=100_000`. All methods (`add_folder`, `add_dataset`, `add_database`) inherit this value. Override per-method with an explicit int, or pass `sample_size=None` to disable sampling for a single call:

```python
catalog = Catalog(sample_size=100_000)              # default
catalog.add_folder("./data")                        # inherits 100_000
catalog.add_folder("./small", sample_size=None)     # no sampling
catalog.add_database("postgresql://localhost/mydb", sample_size=50_000)  # override
```

To disable sampling globally, pass `sample_size=None` to the `Catalog`:

```python
catalog = Catalog(sample_size=None)
```

When a dataset has more rows than `sample_size`, a uniform random sample is used for frequency counts and modality detection. All other statistics (`nb_row`, `nb_missing`, `nb_distinct`, `min`, `max`, `mean`, `std`) are computed on the full dataset.

The actual number of sampled rows is recorded in `Dataset.sample_size` (`None` when no sampling was applied).

## CSV options

Use `csv_skip_copy=True` on the `Catalog` to avoid the UTF-8 temp copy when files are already local and UTF-8 (auto-fallback if encoding detection fails):

```python
catalog = Catalog(app_path="./output", csv_skip_copy=True)
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

**Ordering:** `add_metadata` should be called **after** `add_folder`, `add_dataset`, and `add_database` so manual values take precedence over auto-scanned metadata. In YAML configuration, this is enforced automatically regardless of declaration order. In Python, the caller controls the execution order.

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
Catalog(app_path=None, depth="full", refresh=False, freq_threshold=100, csv_encoding=None, sample_size=100_000, csv_skip_copy=False, app_config=None, quiet=False, verbose=False, log_file=None)
```

| Attribute      | Type                              | Description                                        |
| -------------- | --------------------------------- | -------------------------------------------------- |
| app_path       | str \| None                       | Load existing catalog for incremental scan         |
| depth          | "structure" \| "schema" \| "full" | Default scan depth for add_folder                  |
| refresh        | bool                              | Force full rescan ignoring cache (default: False)  |
| freq_threshold | int                               | Max distinct values for modality detection (0=off) |
| csv_encoding   | str \| None                       | Default CSV encoding (utf-8, cp1252, etc.)         |
| sample_size    | int \| None                       | Default sample size for stats (default: 100_000)   |
| csv_skip_copy      | bool                              | Skip UTF-8 temp copy for local CSV (default: False)|
| app_config     | dict[str, str] \| None            | Key-value config for the web app (see below)       |
| quiet          | bool                              | Suppress progress logging (default: False)         |
| verbose        | bool                              | Show full tracebacks on errors (default: False)    |
| log_file       | str \| Path \| None               | Write error tracebacks to file (truncated each run)|
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
    sample_size=None,
    csv_skip_copy=None,
    storage_options=None,
    refresh=None,
    quiet=None,
    time_series=True,
)
```

| Parameter       | Type                                      | Default  | Description                                   |
| --------------- | ----------------------------------------- | -------- | --------------------------------------------- |
| path            | str \| Path \| list[str \| Path]           | required | Directory or list of directories to scan      |
| folder          | Folder \| None                            | None     | Custom folder metadata                        |
| depth           | "structure" \| "schema" \| "full" \| None | None     | Scan depth (uses catalog.depth if None)       |
| include         | list[str] \| None                         | None     | Glob patterns to include                      |
| exclude         | list[str] \| None                         | None     | Glob patterns to exclude                      |
| recursive       | bool                                      | True     | Scan subdirectories                           |
| infer_stats     | bool                                      | True     | Compute distinct/missing/duplicate counts     |
| csv_encoding    | str \| None                               | None     | Override CSV encoding                         |
| sample_size     | int \| None                               | None     | Sample rows for stats (overrides catalog)     |
| csv_skip_copy       | bool \| None                              | None     | Skip UTF-8 temp copy (overrides catalog)      |
| storage_options | dict \| None                              | None     | Options for remote storage (passed to fsspec) |
| refresh         | bool \| None                              | None     | Force rescan (overrides catalog setting)      |
| quiet           | bool \| None                              | None     | Override catalog quiet setting                |
| time_series     | bool                                      | True     | Group files with temporal patterns            |

**Time series detection:**

When `time_series=True` (default), files with temporal patterns in their names are automatically grouped into a single dataset:

```
data/
├── enquete_2020.csv    ─┐
├── enquete_2021.csv     ├─→ Single dataset "enquete" with nb_resources=3
├── enquete_2022.csv    ─┘
└── reference.csv       ─→ Separate dataset "reference"
```

Detected patterns: year (`2024`), quarter (`2024Q1`, `2024T2`), month (`2024-03`, `202403`), date (`2024-03-15`).

The resulting dataset includes:

- `nb_resources`: number of resources in the series
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
    sample_size=None,
    csv_skip_copy=None,
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
| path            | str \| Path \| list[str \| Path]           | required | File(s) or partitioned directory (local/remote) |
| folder          | Folder \| None                            | None     | Parent folder                                 |
| folder_id       | str \| None                               | None     | Parent folder ID (alternative to folder)      |
| depth           | "structure" \| "schema" \| "full" \| None | None     | Scan depth (uses catalog.depth if None)       |
| infer_stats     | bool                                      | True     | Compute statistics                            |
| csv_encoding    | str \| None                               | None     | Override CSV encoding                         |
| sample_size     | int \| None                               | None     | Sample rows for stats (overrides catalog)     |
| csv_skip_copy       | bool \| None                              | None     | Skip UTF-8 temp copy (overrides catalog)      |
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
| schema            | str \| list[str] \| None                         | None     | Schema(s) to scan                        |
| include           | list[str] \| None                               | None     | Table name patterns to include           |
| exclude           | list[str] \| None                               | None     | Table name patterns to exclude           |
| infer_stats       | bool                                            | True     | Compute column statistics                |
| sample_size       | int \| None                                     | None     | Sample rows for stats (overrides catalog)|
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

Exports JSON metadata files. Uses `app_path` by default if set at init.

### `Catalog.finalize()`

```python
catalog.finalize()
```

Removes entities no longer seen during scan. Called automatically by `export_db()`/`export_app()`.

### `Catalog.export_app()`

```python
catalog.export_app(output_dir=None, open_browser=False, quiet=None)
```

Exports complete standalone datannur app with data. Uses `app_path` by default if set at init.

### `app_config`

Pass a dictionary to `Catalog(app_config={...})` to configure the web app. Entries are written as `config.json` in the database directory.

```python
catalog = Catalog(
    app_path="./my-catalog",
    app_config={
        "contact_email": "contact@example.com",
        "more_info": "Data from [open data portal](https://example.com).",
    },
)
```

If `app_config` is not provided, no `config.json` is generated.

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
