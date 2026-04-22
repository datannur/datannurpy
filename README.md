# datannurpy

[![PyPI version](https://img.shields.io/pypi/v/datannurpy.svg)](https://pypi.org/project/datannurpy/)
[![Python](https://img.shields.io/badge/python-≥3.9-blue.svg)](https://pypi.org/project/datannurpy/)
[![CI](https://github.com/datannur/datannurpy/actions/workflows/ci.yml/badge.svg)](https://github.com/datannur/datannurpy/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/datannur/datannurpy/branch/main/graph/badge.svg)](https://codecov.io/gh/datannur/datannurpy)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Python library for [datannur](https://github.com/datannur/datannur) catalog metadata management.

## Supported formats

A lightweight catalog compatible with most data sources:

| Category          | Formats                                               |
| ----------------- | ----------------------------------------------------- |
| **Spreadsheets**  | CSV, Excel (.xlsx, .xls)                              |
| **Columnar**      | Parquet, Delta Lake, Apache Iceberg, Hive partitioned |
| **Statistical**   | SAS (.sas7bdat), SPSS (.sav), Stata (.dta)            |
| **Databases**     | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |

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
pip install datannurpy[ssh]       # SSH tunneling to remote databases

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

> **Note:** The `iceberg`, `s3`, `azure`, `gcs`, and `cloud` extras require Python 3.10+.

**SQL Server note:** Requires an ODBC driver on the system:

- macOS: `brew install unixodbc freetds`
- Linux: `apt install unixodbc-dev tdsodbc`
- Windows: [Microsoft ODBC Driver](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)

### Air-gapped install

For environments without direct internet access (strict corporate proxies,
classified networks), download wheels on a connected machine and transfer
them to the target:

```bash
# On a connected machine (match target OS/Python if different):
pip download 'datannurpy[postgres,ssh]' -d ./wheels/ \
    --platform manylinux2014_x86_64 --python-version 3.11 \
    --only-binary=:all:

# On the air-gapped target:
pip install --no-index --find-links ./wheels/ 'datannurpy[postgres,ssh]'
```

Omit `--platform`/`--python-version` if source and target share the same
OS and Python version. A CycloneDX SBOM (`*-sbom.cyclonedx.json`) is
published as a GitHub Release asset for each version to support CVE audits
with tools like Dependency-Track or Grype.

## Quick start

```yaml
# catalog.yml
app_path: ./my-catalog
open_browser: true

add:
  - folder: ./data
    include: ["*.csv", "*.xlsx", "*.parquet"]

  - database: sqlite:///mydb.sqlite
```

```bash
python -m datannurpy catalog.yml
```

Or use the Python API:

```python
from datannurpy import Catalog

catalog = Catalog()
catalog.add_folder("./data", include=["*.csv", "*.xlsx", "*.parquet"])
catalog.add_database("sqlite:///mydb.sqlite")
catalog.export_app("./my-catalog", open_browser=True)
```

## CLI

```bash
python -m datannurpy catalog.yml
python -m datannurpy --help     # show usage
python -m datannurpy --version  # show version
```

## Scan depth

The `depth` parameter controls how much metadata is extracted. Set it globally or per entry:

```yaml
depth: variable                   # global default

add:
  - folder: ./data                # inherits "variable"

  - folder: ./big
    depth: stat                   # override for this entry

  - database: sqlite:///db.sqlite
    depth: dataset
```

| Feature                              | `dataset` | `variable` | `stat`         | `value` (default)  |
| ------------------------------------ | :-------: | :--------: | :------------: | :----------------: |
| Folders                              | ✓         | ✓          | ✓              | ✓                  |
| Datasets (format, path, mtime)       | ✓         | ✓          | ✓              | ✓                  |
| Variables (names, types)             |           | ✓          | ✓              | ✓                  |
| DB introspection (PK, FK, comments)  |           | ✓          | ✓              | ✓                  |
| Row count, statistics                |           |            | ✓              | ✓                  |
| Modalities, frequencies, patterns    |           |            |                | ✓                  |
| Auto-tagging (format, security, text)|           |            |                | ✓                  |

> **Note:** At `depth="variable"`, CSV and Excel files only extract column **names** (types require reading data, available from `depth="stat"`). All other formats provide types at this level.

**Typical use cases:**

- **`dataset`** — quick inventory of available files/tables without reading data
- **`variable`** — lightweight schema discovery (column names and types)
- **`stat`** — data profiling without modality detection (faster than `value`)
- **`value`** — full catalog with frequency tables and modality assignment (default)

### Auto-tagging

At `depth="value"` (default), string columns are automatically tagged by content type. Tags use a two-level hierarchy under the `auto` parent:

| Category   | Tags                                                       |
| ---------- | ---------------------------------------------------------- |
| **Format** | `auto---email`, `auto---phone`, `auto---uuid`, `auto---iban` |
| **Security** | `auto---bcrypt`, `auto---argon2`, `auto---jwt`, `auto---secret` |
| **Text**   | `auto---structured`, `auto---semi-structured`, `auto---free-text` → `auto---natural-text` |

Each variable receives at most **one leaf tag**. The frontend can use `parent_id` to filter by category (e.g. selecting `auto---security` shows all bcrypt/argon2/jwt/secret variables).

**Security-tagged columns** (bcrypt, argon2, jwt, secret) have their raw frequency values suppressed — only pattern frequencies are emitted, so no actual secrets appear in the exported catalog.

### Policy tags

Policy tags let you manually control scan behavior for specific variables. Like auto-tags, they live under the `scan` hierarchy and are auto-created — no `tag.csv` entry needed.

| Tag                      | Effect                                                         |
| ------------------------ | -------------------------------------------------------------- |
| `policy---freq-hidden`   | Suppress all frequency and modality data (stats remain visible) |

Assign the tag in your `variable.csv` metadata:

```csv
id,tag_ids
source---employees_csv---first_name,"policy---freq-hidden"
```

This is useful for sensitive columns that auto-tagging does not flag (e.g. first names, internal IDs, business codes).

## Scanning files

```yaml
add:
  # Scan a folder (CSV, Excel, SAS)
  - folder: ./data

  # With custom folder metadata
  - folder: ./data
    id: prod
    name: Production

  # With filtering options
  - folder: ./data
    include: ["*.csv", "*.xlsx"]
    exclude: ["**/tmp/**"]
    recursive: true
    csv_encoding: utf-8        # or cp1252, iso-8859-1 (auto-detected by default)

  # Multiple folders with shared options
  - folder: [./data/sales, ./data/hr]
    include: ["*.csv"]

  # A single file
  - dataset: ./data/sales.csv

  # Multiple files
  - dataset:
      - ./data/sales.csv
      - ./data/products.csv
```

### Time series detection

When `time_series: true` (default), files with temporal patterns in their names are automatically grouped into a single dataset:

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

Set `time_series: false` to treat each file as a separate dataset.

## Parquet formats

Supports simple Parquet files and partitioned datasets (Delta, Hive, Iceberg):

```yaml
add:
  # add_folder auto-detects all formats
  - folder: ./data             # scans *.parquet + Delta/Hive/Iceberg directories

  # Single partitioned directory with metadata override
  - dataset: ./data/sales_delta
    name: Sales Data
    description: Monthly sales
    folder:
      id: sales
      name: Sales
```

With extras `[delta]` and `[iceberg]`, metadata (name, description, column docs) is extracted when available.

## Remote storage

Scan files on SFTP servers or cloud storage (S3, Azure, GCS):

```yaml
env_file: .env               # SFTP_PASSWORD, AWS_KEY, AWS_SECRET, etc.

add:
  # SFTP (paramiko included by default)
  - folder: sftp://user@host/path/to/data
    storage_options:
      password: ${SFTP_PASSWORD}   # or key_filename: /path/to/key

  # Amazon S3 (requires: pip install datannurpy[s3])
  - folder: s3://my-bucket/data
    storage_options:
      key: ${AWS_KEY}
      secret: ${AWS_SECRET}

  # Azure Blob (requires: pip install datannurpy[azure])
  - folder: az://container/data
    storage_options:
      account_name: ${AZURE_ACCOUNT}
      account_key: ${AZURE_KEY}

  # Google Cloud Storage (requires: pip install datannurpy[gcs])
  - folder: gs://my-bucket/data
    storage_options:
      token: /path/to/credentials.json

  # Single remote file
  - dataset: s3://my-bucket/data/sales.parquet
    storage_options:
      key: ${AWS_KEY}
      secret: ${AWS_SECRET}

  # Remote SQLite / GeoPackage database
  - database: sftp://host/path/to/db.sqlite
    storage_options:
      key_filename: /path/to/key
  - database: s3://bucket/geodata.gpkg
    storage_options:
      key: ${AWS_KEY}
      secret: ${AWS_SECRET}
```

The `storage_options` dict is passed directly to [fsspec](https://filesystem-spec.readthedocs.io/). See provider documentation for available options:

- [SFTP](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.sftp.SFTPFileSystem)
- [S3](https://s3fs.readthedocs.io/en/latest/)
- [Azure](https://github.com/fsspec/adlfs)
- [GCS](https://gcsfs.readthedocs.io/en/latest/)

## Scanning databases

```yaml
add:
  # SQLite / GeoPackage
  - database: sqlite:///path/to/db.sqlite
  - database: sqlite:///path/to/geodata.gpkg

  # PostgreSQL / MySQL / Oracle / SQL Server
  - database: postgresql://user:pass@host:5432/mydb
  - database: mysql://user:pass@host:3306/mydb
  - database: oracle://user:pass@host:1521/service_name
  - database: mssql://user:pass@host:1433/mydb

  # SSL/TLS
  - database: postgresql://user:pass@host/db?sslmode=require

  # SQL Server with Windows auth (requires proper Kerberos setup)
  - database: mssql://host/db?TrustedConnection=yes

  # With options
  - database: postgresql://localhost/mydb
    schema: public
    include: ["sales_*"]
    exclude: ["*_tmp"]
    sample_size: 10000
    group_by_prefix: true       # group tables by common prefix (default)
    prefix_min_tables: 2        # minimum tables to form a group

  # Multiple schemas
  - database: postgresql://localhost/mydb
    schema: [public, sales, hr]

  # SSH tunnel (for databases behind a firewall)
  # Requires: pip install datannurpy[ssh]
  - database: mysql://user:pass@dbhost/mydb
    ssh_tunnel:
      host: ssh.example.com
      user: sshuser

  # SSH tunnel with more options
  - database: postgresql://user:pass@dbhost/mydb
    ssh_tunnel:
      host: bastion.example.com
      port: 2222
      user: admin
      key_file: ~/.ssh/id_rsa
```

**Connection string formats:**

- SQLite: `sqlite:///path/to/db.sqlite` or `sftp://host/path/db.sqlite` (remote)
- PostgreSQL: `postgresql://user:pass@host:5432/database`
- MySQL: `mysql://user:pass@host:3306/database`
- Oracle: `oracle://user:pass@host:1521/service_name`
- SQL Server: `mssql://user:pass@host:1433/database`
- DuckDB: pass an `ibis.duckdb.connect(...)` backend directly (no connection string)

**Database metadata enrichment** (requires `depth: variable` or higher):

| Metadata                | Target field          | Backends           |
| ----------------------- | --------------------- | ------------------ |
| Primary keys            | `Variable.key`        | All 6              |
| Foreign keys            | `Variable.fk_var_id`  | All 6              |
| Table/column comments   | `description`         | All except SQLite  |
| NOT NULL, UNIQUE, INDEX | Auto tags (`db---*`)  | All 6              |
| Auto-increment          | Auto tag              | All 6              |

This metadata is always refreshed, even when table data is unchanged (cache hit).

## Sampling

By default, `sample_size` is `100000`. All entries inherit this value. Override per entry, or set `null` to disable:

```yaml
sample_size: 100000               # default

add:
  - folder: ./data                # inherits 100000

  - folder: ./small
    sample_size: null             # no sampling

  - database: postgresql://localhost/mydb
    sample_size: 50000            # override
```

To disable sampling globally:

```yaml
sample_size: null
```

When a dataset has more rows than `sample_size`, a uniform random sample is used for frequency counts and modality detection. All other statistics (`nb_row`, `nb_missing`, `nb_distinct`, `min`, `max`, `mean`, `std`) are computed on the full dataset.

The actual number of sampled rows is recorded in `Dataset.sample_size` (`null` when no sampling was applied).

## CSV options

Avoid the UTF-8 temp copy when files are already local and UTF-8 (auto-fallback if encoding detection fails):

```yaml
csv_skip_copy: true
```

## Manual metadata

```yaml
# Load from a folder containing metadata files
metadata_path: ./metadata

# Or from a database
metadata_path: sqlite:///metadata.db

# Or a list of sources applied in order (later overrides/extends earlier)
metadata_path:
  - ./metadata-base        # shared or generated metadata
  - ./metadata-overlay     # local overrides (e.g. add policy tags)
```

Can be used alone or combined with auto-scanned sources (`add_folder`, `add_database`). Metadata is applied automatically before export.

**Expected structure:** One file/table per entity, named after the entity type:

```
metadata/
├── variable.csv      # Variables (descriptions, tags...)
├── dataset.xlsx      # Datasets
├── institution.json  # Institutions (owners, managers)
├── tag.csv           # Tags
├── concept.csv       # Business glossary concepts
├── modality.csv      # Modalities
├── value.csv         # Modality values
├── config.csv        # Web app config (see app_config)
└── ...
```

**Supported formats:** CSV, Excel (.xlsx), JSON, SAS (.sas7bdat), or database tables.

**File format:** Standard tabular structure following [datannur schemas](https://github.com/datannur/datannur/tree/main/public/schemas).

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

**Ordering:** Metadata is automatically applied before export/finalization, after all `add_folder`, `add_dataset`, and `add_database` calls, so manual values take precedence.

## Environment variables

Environment variables (`$VAR` or `${VAR}`) are expanded in all YAML values. All sources are loaded — `env:`, `env_file`, and `.env` next to the YAML file:

```yaml
env:
  data_dir: /shared/data
  db_host: db.example.com
env_file: /secure/path/.env    # secrets: DB_USER, DB_PASSWORD

add:
  - folder: ${data_dir}/sales
  - folder: ${data_dir}/hr
  - database: oracle://${DB_USER}:${DB_PASSWORD}@${db_host}:1521/ORCL
```

`env_file` supports a list of paths (last overrides first):

```yaml
env_file:
  - /shared/common.env         # defaults
  - /secure/credentials.env    # overrides common.env
```

Priority (first set wins): system env vars > `env:` YAML > `env_file` > `.env` local.

## Output

```yaml
# Complete standalone app
app_path: ./my-catalog
open_browser: true

# JSON metadata only (for existing datannur instance)
output_dir: ./output
```

## Incremental scan

Re-run with the same `app_path` to only rescan changed files (compares mtime) or tables (compares schema + row count):

```yaml
app_path: ./my-catalog

add:
  - folder: ./data               # skips unchanged files
```

Use `refresh: true` to force a full rescan.

## Evolution tracking

Changes between exports are automatically tracked in `evolution.json`:

- **add**: new folder, dataset, variable, modality, etc.
- **update**: modified field (shows old and new value)
- **delete**: removed entity

Cascade filtering: when a parent entity is added or deleted, its children are automatically filtered out to reduce noise. For example, adding a new dataset won't generate separate entries for each variable.

Disable tracking:

```yaml
track_evolution: false
```

## app_config

Configure the web app with key-value entries (written as `config.json`):

```yaml
app_path: ./my-catalog
app_config:
  contact_email: contact@example.com
  more_info: "Data from [open data portal](https://example.com)."
```

If `app_config` is not provided, `config.csv`/`config.xlsx`/`config.json` (columns `id`, `value`) from `metadata_path` is used instead. If neither is provided, no `config.json` is generated.

## post_export

Run Python scripts automatically after export:

```yaml
# Single script (bare name → python-scripts/start_app.py)
post_export: start_app

# Multiple scripts
post_export:
  - export_dcat
  - start_app
```

Script resolution:

| Format | Resolved path |
|---|---|
| `start_app` | `{output}/python-scripts/start_app.py` |
| `hook.py` | `{output}/hook.py` |
| `scripts/hook.py` | `{output}/scripts/hook.py` |
| `/absolute/path.py` | `/absolute/path.py` |

Works with both `app_path` and `output_dir` exports.

## Python API

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
    id=None,
    name=None,
    description=None,
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
| id              | str \| None                               | None     | Override folder ID                            |
| name            | str \| None                               | None     | Override folder name                          |
| description     | str \| None                               | None     | Override folder description                   |

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

## License

MIT License - see the [LICENSE](LICENSE) file for details.
