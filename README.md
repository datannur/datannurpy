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
Catalog(freq_threshold=100, csv_encoding=None, quiet=False)
```

| Attribute        | Type             | Description                                             |
| ---------------- | ---------------- | ------------------------------------------------------- |
| `freq_threshold` | `int`            | Max distinct values for modality detection (0=disabled) |
| `csv_encoding`   | `str \| None`    | Default CSV encoding (`"utf-8"`, `"cp1252"`, etc.)      |
| `quiet`          | `bool`           | Suppress progress logging (default: `False`)            |
| `folders`        | `list[Folder]`   | All folders in catalog                                  |
| `datasets`       | `list[Dataset]`  | All datasets in catalog                                 |
| `variables`      | `list[Variable]` | All variables in catalog                                |
| `modalities`     | `list[Modality]` | All modalities in catalog                               |

### `Catalog.add_folder()`

```python
catalog.add_folder(path, folder=None, *, include=None, exclude=None,
                   recursive=True, infer_stats=True, csv_encoding=None, quiet=None)
```

| Parameter      | Type                | Default  | Description                                |
| -------------- | ------------------- | -------- | ------------------------------------------ |
| `path`         | `str \| Path`       | required | Directory to scan                          |
| `folder`       | `Folder \| None`    | `None`   | Custom folder metadata                     |
| `include`      | `list[str] \| None` | `None`   | Glob patterns to include (`["*.csv"]`)     |
| `exclude`      | `list[str] \| None` | `None`   | Glob patterns to exclude (`["**/tmp/**"]`) |
| `recursive`    | `bool`              | `True`   | Scan subdirectories                        |
| `infer_stats`  | `bool`              | `True`   | Compute distinct/missing/duplicate counts  |
| `csv_encoding` | `str \| None`       | `None`   | Override CSV encoding                      |
| `quiet`        | `bool \| None`      | `None`   | Override catalog quiet setting             |

### `Catalog.add_dataset()`

```python
catalog.add_dataset(path, folder=None, *, folder_id=None, infer_stats=True,
                    csv_encoding=None, quiet=None, id=None, name=None, description=None, ...)
```

| Parameter     | Type             | Default  | Description                                |
| ------------- | ---------------- | -------- | ------------------------------------------ |
| `path`        | `str \| Path`    | required | File or partitioned directory              |
| `folder`      | `Folder \| None` | `None`   | Parent folder                              |
| `folder_id`   | `str \| None`    | `None`   | Parent folder ID (alternative to `folder`) |
| `infer_stats` | `bool`           | `True`   | Compute statistics                         |
| `quiet`       | `bool \| None`   | `None`   | Override catalog quiet setting             |
| `id`          | `str \| None`    | `None`   | Override dataset ID                        |
| `name`        | `str \| None`    | `None`   | Override dataset name                      |
| `description` | `str \| None`    | `None`   | Override dataset description               |

Additional metadata parameters: `type`, `link`, `localisation`, `manager_id`, `owner_id`, `tag_ids`, `doc_ids`, `start_date`, `end_date`, `updating_each`, `no_more_update`

### `Catalog.add_database()`

```python
catalog.add_database(connection, folder=None, *, schema=None, include=None,
                     exclude=None, infer_stats=True, sample_size=None,
                     group_by_prefix=True, prefix_min_tables=2, quiet=None)
```

| Parameter           | Type                | Default  | Description                            |
| ------------------- | ------------------- | -------- | -------------------------------------- |
| `connection`        | `str`               | required | Connection string (see formats below)  |
| `folder`            | `Folder \| None`    | `None`   | Custom root folder                     |
| `schema`            | `str \| None`       | `None`   | Specific schema to scan                |
| `include`           | `list[str] \| None` | `None`   | Table name patterns to include         |
| `exclude`           | `list[str] \| None` | `None`   | Table name patterns to exclude         |
| `infer_stats`       | `bool`              | `True`   | Compute column statistics              |
| `sample_size`       | `int \| None`       | `None`   | Limit rows for stats (large tables)    |
| `group_by_prefix`   | `bool \| str`       | `True`   | Group tables by prefix into subfolders |
| `prefix_min_tables` | `int`               | `2`      | Min tables to form a prefix group      |
| `quiet`             | `bool \| None`      | `None`   | Override catalog quiet setting         |

**Connection string formats:**

- SQLite: `sqlite:///path/to/db.sqlite`
- PostgreSQL: `postgresql://user:pass@host:5432/database`
- MySQL: `mysql://user:pass@host:3306/database`
- Oracle: `oracle://user:pass@host:1521/service_name`
- SQL Server: `mssql://user:pass@host:1433/database`

### `Catalog.add_metadata()`

```python
catalog.add_metadata(path, quiet=None)
```

| Parameter | Type           | Default  | Description                                  |
| --------- | -------------- | -------- | -------------------------------------------- |
| `path`    | `str \| Path`  | required | Folder or database containing metadata files |
| `quiet`   | `bool \| None` | `None`   | Override catalog quiet setting               |

**Supported entity files/tables:** `folder`, `dataset`, `variable`, `modality`, `value`, `freq`, `institution`, `tag`, `doc`

### `Catalog.export_db()`

```python
catalog.export_db(output_dir, quiet=None)
```

Exports JSON metadata files to `output_dir` (for use with existing datannur instance).

### `Catalog.export_app()`

```python
catalog.export_app(output_dir, open_browser=False, quiet=None)
```

Exports complete standalone datannur app with data to `output_dir`.

### `Folder`

```python
Folder(id, name=None, description=None, parent_id=None, type=None, data_path=None)
```

| Parameter     | Type          | Description       |
| ------------- | ------------- | ----------------- |
| `id`          | `str`         | Unique identifier |
| `name`        | `str \| None` | Display name      |
| `description` | `str \| None` | Description       |
| `parent_id`   | `str \| None` | Parent folder ID  |

### ID Helpers

```python
from datannurpy import sanitize_id, build_dataset_id, build_variable_id
```

| Function                                               | Description                | Example                                                 |
| ------------------------------------------------------ | -------------------------- | ------------------------------------------------------- |
| `sanitize_id(s)`                                       | Clean string for use as ID | `"My File (v2)"` → `"My_File_v2"`                       |
| `build_dataset_id(folder_id, dataset_name)`            | Build dataset ID           | `("src", "sales")` → `"src---sales"`                    |
| `build_variable_id(folder_id, dataset_name, var_name)` | Build variable ID          | `("src", "sales", "amount")` → `"src---sales---amount"` |

## License

MIT License - see the [LICENSE](LICENSE) file for details.
