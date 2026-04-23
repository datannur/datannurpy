# Getting Started

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
