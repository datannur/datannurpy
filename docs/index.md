# Getting Started

Python library for [datannur](https://github.com/datannur/datannur) catalog metadata management.

## Supported formats


A lightweight catalog compatible with most data sources:

| Category           | Formats                                               |
| ------------------ | ----------------------------------------------------- |
| **Spreadsheets**   | CSV, Excel (.xlsx, .xls)                              |
| **Columnar**       | Parquet, Delta Lake, Apache Iceberg, Hive partitioned |
| **Statistical**    | SAS (.sas7bdat), SPSS (.sav), Stata (.dta)            |
| **Geospatial**     | GeoJSON, Shapefile, GeoPackage, GeoParquet, GeoTIFF, GML, KML, ESRI File Geodatabase |
| **Databases**      | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |
| **Remote storage** | SFTP, Amazon S3, Azure Blob Storage, Google Cloud Storage |

All scanned data formats support automatic schema inference and statistics computation.

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
pip install datannurpy[ssh]       # SFTP and SSH tunneling to remote databases

# File formats
pip install datannurpy[stat]      # SAS, SPSS, Stata
pip install datannurpy[delta]     # Delta Lake metadata extraction
pip install datannurpy[iceberg]   # Apache Iceberg metadata extraction
pip install datannurpy[geo]       # GeoJSON, Shapefile, GeoTIFF, GML, KML, File Geodatabase

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

Create a `catalog.yml` file that tells datannurpy where to scan data and where to export the catalog:

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

This scans the configured files and database, writes a standalone datannur app to `./my-catalog`, and opens it in your browser. Re-running the same config updates the existing catalog incrementally, so unchanged files and tables are reused when possible.

For export options such as `update_app`, `copy_assets`, or `post_export`, see [Output & exports](/output). For configuration options such as `metadata_path`, `env`, `env_file`, or `app_config`, see [Metadata & configuration](/metadata).

Use `output_dir` instead of `app_path` when you only want JSON metadata for an existing datannur app:

```yaml
output_dir: ./metadata-export

add:
  - folder: ./data
```

Most projects start with this flow: choose the sources in `add`, choose a [scan depth](/scan-depth), optionally enrich the catalog with [manual metadata](/metadata), then export either a complete app or JSON metadata.

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
