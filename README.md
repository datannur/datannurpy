<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/datannur/datannur/main/package/app/assets/main-banner-dark.png">
  <img alt="datannur logo" src="https://raw.githubusercontent.com/datannur/datannur/main/package/app/assets/main-banner.png">
</picture>

[![MIT License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PyPI version](https://img.shields.io/pypi/v/datannurpy.svg)](https://pypi.org/project/datannurpy/)
[![Python](https://img.shields.io/badge/python-≥3.9-blue.svg)](https://pypi.org/project/datannurpy/)
[![CI](https://github.com/datannur/datannurpy/actions/workflows/ci.yml/badge.svg)](https://github.com/datannur/datannurpy/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/datannur/datannurpy/branch/main/graph/badge.svg)](https://codecov.io/gh/datannur/datannurpy)

# datannurpy

datannurpy is the Python builder for [datannur](https://github.com/datannur/datannur). It scans files and databases, extracts metadata and statistics, then generates a ready-to-use catalog bundled with the datannur app.

**Key features:**

- **Broad format support** - CSV, Excel, Parquet, Delta Lake, Iceberg, SAS, SPSS, Stata
- **Geospatial** - GeoJSON, Shapefile, GeoPackage, GeoParquet, GeoTIFF, GML, KML, ESRI File Geodatabase
- **Database introspection** - PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB
- **Remote and cloud storage** - public HTTP(S) URLs, SFTP, S3, Azure Blob, GCS via fsspec
- **Metadata extraction** - Schemas, statistics, frequencies, enumerations, auto-tagging
- **Incremental scans** - Only rescan what changed between runs
- **YAML or Python API** - Declarative configuration or programmatic control

## Quick start

```bash
pip install datannurpy
```

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

This command scans the configured sources, generates the catalog files, and opens the datannur app.

## Documentation

📖 **Full documentation:** [docs.datannur.com/builder](https://docs.datannur.com/builder/)

🗂️ **datannur app:** [github.com/datannur/datannur](https://github.com/datannur/datannur)

🌐 **Website:** [datannur.com](https://datannur.com)

🚀 **Demo:** [dev.datannur.com](https://dev.datannur.com/)

## Contributing

For development documentation and contributing guidelines, see [`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

MIT - see [LICENSE](LICENSE). All dependencies are MIT/Apache 2.0/BSD compatible.
