<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/datannur/datannur/main/public/assets/main-banner-dark.png">
  <img alt="datannur logo" src="https://raw.githubusercontent.com/datannur/datannur/main/public/assets/main-banner.png">
</picture>

[![MIT License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PyPI version](https://img.shields.io/pypi/v/datannurpy.svg)](https://pypi.org/project/datannurpy/)
[![Python](https://img.shields.io/badge/python-≥3.9-blue.svg)](https://pypi.org/project/datannurpy/)
[![CI](https://github.com/datannur/datannurpy/actions/workflows/ci.yml/badge.svg)](https://github.com/datannur/datannurpy/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/datannur/datannurpy/branch/main/graph/badge.svg)](https://codecov.io/gh/datannur/datannurpy)

# datannurpy

datannurpy is the Python builder for [datannur](https://github.com/datannur/datannur) catalogs: it scans files and databases, extracts metadata and statistics, and exports a ready-to-use catalog.

**Key features:**

- **Broad format support** - CSV, Excel, Parquet, Delta Lake, Iceberg, SAS, SPSS, Stata
- **Database introspection** - PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB
- **Remote & cloud storage** - SFTP, S3, Azure Blob, GCS via fsspec
- **Rich metadata** - schema, statistics, frequencies, modalities, auto-tagging
- **Incremental scans** - only rescan what changed between runs
- **YAML or Python API** - declarative configuration or programmatic control

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

## Documentation

📖 **Full documentation:** [docs.datannur.com/builder](https://docs.datannur.com/builder/)

🌐 **Website:** [datannur.com](https://datannur.com)

🚀 **Demo:** [dev.datannur.com](https://dev.datannur.com/)

## Contributing

For development documentation and contributing guidelines, see [`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

MIT - see [LICENSE](LICENSE). All dependencies are MIT/Apache 2.0/BSD compatible.
