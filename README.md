# datannurpy

[![PyPI version](https://img.shields.io/pypi/v/datannurpy.svg)](https://pypi.org/project/datannurpy/)
[![Python](https://img.shields.io/badge/python-≥3.9-blue.svg)](https://pypi.org/project/datannurpy/)
[![CI](https://github.com/datannur/datannurpy/actions/workflows/ci.yml/badge.svg)](https://github.com/datannur/datannurpy/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Python library for [datannur](https://github.com/datannur/datannur) catalog metadata management.

## Installation

```bash
pip install datannurpy
```

For database support:

```bash
pip install datannurpy[postgres]  # PostgreSQL
pip install datannurpy[mysql]     # MySQL
pip install datannurpy[oracle]    # Oracle
# SQLite works out of the box
```

## Quick start

```python
from datannurpy import Catalog

catalog = Catalog()
catalog.add_folder("./data")
catalog.export_app("./my-catalog", open_browser=True)
```

## Scanning files

```python
from datannurpy import Catalog, Folder

catalog = Catalog()

# Scan a folder (CSV, Excel)
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
)

# Add a single file
catalog.add_dataset("./data/sales.csv")
```

## Scanning databases

```python
# SQLite
catalog.add_database("sqlite:///path/to/db.sqlite")

# PostgreSQL / MySQL / Oracle
catalog.add_database("postgresql://user:pass@host:5432/mydb")
catalog.add_database("mysql://user:pass@host:3306/mydb")
catalog.add_database("oracle://user:pass@host:1521/service_name")

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

## Output

```python
# JSON metadata only (for existing datannur instance)
catalog.write("./output")

# Complete standalone app
catalog.export_app("./my-catalog", open_browser=True)
```

## License

MIT License - see the [LICENSE](LICENSE) file for details.
