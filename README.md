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

## Usage

```python
from datannurpy import Catalog

catalog = Catalog()
catalog.add_folder("./data")
catalog.write("./output")
```

### Multiple sources and custom folder metadata

```python
from datannurpy import Catalog, Folder

catalog = Catalog()
catalog.add_folder("./data", Folder(id="prod", name="Production"))
catalog.add_folder("/mnt/archive", Folder(id="archive", name="Archives"))
catalog.write("./output")
```

### Options

```python
catalog.add_folder(
    "./data",
    Folder(id="source", name="Source"),
    include=["*.csv", "*.xlsx"],
    exclude=["**/tmp/**"],
    recursive=True,
    infer_stats=True,
)
```

### Adding individual datasets

```python
catalog.add_dataset("./data/sales.csv")
catalog.add_dataset(
    "./archive/old.csv",
    folder=Folder(id="archive", name="Archives"),
    name="Ventes 2023",
    description="Données historiques",
)
```

### Export with visualization app

```python
# Export a complete datannur app instance
catalog.export_app("./my-catalog", open_browser=True)
```

This creates a standalone visualization app that can be opened directly in a browser or deployed to a web server.

## License

MIT License - see the [LICENSE](LICENSE) file for details.
