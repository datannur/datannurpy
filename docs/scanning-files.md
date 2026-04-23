# Scanning files

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

## CSV options


Avoid the UTF-8 temp copy when files are already local and UTF-8 (auto-fallback if encoding detection fails):

```yaml
csv_skip_copy: true
```

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
