# Scanning files

Use [Scan depth](/scan-depth) to choose how much metadata datannurpy extracts. The same `depth` setting applies to `add_folder`, `add_dataset`, and `add_database`, either globally or per `add` entry.

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

### Filtering patterns

`include` and `exclude` patterns are matched against normalized relative paths from the scanned folder. Paths use `/` separators on every platform, and leading `/` or `./` in patterns is ignored. Filtering first keeps files that match at least one `include` pattern when `include` is set, then removes files that match any `exclude` pattern.

Examples:

| Pattern | Meaning |
| ------- | ------- |
| `name.csv` | Exact file at the scanned folder root |
| `subdir/name.csv` | Exact relative file path |
| `*.csv` | Any CSV file at any depth |
| `subdir/*.csv` | CSV files directly inside `subdir` |
| `**/tmp/**` | Files under any `tmp` directory |
| `tmp/` | Everything under the root `tmp` directory |
| `**/tmp/` | Everything under any directory named `tmp` |

### Time series detection

When `time_series: true` (default), files with temporal patterns in their names or parent folders are automatically grouped into a single dataset:

```
data/
├── enquete_2020.csv    ─┐
├── enquete_2021.csv     ├─→ Single dataset "enquete" with nb_resources=3
├── enquete_2022.csv    ─┘
└── reference.csv       ─→ Separate dataset "reference"
```

The resulting dataset includes `nb_resources`, `start_date`, and `end_date`. Variables track their own `start_date` and `end_date` when their presence changes across periods.

Set `time_series: false` to treat each file as a separate dataset.

See [Time series grouping](/time-series) for supported patterns, database table grouping, schema evolution, and false-positive rules.

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

## Remote storage

Scan files on SFTP servers or cloud storage (S3, Azure, GCS). The `storage_options` dict is passed directly to [fsspec](https://filesystem-spec.readthedocs.io/) — see provider docs for available options:

- [SFTP](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.sftp.SFTPFileSystem)
- [S3](https://s3fs.readthedocs.io/en/latest/)
- [Azure](https://github.com/fsspec/adlfs)
- [GCS](https://gcsfs.readthedocs.io/en/latest/)

```yaml
env_file: .env               # SFTP_PASSWORD, AWS_KEY, AWS_SECRET, etc.
```

### SFTP

Paramiko is included by default.

```yaml
add:
  - folder: sftp://user@host/path/to/data
    storage_options:
      password: ${SFTP_PASSWORD}   # or key_filename: /path/to/key
```

### Amazon S3

Requires `pip install datannurpy[s3]`.

```yaml
add:
  - folder: s3://my-bucket/data
    storage_options:
      key: ${AWS_KEY}
      secret: ${AWS_SECRET}
```

### Azure Blob

Requires `pip install datannurpy[azure]`.

```yaml
add:
  - folder: az://container/data
    storage_options:
      account_name: ${AZURE_ACCOUNT}
      account_key: ${AZURE_KEY}
```

### Google Cloud Storage

Requires `pip install datannurpy[gcs]`.

```yaml
add:
  - folder: gs://my-bucket/data
    storage_options:
      token: /path/to/credentials.json
```

### Single remote file

```yaml
add:
  - dataset: s3://my-bucket/data/sales.parquet
    storage_options:
      key: ${AWS_KEY}
      secret: ${AWS_SECRET}
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

When a dataset has more rows than `sample_size`, a uniform random sample is used for frequency counts and enumeration detection. All other statistics (`nb_row`, `nb_missing`, `nb_distinct`, `min`, `max`, `mean`, `std`) are computed on the full dataset.

The actual number of sampled rows is recorded in `Dataset.sample_size` (`null` when no sampling was applied).
