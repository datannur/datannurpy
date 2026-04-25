# Metadata & configuration

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

## Metadata-first pattern

When the structure of the catalog (folders, dataset IDs, hierarchy) is defined entirely by `metadata_path` and the filesystem only provides files to scan for technical info (variables, stats, sizes, formats), use `create_folders=False`:

```yaml
metadata_path: ./metadata
add:
  - folder: ./data/parquet
    create_folders: false
```

In this mode:

- No folder is created from the scanned directory; the hierarchy is taken from `metadata/folder.csv`.
- Each scanned file is matched to its metadata entry via the `data_path` column of `metadata/dataset.csv`. The scan reuses the metadata-defined `id` and `folder_id`.
- Files with no metadata match are reported according to `on_unmatched`: `"warn"` (default), `"skip"`, or `"error"`.

**Matching:** the scan computes an absolute path for each file and compares it to the resolved `data_path` from `metadata/dataset.csv`. Relative `data_path` values are resolved against the metadata source directory (the folder containing `dataset.csv`). URLs and missing files are not matched.

**Example** — `./metadata/dataset.csv`:

```csv
id,name,folder_id,data_path
sales-2024,Sales 2024,finance,../data/parquet/sales-2024.parquet
hr-headcount,HR headcount,hr,../data/parquet/hr-headcount.parquet
```

Combined with `add_folder('./data/parquet', create_folders=False)`, the scan attaches variables and stats to `sales-2024` and `hr-headcount` (the IDs declared in metadata) without creating any folder from the disk layout.

**Constraints:**

- `create_folders=False` is incompatible with `folder=` / `id=` / `name=` / `description=` / `manager_id=` / `owner_id=` (no folder is created).
- The `data_path` you write in `metadata/dataset.csv` is also exported as-is in the output (it is the public link to the data, not a private match key). Use a URL or a deployment-relative path if you want it to remain valid in the front-end.

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

## app_config


Configure the web app with key-value entries (written as `config.json`):

```yaml
app_path: ./my-catalog
app_config:
  contact_email: contact@example.com
  more_info: "Data from [open data portal](https://example.com)."
```

If `app_config` is not provided, `config.csv`/`config.xlsx`/`config.json` (columns `id`, `value`) from `metadata_path` is used instead. If neither is provided, no `config.json` is generated.
