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
├── organization.json # Organizations (owners, managers)
├── tag.csv           # Tags
├── concept.csv       # Business glossary concepts
├── enumeration.csv   # Enumerations
├── value.csv         # Enumeration values
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

The most common metadata columns are the same whether the source is CSV, Excel, JSON, SAS, or a database table:

| Entity/table | Useful columns |
| ------------ | -------------- |
| `folder` | `id`, `parent_id`, `name`, `description`, `manager_organization_id`, `owner_organization_id`, `tag_ids`, `doc_ids`, `license`, `link`, `localisation` |
| `dataset` | `id`, `folder_id`, `name`, `description`, `manager_organization_id`, `owner_organization_id`, `tag_ids`, `doc_ids`, `license`, `data_path`, `link`, `localisation`, `start_date`, `end_date`, `updating_each` |
| `variable` | `id`, `name`, `dataset_id`, `description`, `tag_ids`, `enumeration_ids`, `concept_id`, `type`, `start_date`, `end_date` |
| `organization` | `id`, `parent_id`, `name`, `description`, `email`, `phone`, `tag_ids`, `doc_ids` |
| `tag` | `id`, `parent_id`, `name`, `description`, `doc_ids` |
| `doc` | `id`, `name`, `description`, `path`, `type`, `last_update` |
| `concept` | `id`, `parent_id`, `name`, `description`, `tag_ids`, `doc_ids` |
| `enumeration` | `id`, `folder_id`, `name`, `description`, `type` |
| `value` | `enumeration_id`, `value`, `description` |
| `config` | `id`, `value` |

Localized text fields can be provided with the app's `field:lang` convention. For example, keep `name` and `description` as the default/fallback language, then add `name:fr` or `description:fr` for French text, or `name:de` or `description:de` for German. Datannurpy preserves localized columns for any entity field that already exists in the schema, such as `name:fr`, `name:de`, and `description:fr` on datasets, variables, tags, organizations, concepts, enumerations, values, docs, and app filter labels. An empty localized cell contributes nothing from that source (the value falls back to a lower source or the scan); use `!` to clear a localized value explicitly.

For folder-based metadata sources, name each file after the entity (`folder.csv`, `folder.xlsx`, `folder.json`, etc.). List fields such as `tag_ids`, `doc_ids`, and `enumeration_ids` can be written as comma-separated values in tabular files, or as arrays in JSON. For full schema details, use the linked datannur schemas as the reference.

**Merge behavior:**

- Existing entities are updated (manual values override auto-scanned values)
- New entities are created
- List fields (`tag_ids`, `doc_ids`, etc.) are merged
- An empty cell, a JSON empty array, or a missing column means "no current value from this metadata source" — it contributes nothing, so the value falls back to what a lower metadata source or the scan provides. It does **not** freeze the previous export's value. Clear a cell to drop your override and return to the scanned value; use `!` to force the final value to empty even when the scan provides one

**Ordering:** Metadata is automatically applied before export/finalization, after all `add_folder`, `add_dataset`, and `add_database` calls, so manual values take precedence. If `app_path/data/db-ui` exists, it is loaded automatically as the last metadata source, after configured `metadata_path` sources. This lets local app edits override scanned metadata without adding `data/db-ui` to the configuration. `data/db-ui` uses the same supported metadata formats as any other metadata source; `data/db` remains the generated export.

### Overlay instructions

Metadata sources can include small instructions for clearing fields, removing individual relations, or deleting entities before export. These instructions are consumed by the builder and are not written to `data/db`.

Use `!` as the exact value of a scalar field to clear it:

```csv
id,description
source---employees_csv,!
```

Use `!` as the exact value of a relation field to clear all accumulated relations for that field:

```csv
id,tag_ids
source---employees_csv,!
```

Use `!id` inside a relation list to remove one accumulated relation while keeping or adding others:

```json
[
  {
    "id": "source---employees_csv---salary",
    "tag_ids": ["finance", "!auto---numeric"]
  }
]
```

Relation removals are applied in metadata source order. If one row contains both `id` and `!id`, removal wins for that relation. Supported relation fields are `tag_ids`, `doc_ids`, `enumeration_ids`, and `source_variable_ids`.

### Deletion

The export [mirrors the current sources](/output#incremental-scan), but how an entity disappears depends on its kind:

- **Parent entities** (`folder`, `dataset`, `enumeration`, `organization`, `tag`, `doc`, `concept`) are reconciled automatically: one no longer produced by the scan and absent from metadata is dropped on the next run.
- **Child entities** (`variable`, `value`, `frequency`) are not tracked individually. Removing their row from a metadata file does **not** delete them — they only disappear when their parent is rescanned or removed (cascade), or, for variables, via an explicit `_delete`. As a safety net, a variable whose dataset no longer exists is removed at export so the catalog stays referentially consistent.

Use `_delete: true` to remove an entity from the final catalog immediately:

```json
[
  {
    "id": "source---old_dataset",
    "_delete": true
  }
]
```

`_delete` is supported for all ID-keyed catalog entities (including `variable`). Composite tables (`value` and `frequency`) cannot be deleted directly; they are removed through cascades from their parent enumeration, variable, dataset, or folder.

Cascades are applied before export:

- deleting a folder removes its descendant folders, contained datasets, dataset variables, frequencies, previews, and contained enumerations;
- deleting a dataset removes its variables, frequencies, and previews;
- deleting a variable removes its frequencies;
- deleting enumerations, tags, docs, concepts, or organizations also cleans related references where needed.

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
- Each scanned file is matched to its metadata entry via `_match_path` when present, or via `data_path` as a fallback. The scan reuses the metadata-defined `id` and `folder_id`.
- Files with no metadata match are reported according to `on_unmatched`: `"warn"` (default), `"skip"`, or `"error"`.

**Matching:** `_match_path` is an optional technical key used only to attach scanned files to metadata rows. Use it when the scan key differs from the public `data_path`, for example for remote sources or logical time-series datasets. If `_match_path` is omitted, `data_path` is used as the match key.

For remote paths, credentials are not part of the match identity: `sftp://user@example.org/path/file.csv` and `sftp://example.org/path/file.csv` match the same scan key. The canonical remote identity keeps the protocol, host, optional port, and path.

For time-series datasets, `_match_path` can use the same normalized period syntax produced by datannurpy: `[YYYY]`, `[YYYY/MM]`, `[YYYY]Q[N]`, or `[YYYY/MM/DD]`. This matches the logical series, not only the latest physical file.

**Example** — `./metadata/dataset.csv`:

```csv
id,name,folder_id,data_path
sales-2024,Sales 2024,finance,../data/parquet/sales-2024.parquet
hr-headcount,HR headcount,hr,../data/parquet/hr-headcount.parquet
```

Combined with `add_folder('./data/parquet', create_folders=False)`, the scan attaches variables and stats to `sales-2024` and `hr-headcount` (the IDs declared in metadata) without creating any folder from the disk layout.

**Remote and time-series example** — `./metadata/dataset.csv`:

```csv
id,name,folder_id,data_path,_match_path
sales,Sales,finance,sftp://example.org/shared/data/sales_2024.csv,sftp://example.org/shared/data/sales_[YYYY].csv
traffic,Traffic,transport,sftp://example.org/shared/data/traffic_2024-03.csv,sftp://example.org/shared/data/traffic_[YYYY/MM].csv
```

Here `data_path` remains the public path shown in the catalog, while `_match_path` is only used to attach the scan result to the metadata row. If the SFTP scan is configured as `sftp://user@example.org/shared/data`, the user part is ignored for matching.

**Constraints:**

- `create_folders=False` is incompatible with `metadata=` in `add_folder()` (no folder is created).
- The `data_path` you write in `metadata/dataset.csv` is exported as-is in the output. Use `_match_path` when the technical scan key should differ from the public path shown in the front-end.

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
