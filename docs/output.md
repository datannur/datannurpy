# Output & exports

## Output


```yaml
# Complete standalone app
app_path: ./my-catalog
open_browser: true

# JSON metadata only (for existing datannur instance)
output_dir: ./output
```

Top-level export options:

| Key | Type | Default | Applies to | Description |
|---|---|---|---|---|
| `app_path` | path | `None` | app export | Output directory for a standalone datannur app |
| `output_dir` | path | `None` | db export | Output directory for JSON metadata only |
| `open_browser` | bool | `false` | app export | Open the generated app in the browser after export |
| `refresh` | bool | `false` | scan | Force a full rescan instead of reusing unchanged files or tables |
| `track_evolution` | bool | `true` | app + db export | Write `evolution.json` with added, updated, and deleted entities |
| `update_app` | bool | `false` | app export | Refresh bundled front-end app files when `app_path` already exists |
| `copy_assets` | rule or list of rules | `None` | app + db export | Copy extra local files or directories into the export |
| `export_size_report` | bool | `false` | app + db export | Print a per-table size report after writing the database |
| `post_export` | script name, path, or list | `None` | app + db export | Run Python scripts after export finishes |

Set `export_size_report: true` to print a size report by table after writing the database. The report includes raw `.json`, raw `.json.js`, and estimated gzipped `.json` sizes with percentages, which helps identify the tables that dominate catalog weight.

### Large exports

Large catalogs are usually dominated by `frequency` and `value`, because those tables store repeated values for many variables. Enable `export_size_report` to check which tables matter before changing scan settings.

If the export is larger than expected, the main levers are scan depth, frequency generation, automatic enumeration generation, and sampling. `depth: stat` keeps variable statistics without writing frequency tables or enumerations; `depth: variable` keeps only schema-level metadata; `auto_enumerations: false` keeps `depth: value` frequency tables but skips automatic enumeration entities and generated variable links; `freq_threshold` controls when high-cardinality string columns switch from value frequencies to pattern frequencies; `sample_size` limits the rows used for frequency counts and automatic enumeration detection while keeping core statistics on the full dataset.

`.json.js` reflects local or shared-folder usage (`file://`), `.json` reflects uncompressed HTTP, and `.json.gz` reflects HTTP served with gzip.

### Dataset previews

At `stat` and `value` depth, datannurpy exports small dataset previews by default. Database-only exports write them to `<output_dir>/preview/<dataset_id>.json` and `<output_dir>/preview/<dataset_id>.json.js`; app exports place the same files under `data/db/preview/`. The JSON file is an array of row objects, and the JSON-JS file uses `jsonjs.data['<dataset_id>']`, matching the metadata table convention.

Use `preview_rows` to control the maximum number of rows per dataset. The default is `100`; set `preview_rows: 0` or `preview_rows: false` globally or on an individual `add` entry to disable previews for sensitive sources. Previews are not collected at `dataset` or `variable` depth, because those modes do not read data rows.

## Incremental scan


Re-run with the same `app_path` to only rescan changed files (compares mtime) or tables (compares schema + row count):

```yaml
app_path: ./my-catalog

add:
  - folder: ./data               # skips unchanged files
```

Use `refresh: true` to force a full rescan.

Existing app exports update `data/db` by default and preserve local app state under `data/`. To refresh the bundled front-end app files after upgrading datannurpy, set `update_app: true` or call `catalog.export_app(update_app=True)`.

When `app_path/data/db-ui` exists, it is loaded automatically as the last metadata source before export. See [Manual metadata](/metadata) for merge ordering and overlay instructions.

## Evolution tracking


Changes between exports are automatically tracked in `evolution.json`:

- **add**: new folder, dataset, variable, enumeration, etc.
- **update**: modified field (shows old and new value)
- **delete**: removed entity

Cascade filtering: when a parent entity is added or deleted, its children are automatically filtered out to reduce noise. For example, adding a new dataset won't generate separate entries for each variable.

Disable tracking:

```yaml
track_evolution: false
```

## copy_assets


Copy local files or directories into the exported catalog during export:

```yaml
copy_assets:
  - from: ./staging/docs
    to: data/doc
    include: "*.pdf"
    clean: true

  - from: ./data
    to: data/source
```

Rules:

- `from` is resolved relative to the YAML config directory
- `to` is resolved relative to the export directory and must stay inside it
- directories are copied recursively; single files are copied into the destination directory
- `include` is optional and accepts a glob string or list of globs
- `clean: true` removes destination files that are not present in the filtered source set
- copies are incremental: a file is updated only when it is missing, its size changed, or its source `mtime` is newer

Works with both `app_path` and `output_dir` exports.

## post_export


Run Python scripts automatically after export:

```yaml
# Single script (bare name → app/scripts/python/start_app.py when bundled,
# with fallback to python-scripts/start_app.py)
post_export: start_app

# Multiple scripts
post_export:
  - export_dcat
  - start_app
```

Script resolution:

| Format | Resolved path |
|---|---|
| `start_app` | `{output}/app/scripts/python/start_app.py` if present, otherwise `{output}/python-scripts/start_app.py` |
| `hook.py` | `{config_dir}/hook.py` |
| `scripts/hook.py` | `{config_dir}/scripts/hook.py` |
| `/absolute/path.py` | `/absolute/path.py` |

Explicit script paths are resolved relative to the YAML config file directory, like the other path-based options.

For app exports, `copy_assets` runs after the app shell is installed and before `data/db` is written, so copied files can participate in the final database export. It also runs before `post_export`, so custom scripts can consume copied files.

The bundled `export_dcat` script writes semantic export artifacts to `data/db-semantic/`, including DCAT RDF files, `validation.json`, and `dcat-report.html` when supported by the app version.

Works with both `app_path` and `output_dir` exports.

