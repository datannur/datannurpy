# Output & exports

## Output


```yaml
# Complete standalone app
app_path: ./my-catalog
open_browser: true

# JSON metadata only (for existing datannur instance)
output_dir: ./output
```

## Incremental scan


Re-run with the same `app_path` to only rescan changed files (compares mtime) or tables (compares schema + row count):

```yaml
app_path: ./my-catalog

add:
  - folder: ./data               # skips unchanged files
```

Use `refresh: true` to force a full rescan.

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


Copy local files or directories into the exported catalog after export:

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
# Single script (bare name → python-scripts/start_app.py)
post_export: start_app

# Multiple scripts
post_export:
  - export_dcat
  - start_app
```

Script resolution:

| Format | Resolved path |
|---|---|
| `start_app` | `{output}/python-scripts/start_app.py` |
| `hook.py` | `{config_dir}/hook.py` |
| `scripts/hook.py` | `{config_dir}/scripts/hook.py` |
| `/absolute/path.py` | `/absolute/path.py` |

Explicit script paths are resolved relative to the YAML config file directory, like the other path-based options.

`copy_assets` runs before `post_export`, so custom scripts can consume copied files.

Works with both `app_path` and `output_dir` exports.

