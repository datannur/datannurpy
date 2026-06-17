# Scan depth

The `depth` parameter controls how much metadata is extracted from files, folders, datasets, and databases. Set it once as a global default, or override it for any `add` entry.

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
| Enumerations, frequencies, patterns  |           |            |                | ✓                  |
| Auto-tagging (format, security, text)|           |            |                | ✓                  |

> **Note:** At `depth="variable"`, CSV and Excel files only extract column **names** (types require reading data, available from `depth="stat"`). All other formats provide types at this level.

**Typical use cases:**

- **`dataset`** — quick inventory of available files/tables without reading data
- **`variable`** — lightweight schema discovery (column names and types)
- **`stat`** — data profiling without enumeration detection (faster than `value`)
- **`value`** — full catalog with frequency tables and enumeration assignment (default)

If you need value frequencies but manage enumerations manually through `metadata_path`, use `auto_enumerations: false` with `depth: value`. Frequencies remain available, but automatic enumeration entities and generated variable links are skipped.

## Auto-tagging

At `depth="value"` (default), string columns are automatically tagged by content type. Tags use a two-level hierarchy under the `auto` parent:

| Category   | Tags                                                       |
| ---------- | ---------------------------------------------------------- |
| **Format** | `auto---email`, `auto---phone`, `auto---uuid`, `auto---iban` |
| **Security** | `auto---bcrypt`, `auto---argon2`, `auto---jwt`, `auto---secret` |
| **Text**   | `auto---structured`, `auto---semi-structured`, `auto---free-text` → `auto---natural-text` |

Each variable receives at most **one leaf tag**. The frontend can use `parent_id` to filter by category (e.g. selecting `auto---security` shows all bcrypt/argon2/jwt/secret variables).

**Security-tagged columns** (bcrypt, argon2, jwt, secret) have their raw frequency values suppressed — only pattern frequencies are emitted, so no actual secrets appear in the exported catalog.

## Policy tags

Policy tags let you manually control scan behavior for specific variables. Like auto-tags, they live under the `scan` hierarchy and are auto-created — no `tag.csv` entry needed.

| Tag                      | Effect                                                         |
| ------------------------ | -------------------------------------------------------------- |
| `policy---frequency-hidden`   | Suppress all frequency and enumeration data (stats remain visible) |

Assign the tag in your `variable.csv` metadata:

```csv
id,tag_ids
source---employees_csv---first_name,"policy---frequency-hidden"
```

This is useful for sensitive columns that auto-tagging does not flag (e.g. first names, internal IDs, business codes).
