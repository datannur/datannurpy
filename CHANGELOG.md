# datannurpy

## 0.30.0 (2026-07-12)

- add: ODS spreadsheets (`.ods`) are scanned like Excel on every source and depth (new core dependency `odfpy`)
- add: a `.zip` holding exactly one data file (CSV, Excel, ODS, Parquet …) is scanned as a `dataset:` — generalizes the zipped-Shapefile support, same Zip Slip / zip-bomb guards; multi-file archives stay out of scope
- add: GPX (`.gpx`) joins the vector geo formats (`geo` extra) — the first non-empty layer is scanned, bbox included
- add: WFS `GetFeature` URLs are detected as GeoJSON (`outputFormat=json`/`application/json` on an OGC request); `format`/`fmt`/`outputFormat` query keys are now case-insensitive and accept media types
- fix: multi-layer vector containers scanned as a single dataset no longer emit pyogrio's "more than one layer" warning
- add: messy CSVs degrade instead of failing — on type-conversion errors the scan drops the unconvertible rows (accepted only when marginal, warning with the count), then falls back to all-text columns (every parseable row kept); values are never altered
- fix: zero-row warnings tell the truth — `empty file (0 bytes)` only for genuinely empty files, `no data rows` otherwise; a scanner-reported failure (`✗`) is no longer re-reported as "empty file" and its dataset keeps `nb_row` null instead of a false `0`
- fix: scan errors handled inside a scanner (logged `✗` but invisible to the tally) now count in the run `[summary]` and the `on_scan_error: fail` exit code
- fix: GeoParquet with a PROJJSON CRS (e.g. Swiss LV95) scans on the DuckDB fast path instead of the slower PyArrow fallback, whose debug message no longer embeds the full PROJJSON blob

## 0.29.5 (2026-07-10)

- fix: the run `[summary]` now counts `add_dataset` and `add_geodatabase` sources — a dataset added individually (single file, HTTP URL, zipped Shapefile, partitioned directory, `.gdb` layer) was missing from the `scanned`/`unchanged` tallies, understating them on incremental runs

## 0.29.4 (2026-07-10)

- fix: geo scans no longer emit polars' `geoarrow.wkb` extension-type warning — the annotation is stripped at read time, keeping the raw WKB binary the pipeline expects and immunizing against polars 2.0's `load_as_extension` default; the `POLARS_UNKNOWN_EXTENSION_TYPE_BEHAVIOR` workaround is no longer needed

## 0.29.3 (2026-07-09)

- fix: an invalid metadata table no longer silently discards *all* curation — validation is now per-table, so a broken `dataset.csv` is skipped while a valid `tag.csv`/`variable.csv` still apply (previously one bad file dropped every table)
- add: `on_metadata_error` option (`"warn"` default, `"fail"`) — metadata loading stays continue-on-error, but `on_metadata_error: fail` makes the CLI exit `3` when any metadata table fails validation, so CI no longer publishes a catalogue stripped of its curation green; the default keeps today's tolerant exit `0`
- perf: faster folder scans on network storage (SFTP, S3/GCS/Azure, NFS/SMB mounts) — discovery now lists each directory once and reads file dates from that listing, instead of a separate lookup per file. Biggest gain on incremental runs where nothing changed

## 0.29.2 (2026-07-09)

- fix: security-tagged columns (passwords, hashes, secrets) are now masked in the exported preview — values replaced by a `•••` placeholder, matching the frequency/enumeration suppression that already hid them
- add: `on_scan_error` option (`"warn"` default, `"fail"`) — scanning stays continue-on-error (a failed file/table is logged and skipped), but `on_scan_error: fail` makes the CLI exit `2` when any dataset failed to scan, so CI no longer publishes a truncated catalogue green; the default keeps today's tolerant exit `0`, and a `ConfigError` still exits `1`
- fix: scan logging no longer crashes on a legacy-code-page Windows console — the streams are forced to UTF-8 so the `✓`/`✗`/`⏳` status glyphs (which `cp1252` can't encode) print instead of raising `UnicodeEncodeError`
- fix: Windows support — reconcile `\`/`/` separators in `FileSystem`, metadata match keys and database URIs (`sqlite:///C:/…`), name partitioned datasets from their folder, and tolerate a temp-file lock in the statistical scanner
- ci: add a required Windows job (Python 3.13) on `windows-latest`, closing a Linux-only blind spot; local Iceberg tables stay unsupported on Windows (upstream pyiceberg limitation)

## 0.29.1 (2026-07-08)

- change: HTTP(S) URL scanning now requires Python ≥ 3.10 — aiohttp's CVE fixes are 3.14+ only (which dropped 3.9), so aiohttp is installed only on 3.10+; on 3.9 an HTTP URL raises a clear error and every other feature keeps working
- fix: the per-source scan summary reports real `scanned`/`unchanged` counts instead of net deltas — no more misleading `0 datasets, 0 variables` on an all-unchanged run
- add: a whole-run `[summary]` line at export — catalogue totals plus this run's `scanned`/`unchanged`/`errors`, aggregated across all sources
- add: a zipped Shapefile (`.zip` with one `.shp` + sidecars) is scanned as a single `dataset:` on any source — extracted safely (Zip Slip- and bomb-guarded) and read like a plain Shapefile, CRS included; multi-Shapefile zips and `folder:` auto-discovery stay out of scope
- add: gzip-compressed CSV (`.csv.gz`) is scanned transparently on any source and depth — no longer silently skipped by `folder` scans, catalogued like its uncompressed twin, and bounded against decompression bombs; other compressed forms (`.parquet.gz`, `.zip`) stay out of scope
- add: an HTTP(S) `dataset:` without a usable extension now auto-detects its format (path-segment token, `?format=`, `Content-Type`, then content sniffing); set `format:` to override, and a `?query` after a known extension is handled too
- fix: HTTP/API URLs with a query string are now scanned correctly — the temp download uses fsspec `get_file` (was `download`, which created a directory for such URLs → empty scan) under a safe name carrying the resolved format's extension, so suffix-based readers (Excel engine, pyogrio) work
- fix: the default `id`/`name` for a URL dataset strip the query string (readable name) and add a short URL hash to the id, so endpoints differing only by query string no longer collide
- change: `data_size` is left empty (None) instead of `0` when the server sends no `Content-Length`
- change: a `401`/`403` on an HTTP `dataset:` reports an authentication-required error (and a `5xx` a server error) instead of a misleading "not found"
- add: HTTP `dataset:` sources are resilient in unattended runs (e.g. a GitHub Action) — a transient failure (connection blip, timeout, `5xx`/`429`) is retried with exponential backoff, and a hung endpoint fails fast (30s connect / 60s read timeout) instead of blocking on aiohttp's 5-minute default; a user-supplied `storage_options.client_kwargs` timeout is respected
- add: incremental scans use the `ETag` as an extra freshness signal for an HTTP `dataset:` — an endpoint sending an `ETag` but no `Last-Modified` (dynamic API) is skipped when unchanged instead of re-scanned every run; when both are present the scan re-runs if *either* changes (so a sub-second change a 1s-granular `Last-Modified` would miss is still caught)
- perf: a remote file's metadata is now read once per scan instead of ~4× — `info()` is memoized on the filesystem, cutting the redundant HEAD requests (HTTP/S3/SFTP)

## 0.29.0 (2026-07-07)

- add: scan a public HTTP(S) URL as a single `dataset:` (e.g. `dataset: https://.../data.csv`) — full scan pipeline, URL exported as `data_path`, format from the extension, redirects followed, a missing URL failing loudly like a missing local file; `aiohttp` is now a core dependency so it works out of the box
- add: incremental scans of HTTP(S) URLs use the server's `Last-Modified` header — an unchanged URL is skipped (and it fills `last_update_date`); a source without it is always re-scanned

## 0.28.0 (2026-07-06)

- fix: an emptied metadata cell (or removed column) now falls back to the scanned value instead of keeping the previous override — empty means "no value from this source", not "no change"; `!` still clears the final value
- change: the incremental base is now a scan-only cache under `data/db/_scan/`, so each publish rebuilds `data/db` from the current scan + metadata and never reuses its own overlays as the next run's base — the first run after upgrade rebuilds `_scan` with one full rescan

## 0.27.4 (2026-06-30)

- docs: document the metadata-first `_match_path` pattern for each period type in the time-series supported-patterns table
- fix: a missing folder in `add_folder()` now logs one clean, redacted error line instead of being absent from the log or showing traceback noise / `NoneType: None`
- fix: variables orphaned by a deleted dataset (e.g. a metadata `variable.csv` row re-asserting a variable whose dataset is gone) are now swept at export, so the catalog stays referentially consistent instead of keeping phantom variables
- docs: document the mirror model (each run reflects current scan + metadata) and the parent-vs-child deletion semantics

## 0.27.3 (2026-06-27)

- fix: `value` and `frequency` tables no longer duplicate every row on each incremental run — reload now rebuilds their runtime ids with the same hashed builders used to match existing rows

## 0.27.2 (2026-06-24)

- fix: on a second run, unchanged metadata-first time series are skipped instead of failing with a duplicate-id error — incremental planning now matches them on their normalized series key, not only the latest physical file

## 0.27.1 (2026-06-22)

- fix: in metadata-first mode, a `[YYYY/MM]` `_match_path` no longer collapses onto a yearly `[YYYY]` series sharing the same filename skeleton — period match keys now carry the series frequency, so each granularity matches only its own group
- fix: db-only configs (`output_dir` without `app_path`) now load the previous database from `output_dir` for incremental scans, so unchanged datasets are skipped instead of rescanned every run (unless `refresh: true`)

## 0.27.0 (2026-06-20)

- add: extract geo metadata into new dataset fields `crs`, `geometry_type`, and a WGS84 `bbox` array `[west, south, east, north]` — from GeoPackage `gpkg_*` tables and GeoParquet `geo` metadata (no dependency); `bbox` reprojection uses the optional `geo` extra (pyproj). Requires jsonjsdb >= 0.9.1 (serializes numeric list columns as JSON arrays)
- add: scan vector geo files — GeoJSON (`.geojson`), Shapefile (`.shp`), GML (`.gml`), KML (`.kml`) — via the optional `geo` extra (pyogrio); attributes get the usual schema/stats and the layer's `crs`/`geometry_type`/`bbox` populate the dataset (Shapefile sidecars fold into the one dataset)
- add: scan GeoTIFF rasters (`.tif`/`.tiff`) via the optional `geo` extra (rasterio) — one variable per band (`type` `band`) with min/max/mean/std, plus the dataset's `crs`/`bbox` and a new `spatial_resolution` field (pixel size in metres for a projected CRS)
- add: `catalog.add_geodatabase(path)` scans an ESRI File Geodatabase (`.gdb`) via the optional `geo` extra (pyogrio) — each layer becomes a dataset (with schema/stats and `crs`/`geometry_type`/`bbox`) nested under a container folder, mirroring `add_database`; works on local paths and remote URLs (SFTP/S3/…), and remote Shapefiles now download their `.shx`/`.dbf`/`.prj` companions
- add: the `geo` extra also installs `pygeometa` and `pystac`, so the app's ISO 19139 and STAC exports work out of the box (DCAT already works via the core `rdflib`/`pyshacl`)

## 0.26.7 (2026-06-19)

- perf: faster metadata loading via jsonjsdb 0.9.0 partial reads and single-rebuild upserts, cheaper localized-column detection, and a single metadata folder scan
- perf: batch preview flags, skipped-dataset updates, and enumeration value creation to avoid per-row table rebuilds
- fix: match local/UNC time-series `_match_path` keys with `[YYYY]` placeholders by adding an absolute normalized series candidate, mirroring the canonical remote URL candidate already used for SFTP

## 0.26.6 (2026-06-17)

- add: support `auto_enumerations: false` to keep value frequencies while disabling automatic enumeration creation and variable links
- fix: support metadata-first matching with remote paths and logical time-series `_match_path` keys while keeping `data_path` as the public exported path

## 0.26.5 (2026-06-16)

- fix: resolve relative links in exported Markdown document content from the original source document directory using browser-compatible local paths
- perf: reduce redundant filesystem metadata calls during exports, preview synchronization, copy assets cleanup, metadata loading, and local path validation

## 0.26.4 (2026-06-15)

- fix: resolve relative links in exported Markdown document content from the original source document directory
- perf: avoid per-dataset preview file probes when preview exports are disabled or can be discovered from the preview directory
- perf: cache repeated metadata match path existence checks during dataset metadata loading
- perf: batch Markdown document export hash metadata through jsonjsdb
- change: make the export size report opt-in with `export_size_report`
- security: constrain cryptography to 48.0.1 or newer in uv dependency resolution

## 0.26.3 (2026-06-14)

- fix: use jsonjsdb 0.8.10 to preserve JSON hash manifest entries for subdirectory exports
- fix: preserve explicit empty `value.value` metadata cells as enumeration values
- fix: rewrite relative links in exported Markdown document content

## 0.26.2 (2026-06-10)

- fix: preserve literal `!` values in value and frequency metadata composite keys
- fix: preserve folder metadata fields declared in the app schema during metadata import
- fix: trim metadata identity and scalar string fields during metadata import
- fix: normalize integer-like float values from tabular metadata readers during metadata import
- fix: keep database scanner type checks compatible with Pyright 1.1.410
- fix: omit default `is_pattern: false` values from variable metadata exports
- fix: use jsonjsdb 0.8.9 paired JSON writers for Markdown document exports

## 0.26.1 (2026-06-09)

- update: bundle the latest datannur front app with UI and static export fixes

## 0.26.0 (2026-06-04)

- add: preserve localized metadata fields using the front app `field:lang` convention (for example `name:fr` and `description:fr`) through metadata loading and JSON app exports

## 0.25.2 (2026-05-28)

- add: include bundled DCAT semantic export dependencies and documentation for `export_dcat` post-export artifacts
- add: enrich the editorial demo overlay with realistic JSON metadata so DCAT coverage reaches 100% for required publication fields
- fix: delay config-driven `open_browser` until after `post_export` scripts so first-run apps detect generated semantic artifacts
- fix: bundled DCAT exporter accepts slash-separated datetime update values as XML Schema `dateTime`

## 0.25.1 (2026-05-26)

- fix: update the README banner URL

## 0.25.0 (2026-05-26)

- breaking: adapt to the new embedded front app layout under `app/`, including scripts, templates, and static deploy paths
- fix: adapt demo and app export helpers to the embedded front app layout under `app/`
- fix: align technical dataset/evolution fields with the front schema contract (`preview_rows`, `last_update_date`, `schema_signature`, `parent_entity`)
- fix: require `jsonjsdb` 0.8.8 so `parent_entity` remains internal to evolution tracking exports
- add: support `configFilter` metadata table exports for app-level filters
- fix: preserve bundled demo `db-source/` and `pdf/` assets when refreshing the embedded front app

## 0.24.1 (2026-05-21)

- change: rename organization link fields to `manager_organization_id` and `owner_organization_id`
- change: rename variable relation fields to `source_variable_ids` and `fk_variable_id`

## 0.24.0 (2026-05-21)

- add: support variable `business_key` and tag `implied_tag_ids` / `propagate_to_parents` metadata fields
- change: app tooling now uses Python scripts instead of Node.js for static export/deploy, OpenAPI generation, and REST API launch helpers
- build: constrain `uv` dependency resolution to packages published at least 3 days ago
- perf: keep `datannurpy` imports lightweight and avoid runtime imports for CLI `--help` / `--version`
- fix: `export_app()` now preserves local app `data/` state by default and refreshes bundled app files only on first install or with `update_app=True`
- fix: reuse bulk cascade cleanup when removing variables, enumerations, tags, docs, concepts, and organizations
- add: support app metadata overlays via `app_path/data/db-ui`, loaded as the last metadata source when it exists
- add: support metadata overlay instructions: `!` clears fields, `!id` removes relation IDs, and `_delete` tombstones remove entities before export, including folder cascades

## 0.23.1 (2026-05-07)

- doc: clarify builder docs for scan depth, metadata sources, remote storage extras, and database introspection support
- fix: ui app preview and about page screenshot

## 0.23.0 (2026-05-05)

- add: export dataset previews as JSON and JSON-JS files, controlled by global and per-source `preview_rows`
- add: datasets now expose `has_preview` when preview files are available
- fix: metadata `dataset._match_path` now strictly overrides `data_path` for scan matching, allowing exported `data_path` to remain a public URL
- fix: local file scans now export portable `data_path` values while keeping absolute `_match_path` values for runtime matching

## 0.22.4 (2026-05-04)

- change: `include`/`exclude` filtering now uses documented glob semantics for folder paths and database table names
- perf: `depth: variable` scans now avoid remote ZIP reads and redundant latest-file rescans for `.xlsx` time-series
- doc: move scan depth into a dedicated cross-cutting guide and link it from file and database scanning docs
- fix: scanner and metadata warnings now keep relative path labels instead of falling back to file basenames
- fix: Excel parser diagnostics from openpyxl/xlrd are now captured as debug log details instead of leaking as raw terminal output

## 0.22.3 (2026-05-01)

- add: non-quiet exports now print a per-table size report for `.json`, `.json.js`, and gzipped `.json` output
- improve: log prefix spacing is now consistent after leading icons across scan, metadata, export, and post-export output
- fix: log_file now writes in UTF-8 in datannurpy.utils.log
- improve: folder and database scan logs now use clearer resource labels and summaries, with root-relative paths for folder scans and file/table counts shown before dataset totals
- fix: time-series scans now canonicalize header aliases before variable ID build, avoiding duplicate variable IDs from encoding-damaged column names
- fix: time-series grouping now handles more realistic file/table layouts while rejecting incomplete no-year periods
- doc: add shared time-series grouping guide and a `make check-py39` validation target
- fix: `.xls` files that are actually HTML reports are now detected early and skipped cleanly as untreatable, including in schema-only and remote scans

## 0.22.2 (2026-04-30)

- fix: non-tabular and malformed CSVs are now skipped cleanly as untreatable (pre-flight check, `strict_mode=false` retry, targeted DuckDB catch) instead of crashing or being mislabeled as empty
- fix: CSV scanner recognizes common missing-value strings (`NA`, `N/A`, `NULL`, `NaN`, …) as NULL so a stray `NA` no longer breaks numeric type inference
- fix: GeoParquet fallback now triggers on any unparseable CRS (e.g. `OGC:CRS84`, `EPSG:4326`), not only projjson — detection is based on the file's `b"geo"` schema metadata
- change: GeoParquet CRS fallback message demoted from warning to debug — only printed when `verbose=True` (still recorded in `log_file`)
- add: `log_debug()` helper in `datannurpy.utils`

## 0.22.1 (2026-04-30)

- fix: GeoParquet files with a projjson CRS no longer crash the scan — `scan_simple` falls back to a PyArrow read when DuckDB/Ibis cannot parse the `GEOMETRY('<projjson>')` type
- fix: autotag content types `md5`, `sha1`, `sha256`, `sha512` are now registered in the tag tree (previously detected but unattached)

## 0.22.0 (2026-04-30)

- change: `export_db` / `export_app` omit columns that are entirely null/empty from the exported JSON files
- add: public `copy_assets()` helper and `copy_assets` / `base_dir` parameters on `export_db()` / `export_app()`
- add: `license` fields on folders and datasets, with matching kwargs and export support
- change: scan APIs now use shared `EntityMetadata` via `metadata=` in Python and YAML
- add: warning when a remote scan exceeds the materialization cap (suggests `sample_size`)
- fix: folder discovery skips unreadable subdirectories (e.g. SFTP ACLs) instead of aborting
- fix: `add_folder(depth="dataset")` logs each dataset like other depth modes
- fix: time series detection — recognizes compact `YYYYMMDD`, handles 4-digit years sharing a constant 2-digit token, and splits mixed yearly/quarterly/monthly/daily granularities
- fix: `include`/`exclude` as a bare string no longer iterates character-by-character
- perf: `depth: value` materializes the source once (≤1M rows) and shares it across autotag, frequency, and pattern passes
- perf: frequency value counts via PyArrow (~27× on wide datasets); cleaner float/timestamp formatting
- perf: pattern frequency fully vectorized in PyArrow (~1.2×)
- perf: incremental scan batched — `_seen` updates, `mark_datasets_seen`, `_match_path` index, `finalize` cascade, orphan-tag detection, and `last_update_date` from cached mtime
- perf: `add_database` re-applies cached metadata for unchanged tables in one batch
- perf: enumeration assignment from frequencies looks up columns in O(1)

## 0.21.0 (2026-04-27)

- add: `copy_assets` export option — copy local files or directories into the exported catalog with optional `include` filters and `clean` cleanup
- change: rename `institution` to `organization`, `freq` to `frequency`, and `modality` to `enumeration`
- fix: `post_export` resolves explicit script paths relative to the YAML config directory; bare names still target `python-scripts/`
- fix: incremental file refresh now keeps matching unchanged datasets after reload when `metadata/dataset.csv` provides relative `data_path` values
- fix: export effective `sample_size` for sampled file-based scans (`dataset.json`)

## 0.20.0 (2026-04-26)

- add: metadata-first pattern — `add_folder(create_folders=False)` skips folder creation from disk, reuses `id`/`folder_id` from `metadata/dataset.csv` (matched by `data_path`). New `on_unmatched` parameter (`"skip"` / `"warn"` / `"error"`)
- add: `Folder` exposes `manager_organization_id` and `owner_organization_id` (organization links), with matching kwargs on `add_folder` / `add_database`
- fix: `add_metadata` formats dates as `YYYY/MM/DD` (was ISO-8601), aligned with `get_mtime_iso`
- fix: CSV header parsing on `depth: variable` — BOM, `;` separator with commas in headers, multi-line quoted fields
- fix: CSV header deduplication (DuckDB-style `name_1`, `name_2`) and bare `\r` normalization (Mac Classic / SDMX exports)
- internal: `Dataset._match_path` runtime field for scan↔metadata matching (not exported)

## 0.19.2 (2026-04-25)

- fix: `add_metadata` coerces `datetime` / `pd.Timestamp` to ISO-8601 string (CSV/Excel date columns auto-inferred by DuckDB broke `export_db` / `export_app`)
- fix: time series detection requires a 4-digit year (suffixes like `MONTH12`/`MONTH13` no longer falsely grouped)
- fix: pattern detection validates `\p{L}` probe output and falls back to DuckDB on Oracle (accents no longer collapse to `?`)

## 0.19.1 (2026-04-24)

- perf: `add_metadata` now O(N) via batched `add_all` / `remove_all`
- perf: batched variable updates in `update_cached_metadata` and `resolve_foreign_keys` (DB introspection of cached tables)
- fix: `add_metadata` skips `pd.NA` / `pd.NaT` (fully-empty nullable columns)

## 0.19.0 (2026-04-23)

- add: VitePress documentation site under `docs/` — flat structure (one page per section) sourced from the README, published to [docs.datannur.com/builder](https://docs.datannur.com/builder/) via `npm run docs:release`
- doc: slim README focused on intro, quick start, and links to the full doc site; detailed guides now live in VitePress

## 0.18.0 (2026-04-22)

- breaking: `paramiko` moved to optional `[ssh]` extra — SSH tunneling to remote databases now requires `pip install datannurpy[ssh]`. Reduces the core supply-chain surface for users scanning only local files or DuckDB.
- add: CycloneDX SBOM (`*-sbom.cyclonedx.json`) published as a GitHub Release asset on each version, listing all dependencies, versions, licenses and hashes for CVE audits (Dependency-Track, Grype…)
- doc: air-gapped install recipe in README (`pip download` + `pip install --no-index --find-links`) for environments behind strict corporate proxies

## 0.17.1 (2026-04-21)

- add: `concept` entity (business glossary) and `concept_id` field on `Variable` — `concept.{csv,xlsx,json}` in `metadata_path` is now loaded instead of ignored
- add: `metadata_path` accepts a list — sources are applied in order (list fields unioned, scalars overridden)
- add: auto-load `config.{csv,xlsx,json}` (columns `id`, `value`) from `metadata_path` when `app_config` is not set
- add: `make download-app` preserves `db-source/` into `examples/datannur_app/` for the editorial demo

## 0.17.0 (2026-04-19)

- add: `policy---freq-hidden` tag — assign to a variable in metadata to suppress all frequency and enumeration data while keeping stats; tag is auto-created in the `scan > policy` hierarchy like other system tags
- breaking: `add_metadata()` removed from public API — metadata is now loaded automatically via `metadata_path` parameter on `Catalog()` (also supported as top-level YAML field)
- improve: cleaner scan logging — per-dataset timing, progress indicator (`⏳`), better visual spacing between schemas/summaries, and log file no longer contains intermediate `...` lines
- add: auto-detect AVS13 (Swiss social security number), MD5, SHA1, SHA256, SHA512 content types
- fix: JWT false positives on URLs, secret detection for short tokens, phone format coverage
- perf: batch database introspection — `batch_table_data_size` and `batch_table_row_count` fetch size/count for all tables in a single query per schema instead of per table
- perf: batched enumeration assignment — `assign_from_freq` processes all variables at once via `add_all()` instead of one at a time

## 0.16.2 (2026-04-17)

- add: `scan` root tag grouping all system-generated tags (`auto` and `db` are now children of `scan`)
- add: orphan scan tag cleanup in `finalize` — unreferenced `auto---*` and `db---*` tags are removed, keeping only those actually used by entities
- add: `post_export` config option — run Python scripts automatically after export (`post_export: generate_links` or a list)
- fix: `_extract_period_from_segment` position sort used `str.find()` which returns the first occurrence — duplicate year values in a segment caused incorrect sort order, breaking time series detection
- fix: empty strings in string columns now treated as missing values — `nullif("")` applied before aggregation, fixing `nb_missing`, `nb_distinct`, freq tables, and string length stats
- fix: `env_file` and `.env` are no longer mutually exclusive — both are loaded; `env_file` now supports a list of paths
- breaking: `env:` YAML now takes priority over `.env` file (previously `.env` won); system env vars still win over everything
- fix: Oracle pattern frequencies full of `?` — `\-` in regex charset replaced with portable `-` at end, and NULL bytes (`\x00`) stripped before pattern analysis

## 0.16.1 (2026-04-16)

- add: auto-tagging of string columns by content type (email, phone, UUID, IBAN, bcrypt, argon2, JWT, secret, natural text) with hierarchical `auto---*` tags; security-tagged columns have raw frequency values suppressed
- add: pattern frequency analysis for high-cardinality string columns (`nb_distinct > freq_threshold`) with `is_pattern` flag

## 0.16.0 (2026-04-16)

- breaking: YAML config — `export_app:` and `export_db:` blocks replaced by flat top-level keys (`app_path` implies app export, `output_dir` for JSON-only export, `open_browser` and `track_evolution` as top-level options)
- add: `export_app()` now accepts `track_evolution` parameter
- fix: `export_app()` no longer calls `finalize()` when no scan was performed — reloading a catalog from disk then re-exporting the app no longer empties it
- fix: remote parquet scan now includes `data_size` in results (was missing due to code duplication)
- refactor: deduplicate full-scan dispatch — local and remote paths now share `_scan_local()`
- add: YAML shorthand format for `add` entries (`- folder: ./data` instead of `- type: folder\n  path: ./data`)
- add: `id`, `name`, `description` kwargs on `add_folder` and `add_database`

## 0.15.0 (2026-04-14)

- breaking: renamed depth levels — `"structure"` → `"dataset"`, `"schema"` → `"variable"`, `"full"` → `"value"`
- add: new `depth="stat"` level — computes statistics (row count, min, max, mean, etc.) without enumeration detection or frequency tables
- removed `infer_stats` parameter from `add_folder`, `add_dataset`, `add_database` — use `depth` to control scan level
- fix: variable types preserved during sampling — `build_variables` now reads schema from original source table instead of degraded memtable
- fix: `_READSTAT_TYPE_MAP` expanded to cover `int8`, `int16`, `int32`, `float` (Stata/SPSS types now map to standard `"integer"`/`"float"`)
- fix: `group_table_time_series` now applies `_refine_group` — constant digits in table prefixes (e.g. `03` in `PREFIX03`) are no longer falsely detected as period components
- fix: `_extract_period_from_segment` returns matches in string position order — placeholder↔position alignment is now correct
- fix: Oracle `get_table_data_size` always null — ORA-00942 on `ALL_SEGMENTS` killed fallback to `USER_SEGMENTS`; now tries `DBA_SEGMENTS` → `ALL_SEGMENTS` → `USER_SEGMENTS` with per-view error handling
- fix: Oracle `list_schemas` / `list_tables` now try `DBA_TABLES` → `ALL_TABLES` before `USER_TABLES` — works for users with `SELECT ANY DICTIONARY` privilege
- fix: `make test-db` now installs database drivers automatically (`uv sync --extra databases`)

## 0.14.2 (2026-04-06)

- add: `freq` entity support in `add_metadata` — frequency tables can be loaded from CSV/Excel/database metadata files
- security: SSH tunnel now rejects unknown host keys (`RejectPolicy`) with actionable `ConfigError` guiding users to add the key via `ssh <host>`
- security: SSH tunnels now use `WarningPolicy` instead of `AutoAddPolicy` — prevents silent MITM key acceptance
- security: Oracle SQL queries use bind variables (`:name`) instead of string interpolation
- security: non-Oracle SQL introspection queries use `_quote()` escaping for safe interpolation
- security: GitHub Actions pinned by commit SHA across all workflows
- fix: unified type strings — floating-point types now consistently return `"float"` instead of `"number"` (aligns with frontend `varTypes`)
- refactor: replaced `pyarrow_type_to_str` with `ibis.Schema.from_pyarrow` + `ibis_type_to_str` — single type conversion path
- perf: shared test fixtures, lazy pandas imports, xdist tuned to 4 workers

## 0.14.1 (2026-04-05)

- perf: batch database introspection — one set of system catalog queries per schema instead of per table
- fix: MySQL connection now forwards extra URL parameters (SSL options, charset, etc.) to the driver
- fix: MySQL `CAST(... AS DOUBLE)` replaced with implicit float conversion for MySQL 5.x compatibility
- fix: database folder name and ID no longer include URL query parameters (e.g. `?ssl_mode=DISABLED`)
- fix: credentials and query parameters stripped from `data_path` in exported database folders
- fix: MySQL PK introspection no longer leaks across tables (constraint_name is always `PRIMARY`)
- fix: clearer error message when database connection fails
- fix: database credentials with special characters (`@`, `#`, `$`, etc.) now handled correctly across connection, tunnel, and folder naming

## 0.14.0 (2026-04-04)

- add: `add_database` automatically extracts structural metadata (primary keys, foreign keys, table/column comments, constraint tags) from system catalogs when `depth="schema"` or `"full"`
- add: `ssh_tunnel` parameter on `add_database` for databases behind firewalls (paramiko-based)
- add: `time_series` parameter on `add_database` — groups temporal tables (e.g. `stats_2023`, `stats_2024`) into a single dataset with period tracking per variable
- add: time series prefix grouping now uses effective table list — series datasets are placed in the correct prefix folder instead of year-specific subfolders
- fix: `ibis_type_to_str` now maps `dt.Binary` to `"binary"` — MySQL `BINARY(n)`, `VARBINARY`, `BLOB`, `LONGBLOB` were reported as `"unknown"`
- fix: frequency table `ibis.union` fallback to `pa.concat_tables` for MySQL mixed collations
- fix: enumeration value/freq IDs now use hash-based keys instead of `sanitize_id` — eliminates ID collisions for values with special characters (JSON, URLs with `$@~`)
- fix: YAML config `log_file` path now resolved relative to config file directory
- fix: `ibis_type_to_str` now resolves `dt.Unknown` columns with known `raw_type` (e.g. MySQL `DOUBLE` → `"float"`, `TINYINT`/`MEDIUMINT` → `"integer"`) — previously reported as `"unknown"` with no stats

## 0.13.3 (2026-04-03)

- fix: YAML config now enforces `add_metadata` execution after all other `add` entries, ensuring manual metadata always overrides auto-scanned values

## 0.13.2 (2026-04-02)

- add: skip non-dataset Excel files (reports, pivots, merged cells) via header validation

## 0.13.1 (2026-04-02)

- fix: time series period detection now works at group level — folder dates (e.g. `old_2024_08/data_2018.csv`) no longer override file dates, and constant dates in identifiers are ignored
- fix: Oracle sampling now uses native `SAMPLE(pct)` clause — `ibis.random()` was evaluated once as a scalar, returning all or no rows
- fix: Oracle tables no longer scanned twice when connected user's schema is already listed — `get_schemas_to_scan` now deduplicates via `SELECT USER FROM DUAL`
- fix: Oracle `data_size` falls back to `user_segments` when `all_segments` returns NULL (insufficient privileges)
- fix: `log_file` now captures all log levels (warnings, skip, progress, summary) — previously only errors were written to the file
- fix: `depth: "schema"` now sets variable types for SAS/SPSS/Stata files — previously all types were `null` (local and remote)
- fix: Excel mixed-type columns now preserve `NaN` as missing instead of converting to `"nan"` string
- fix: nulls excluded from frequency tables — `nb_missing` already carries that information

## 0.13.0 (2026-03-31)

- add: `data_size` field on `Dataset` — data size in bytes (files, database tables, and partitioned datasets)
- refactor: SAS/SPSS/Stata scanner rewritten with streaming Parquet pipeline — constant RAM regardless of file size, fixes OOM on large files (10 Go+)
- add: `sample_size` support for statistical files (SAS/SPSS/Stata) — same reservoir sampling as CSV
- add: `sample_size=100_000` default on `Catalog` — all methods inherit it, override per-method with an int or disable with `None`
- add: `sample_size` support for Parquet, Delta, Hive, and Iceberg — DuckDB reservoir sampling, same pattern as CSV/databases
- rename: `skip_copy` → `csv_skip_copy` (applies only to CSV)

## 0.12.1 (2026-03-31)

- add: `verbose` and `log_file` parameters on `Catalog` for error diagnostics (full tracebacks on stderr or to file)
- fix: scanner errors now use `log_error` instead of `log_warn` — consistent error reporting with traceback support
- fix: Iceberg scan now works regardless of the caller's working directory

## 0.12.0 (2026-03-29)

- remove: `datannurpy` console_scripts entry point — use `python -m datannurpy` instead
- add: CLI supports `--help` and `--version` flags
- fix: ID conflict when scanning same database with different schemas in separate `add_database` calls
- fix: Oracle DATE/TIMESTAMP columns now get `nb_distinct`/`nb_missing` stats from `build_variables`
- fix: remote file warnings now show original filename instead of temporary path
- fix: skip columns with empty names from trailing CSV separators or blank Excel headers
- fix: remote CSV schema-only scan streams header line instead of fixed 4KB partial download
- refactor: remove Oracle date/timestamp stats raw SQL fallback — date columns are now fully skipped for stats on Oracle (simplifies sampling support)
- add: `sample_size` parameter on `add_database` for uniform random sampling on large tables — exact stats on full table, cardinality and freq on sample
- refactor: CSV scanner rewritten with DuckDB streaming — constant RAM regardless of file size, automatic UTF-8/cp1252 handling
- add: `sample_size` on `add_folder` / `add_dataset` and `csv_skip_copy` on `Catalog` / `add_folder` / `add_dataset` for CSV files

## 0.11.0 (2026-03-26)

- add: `app_config` parameter on `Catalog` — pass a `dict[str, str]` to populate a `config.json` in the exported database (e.g. `contact_email`, `banner`, `more_info`)

## 0.10.2 (2026-03-23)

- add: `add_folder` and `add_dataset` accept a list of paths, `add_database` accepts a list of schemas — shared options apply to all entries
- add: `env:` section in YAML config for reusable non-sensitive variables (expanded alongside `env_file` and `.env`)
- fix: Oracle date/timestamp stats computed via raw SQL — bypasses ibis `ExtractEpochSeconds` unsupported operation
- fix: skip `std` computation when a single distinct value, skip all stats when all values are missing
- fix: introduce `ConfigError` for user-facing errors — CLI no longer masks internal bugs (`ValueError`/`FileNotFoundError` from libraries) and now catches `TypeError` from parameter typos
- fix: `last_update_date` for database tables is `None` on first scan instead of current date — populated only when a change is detected

## 0.10.1 (2026-03-22)

- add: new `"geometry"` variable type for spatial columns (POINT, POLYGON, etc. from GeoPackage, PostGIS, etc.)
- fix: Excel date-only columns (all values at midnight) now correctly detected as `"date"` instead of `"datetime"`

## 0.10.0 (2026-03-22)

- add: `min`, `max`, `mean`, `std` statistics on Variable (numeric, string length, date as epoch seconds)
- add: graceful fallback when loading a stale/incompatible database emits a warning and starts fresh instead of crashing
- fix: `refresh=True` now skips loading the existing database entirely
- fix: date statistics no longer overflow on dates before 1901 (epoch_seconds cast to int64)

## 0.9.8 (2026-03-20)

- fix: remote `depth="schema"` SAS/Stata scan no longer fails — use pandas streaming instead of partial download (pyreadstat needs full file)
- fix: remote `depth="schema"` SPSS scan falls back to full download (no streaming support)
- fix: remote `depth="schema"` Excel xlsx reads only headers via openpyxl streaming (no full download)

## 0.9.7 (2026-03-20)

- fix: Oracle crash on connection close (`DPY-1001: not connected`) — `close_connection` now ignores errors when internal connection is already closed
- fix: `depth="structure"` for `add_database` no longer runs `COUNT(*)` and schema introspection on every table — just enumerates tables like `add_folder`

## 0.9.6 (2026-03-20)

- add: validate unknown parameters with closest match suggestion on all public methods
- add: scan continues on file/table error instead of crashing — errors logged and reported in summary
- fix: Excel files with mixed-type columns (`int`, `bytes`, etc.) no longer crash — fallback converts `object` columns to `str`
- fix: `depth="schema"` Excel scan now reads headers only (`nrows=0`) instead of loading entire file
- fix: `depth="schema"` no longer assigns fake `type="string"` to CSV/Excel variables — uses `None` when type is unknown

## 0.9.5 (2026-03-19)

- fix: Oracle < 23 compatibility (`ORA-00923`) — use `con.sql()` instead of `con.table()` to avoid boolean expressions unsupported before Oracle 23
- ci: test Oracle on both 18c and 23c via matrix strategy

## 0.9.4 (2026-03-19)

- add: `.env` file support for YAML config (expand `$VAR` / `${VAR}` in all values, optional `env_file` key)
- fix: add missing Oracle system schemas to exclusion list
- fix: remote SFTP/S3 scans returning 0 datasets on Windows (`Path()` converting `/` to `\`)

## 0.9.3 (2026-03-17)

- add: Oracle TNS name support via `oracle_client_path` parameter in `add_database()` (thick mode)
- fix: CSV scanner now uses Polars instead of DuckDB for cross-platform encoding support (Windows CP1252)
- chore: CI now tests minimum and maximum dependency versions

## 0.9.2 (2026-03-16)

- refactor: replace `warnings.warn` with quiet-aware `log_warn` and clean CLI errors
- chore: bump jsonjsdb to >=0.8.3

## 0.9.1 (2026-03-16)

- fix: add missing `fsspec` explicit dependency for remote storage
- fix: auto-exclude common non-data dirs (`.git`, `.venv`, `__pycache__`, `.ipynb_checkpoints`, etc.) and `~$*` temp files from scans

## 0.9.0 (2026-03-12)

- add: YAML configuration support with `run_config()`
- add: CLI with `python -m datannurpy config.yml`
- add: `datannurpy` entry point for venv usage
- add: `type: dataset` support in YAML config

## 0.8.0 (2026-03-12)

- add: `time_series` parameter for `add_folder` to group temporal files (e.g., `data_2020.csv`, `data_2021.csv`) into single datasets
- add: `nb_files` field on Dataset schema for time series file count
- add: variable period tracking (`start_date`, `end_date`) based on presence across periods

## 0.7.0 (2026-03-08)

- add: remote storage support for `add_folder`, `add_dataset`, `add_database`
- add: `storage_options` parameter for SFTP/S3/Azure/GCS credentials
- add: `add_database` now supports remote SQLite/GeoPackage files
- add: `depth="schema"` optimization for remote files (minimal downloads)

## 0.6.0 (2026-03-08)

- add: `depth` parameter for `add_dataset` and `add_metadata`
- refactor: `depth="structure"` now clears variable/enumeration/value/freq at load time

## 0.5.0 (2026-03-07)

- add: evolution cascade filtering via parent_relations (jsonjsdb 0.8.2)
- add: Value.id and Freq.id are now runtime fields (not persisted)
- fix: evolution stability - no spurious changes on successive runs
- fix: preserve last_update_timestamp for unchanged database tables
- fix: \_enumerations folder no longer deleted on incremental scan
- refactor: \_compute_runtime_id helper for id column computation

## 0.4.2 (2026-03-05)

- add: jsonjsdb as runtime dependency
- refactor: migrate internal storage to jsonjsdb
- remove: `entities/` module (consolidated into `schema.py`)
- remove: `importer/db.py` and `exporter/db.py` (replaced by jsonjsdb)

## 0.4.1 (2026-02-01)

- add: e2e test with demo data and db export
- add: incremental scan with db import

## 0.4.0 (2026-01-29)

- add: manual metadata merging with add_metadata

## 0.3.2 (2026-01-29)

- add: test coverage with codecov and improve test coverage to 100%
- fix: Iceberg scanner no longer requires network access (replaced DuckDB extension with PyIceberg)
- refactor: remove all methodes from catalog.py and rename writer to exporter
- refactor: remove \_ prefix and close db and file connection

## 0.3.1 (2026-01-25)

- add: progress logging with quiet option
- change: improve error messages
- fix: Excel reader no longer requires network access (removed DuckDB spatial dependency)
- fix: resolve relative path before opening browser in export_app
- fix: csv encoding fallback to handle utf-8, cp1252 and iso-8859-1
- refactor: make catalog.py as facade

## 0.3.0 (2026-01-22)

- add: enumeration support
- add: folder based on db table prefix
- add: support for parquet files and Delta/Hive/Iceberg directories
- add: support for SAS, SPSS and Stata files
- add: mssql db support
- add: gpkg support (sqlite based)

## 0.2.1 (2026-01-21)

- add: oracle db support

## 0.2.0 (2026-01-20)

- add: db scan support and use ibis instead of polars

## 0.1.3 (2026-01-19)

- fix: include visualization app in package

## 0.1.2 (2026-01-19)

- change: use static Python version badge in README

## 0.1.1 (2026-01-19)

- add: Python version classifiers for PyPI badges

## 0.1.0 (2026-01-19)

- initial release
