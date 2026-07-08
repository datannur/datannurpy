# Scanning files

Use [Scan depth](/scan-depth) to choose how much metadata datannurpy extracts. The same `depth` setting applies to `add_folder`, `add_dataset`, `add_database`, and `add_geodatabase`, either globally or per `add` entry.

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

## Geospatial formats

datannurpy scans vector and raster geospatial files and enriches each dataset with spatial metadata. The `geo` extra provides the vector reader (pyogrio), the raster reader (rasterio), CRS reprojection (pyproj), and the libraries for the ISO 19139 / STAC exports (pygeometa, pystac):

```bash
pip install datannurpy[geo]
```

GeoPackage and GeoParquet are read without the extra; their `bbox` is reprojected to WGS84 only when pyproj is available.

```yaml
add:
  # Vector and raster files are auto-detected by add_folder, like any other format
  - folder: ./geodata          # *.geojson, *.shp, *.gml, *.kml, *.tif, *.parquet

  # A single geospatial file
  - dataset: ./geodata/parcels.shp

  # A GeoPackage is a SQLite container — scanned as a database
  - database: sqlite:///./geodata/cadastre.gpkg

  # An ESRI File Geodatabase is a multi-layer container — one dataset per layer
  - geodatabase: ./geodata/cadastre.gdb
```

| Format | Extension | Added with |
| ------ | --------- | ---------- |
| GeoJSON | `.geojson` | `folder` / `dataset` |
| Shapefile | `.shp` (+ `.shx`/`.dbf`/`.prj`) | `folder` / `dataset` |
| Zipped Shapefile | `.zip` (one `.shp` + sidecars) | `dataset` |
| GML | `.gml` | `folder` / `dataset` |
| KML | `.kml` | `folder` / `dataset` |
| GeoTIFF (raster) | `.tif`, `.tiff` | `folder` / `dataset` |
| GeoParquet | `.parquet` | `folder` / `dataset` |
| GeoPackage | `.gpkg` | `database: sqlite:///…` |
| ESRI File Geodatabase | `.gdb` | `geodatabase` |

A Shapefile is often distributed as a single `.zip` (IGN, Census TIGER, Eurostat …). Point a `dataset:` at it and the inner Shapefile is extracted and scanned like a plain one — CRS reprojection included — on any source:

```yaml
add:
  - dataset: https://data.example.org/ADMIN-EXPRESS_FRA.zip
```

The archive must hold exactly one Shapefile. Multi-Shapefile archives, and auto-discovery of zips inside a `folder:` scan, are not supported — extract those first.

### Spatial metadata

Each spatial dataset gains these fields (left null for non-spatial data):

| Field | Description |
| ----- | ----------- |
| `crs` | Native coordinate reference system, e.g. `EPSG:2056` |
| `geometry_type` | OGC geometry type (`point`, `polygon`, …); null for rasters and mixed layers |
| `bbox` | Bounding box `west,south,east,north` in WGS84 (EPSG:4326), lon/lat order |
| `spatial_resolution` | Raster pixel size in metres (projected CRS only); null for vectors |

For vector layers, attribute columns become variables with the usual schema and statistics; the geometry itself is kept as an un-profiled binary variable. For rasters, each band becomes a variable (`type: band`) carrying its min/max/mean/std.

### Multi-layer containers

GeoPackage and File Geodatabase hold several layers, so each layer becomes its own dataset under a container folder — exactly like database tables. A GeoPackage is a SQLite file, so it is added through `database:` (`sqlite:///…`); a File Geodatabase is a GDAL directory, so it has its own `geodatabase:` entry (or `catalog.add_geodatabase(path)` in Python). Both accept `include`/`exclude` glob patterns to filter layers by name:

```yaml
add:
  - geodatabase: ./geodata/cadastre.gdb
    include: ["parcels_*"]      # only layers starting with parcels_
    exclude: ["*_tmp"]
```

Geospatial sources work over [remote storage](#remote-storage) like any other file: remote Shapefiles automatically fetch their `.shx`/`.dbf`/`.prj` companions, and a remote File Geodatabase directory is downloaded before scanning.

## CSV options


Avoid the UTF-8 temp copy when files are already local and UTF-8 (auto-fallback if encoding detection fails):

```yaml
csv_skip_copy: true
```

## Compressed CSV

A gzip-compressed CSV (`.csv.gz`) is scanned like any other file — on any source, at any [depth](./scan-depth.md) — and catalogs exactly like its uncompressed twin (same `id`/`name`, same variables):

```yaml
add:
  - dataset: ./data/sales.csv.gz
  - folder: sftp://user@host/exports    # *.csv.gz alongside *.csv
```

Only single-stream **gzip of CSV** is supported; other compressed forms (`.parquet.gz`, `.zip` archives, zipped Shapefiles) are not — extract them first. HTTP transport compression (`Content-Encoding: gzip`) is handled automatically and needs nothing special.

## Remote storage

Scan files on public HTTP(S) URLs, SFTP servers, or cloud storage (S3, Azure, GCS). The `storage_options` dict is passed directly to [fsspec](https://filesystem-spec.readthedocs.io/) — see provider docs for available options:

- [SFTP](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.sftp.SFTPFileSystem)
- [S3](https://s3fs.readthedocs.io/en/latest/)
- [Azure](https://github.com/fsspec/adlfs)
- [GCS](https://gcsfs.readthedocs.io/en/latest/)

```yaml
env_file: .env               # SFTP_PASSWORD, AWS_KEY, AWS_SECRET, etc.
```

### SFTP

Requires `pip install datannurpy[ssh]`.

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

### Public HTTP(S) URLs

Point a `dataset:` at a public URL and get documented variables, stats and a preview — the natural way to catalog open data already online (data portals, Zenodo, etc.). Works out of the box (no extra to install).

```yaml
add:
  - dataset: https://data.example.org/opendata/sales.csv
```

- One URL is one file — `dataset:` only; `folder:` (directory listing) is not supported over HTTP.
- Public, no authentication.
- Redirects are followed; `https://` is recommended.

**Format detection.** A URL with a recognized extension (`.csv`, `.xlsx`, …) just works — any query string is ignored (`.../sales.csv?token=…`). API endpoints often have no extension, so the format is then detected automatically, in order: the last path segment used as a token (`.../HCL_NOGA/multiplelevels/CSV`), a `?format=` query parameter, the HTTP `Content-Type`, and finally content sniffing of the first bytes (best-effort, logged with a warning; skipped at `depth: dataset`). When nothing is conclusive the run fails asking you to set `format:`.

Set `format:` explicitly to override detection (or force it when signals disagree). It also skips the detection request — a small speed-up when you already know the format, worth it across many endpoints of the same API:

```yaml
add:
  # CSV served by an API with no extension in the URL
  - dataset: https://www.i14y.admin.ch/api/Nomenclatures/HCL_NOGA/multiplelevels/CSV?language=fr
    format: csv
    id: noga_08
  # Excel served by a ".../xls?SnapshotDate=..." endpoint
  - dataset: https://www.agvchapp.bfs.admin.ch/fr/state/results/xls?SnapshotDate=31.12.2025
    format: excel
    id: commune_district
```

Accepted `format:` values are the delivery formats (`csv`, `excel`, `parquet`, `sas`, `spss`, `stata`, `geojson`, `shapefile`, `gml`, `kml`, `geotiff`) or a matching extension spelling (`xlsx`, `xls`, `pq`, …).
- A missing URL (404, DNS error, timeout) fails the run with a non-zero exit code, just like a missing local file — a CI build fails red rather than publishing a truncated catalog.
- Incremental scans use the server's `Last-Modified` header: a URL is skipped when it is unchanged, and its date populates `last_update_date`. A server that sends no `Last-Modified` (e.g. a dynamic endpoint) is re-scanned on every run.

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

When a dataset has more rows than `sample_size`, a uniform random sample is used for frequency counts and automatic enumeration detection. All other statistics (`nb_row`, `nb_missing`, `nb_distinct`, `min`, `max`, `mean`, `std`) are computed on the full dataset.

Set `auto_enumerations: false` to keep `depth: value` frequency tables without creating automatic enumeration entities or generated variable links. This is useful when enumerations are provided manually through `metadata_path`. Like `sample_size`, it can be set globally or per `add` entry.

The actual number of sampled rows is recorded in `Dataset.sample_size` (`null` when no sampling was applied).

## Dataset previews

By default, `preview_rows` is `100`. At `stat` and `value` depth, each scanned dataset exports up to that many rows in `preview/<dataset_id>.json` and `preview/<dataset_id>.json.js`. These rows come from data already read during scanning when possible, including reservoir samples used for frequency detection.

Override the limit per file source, or set `false` to disable previews for one source while keeping the global default:

```yaml
preview_rows: 100

add:
  - folder: ./data/public
    preview_rows: 50

  - dataset: ./data/private.csv
    preview_rows: false
```

Previews are scan-time data. They are not generated at `dataset` or `variable` depth, and export commands do not have a separate `preview_rows` override.
