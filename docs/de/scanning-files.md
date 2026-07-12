# Dateien scannen

Über die [Scan-Tiefe](/de/scan-depth) wird festgelegt, wie viele Metadaten datannurpy extrahiert. Dieselbe `depth`-Einstellung gilt für `add_folder`, `add_dataset`, `add_database` und `add_geodatabase`, entweder global oder pro `add`-Eintrag.

## Dateien scannen


```yaml
add:
  # Einen Ordner scannen (CSV, Excel, SAS)
  - folder: ./data

  # Mit eigenen Ordner-Metadaten
  - folder: ./data
    id: prod
    name: Production

  # Mit Filteroptionen
  - folder: ./data
    include: ["*.csv", "*.xlsx"]
    exclude: ["**/tmp/**"]
    recursive: true
    csv_encoding: utf-8        # oder cp1252, iso-8859-1 (standardmäßig automatisch erkannt)

  # Mehrere Ordner mit gemeinsamen Optionen
  - folder: [./data/sales, ./data/hr]
    include: ["*.csv"]

  # Eine einzelne Datei
  - dataset: ./data/sales.csv

  # Mehrere Dateien
  - dataset:
      - ./data/sales.csv
      - ./data/products.csv
```

### Filtermuster

`include`- und `exclude`-Muster werden gegen normalisierte relative Pfade ab dem gescannten Ordner abgeglichen. Pfade verwenden auf jeder Plattform `/` als Trennzeichen; führende `/` oder `./` in Mustern werden ignoriert. Die Filterung behält zuerst Dateien, die mindestens einem `include`-Muster entsprechen (sofern `include` gesetzt ist), und entfernt anschließend Dateien, die einem `exclude`-Muster entsprechen.

Beispiele:

| Muster | Bedeutung |
| ------- | ------- |
| `name.csv` | Exakte Datei im Wurzelverzeichnis des gescannten Ordners |
| `subdir/name.csv` | Exakter relativer Dateipfad |
| `*.csv` | Jede CSV-Datei in beliebiger Tiefe |
| `subdir/*.csv` | CSV-Dateien direkt in `subdir` |
| `**/tmp/**` | Dateien unterhalb eines beliebigen `tmp`-Verzeichnisses |
| `tmp/` | Alles unterhalb des `tmp`-Verzeichnisses im Wurzelverzeichnis |
| `**/tmp/` | Alles unterhalb eines beliebigen Verzeichnisses namens `tmp` |

### Zeitreihen-Erkennung

Bei `time_series: true` (Standard) werden Dateien mit zeitlichen Mustern in ihren Namen oder übergeordneten Ordnern automatisch zu einem einzigen Datensatz gruppiert:

```
data/
├── enquete_2020.csv    ─┐
├── enquete_2021.csv     ├─→ Single dataset "enquete" with nb_resources=3
├── enquete_2022.csv    ─┘
└── reference.csv       ─→ Separate dataset "reference"
```

Der resultierende Datensatz enthält `nb_resources`, `start_date` und `end_date`. Variablen führen ihre eigenen `start_date`- und `end_date`-Werte, wenn sich ihr Vorkommen über die Perioden hinweg ändert.

Mit `time_series: false` wird jede Datei als eigener Datensatz behandelt.

Siehe [Zeitreihen-Gruppierung](/de/time-series) für unterstützte Muster, die Gruppierung von Datenbanktabellen, Schema-Evolution und Regeln gegen Fehlerkennungen.

## Parquet-Formate


Unterstützt einfache Parquet-Dateien und partitionierte Datensätze (Delta, Hive, Iceberg):

```yaml
add:
  # add_folder erkennt alle Formate automatisch
  - folder: ./data             # scannt *.parquet + Delta/Hive/Iceberg-Verzeichnisse

  # Einzelnes partitioniertes Verzeichnis mit Metadaten-Überschreibung
  - dataset: ./data/sales_delta
    name: Sales Data
    description: Monthly sales
    folder:
      id: sales
      name: Sales
```

Mit den Extras `[delta]` und `[iceberg]` werden Metadaten (Name, Beschreibung, Spaltendokumentation) extrahiert, sofern verfügbar.

## Geodatenformate {#geospatial-formats}

datannurpy scannt Vektor- und Raster-Geodateien und reichert jeden Datensatz mit räumlichen Metadaten an. Das `geo`-Extra liefert den Vektor-Reader (pyogrio), den Raster-Reader (rasterio), die CRS-Reprojektion (pyproj) und die Bibliotheken für die ISO 19139 / STAC-Exporte (pygeometa, pystac):

```bash
pip install datannurpy[geo]
```

GeoPackage und GeoParquet werden auch ohne das Extra gelesen; ihre `bbox` wird nur dann nach WGS84 reprojiziert, wenn pyproj verfügbar ist.

```yaml
add:
  # Vektor- und Rasterdateien werden von add_folder automatisch erkannt, wie jedes andere Format
  - folder: ./geodata          # *.geojson, *.shp, *.gml, *.kml, *.gpx, *.tif, *.parquet

  # Eine einzelne Geodatei
  - dataset: ./geodata/parcels.shp

  # Ein GeoPackage ist ein SQLite-Container — wird als Datenbank gescannt
  - database: sqlite:///./geodata/cadastre.gpkg

  # Eine ESRI File Geodatabase ist ein Multi-Layer-Container — ein Datensatz pro Layer
  - geodatabase: ./geodata/cadastre.gdb
```

| Format | Erweiterung | Hinzugefügt über |
| ------ | --------- | ---------- |
| GeoJSON | `.geojson` | `folder` / `dataset` |
| Shapefile | `.shp` (+ `.shx`/`.dbf`/`.prj`) | `folder` / `dataset` |
| Gezipptes Shapefile | `.zip` (ein `.shp` + Begleitdateien) | `dataset` |
| GML | `.gml` | `folder` / `dataset` |
| KML | `.kml` | `folder` / `dataset` |
| GPX | `.gpx` | `folder` / `dataset` |
| GeoTIFF (Raster) | `.tif`, `.tiff` | `folder` / `dataset` |
| GeoParquet | `.parquet` | `folder` / `dataset` |
| GeoPackage | `.gpkg` | `database: sqlite:///…` |
| ESRI File Geodatabase | `.gdb` | `geodatabase` |

Ein Shapefile wird oft als einzelne `.zip`-Datei verteilt (IGN, Census TIGER, Eurostat …). Ein `dataset:` darauf zeigen lassen — das enthaltene Shapefile wird extrahiert und wie ein gewöhnliches gescannt, inklusive CRS-Reprojektion, auf jeder Quelle:

```yaml
add:
  - dataset: https://data.example.org/ADMIN-EXPRESS_FRA.zip
```

Dasselbe funktioniert für jedes [Zip-Archiv](#zip-archive), das genau eine scannbare Datei enthält.

GPX-, KML- und GML-Dateien können mehrere Layer enthalten (GPX stellt immer Waypoints, Routen und Tracks bereit); beim Scannen als einzelnes `dataset:` wird der erste nicht-leere Layer gelesen — eine Track-Aufzeichnung ohne Waypoints liefert also weiterhin ihre Tracks.

### Räumliche Metadaten

Jeder räumliche Datensatz erhält diese Felder (bei nicht-räumlichen Daten bleiben sie null):

| Feld | Beschreibung |
| ----- | ----------- |
| `crs` | Natives Koordinatenreferenzsystem, z. B. `EPSG:2056` |
| `geometry_type` | OGC-Geometrietyp (`point`, `polygon`, …); null bei Rastern und gemischten Layern |
| `bbox` | Begrenzungsrahmen `west,south,east,north` in WGS84 (EPSG:4326), Reihenfolge Lon/Lat |
| `spatial_resolution` | Raster-Pixelgröße in Metern (nur projizierte CRS); null bei Vektoren |

Bei Vektor-Layern werden Attributspalten zu Variablen mit dem üblichen Schema und den üblichen Statistiken; die Geometrie selbst bleibt als nicht profilierte Binärvariable erhalten. Bei Rastern wird jedes Band zu einer Variablen (`type: band`) mit min/max/mean/std.

### Multi-Layer-Container

GeoPackage und File Geodatabase enthalten mehrere Layer, daher wird jeder Layer zu einem eigenen Datensatz unter einem Container-Ordner — genau wie Datenbanktabellen. Ein GeoPackage ist eine SQLite-Datei und wird deshalb über `database:` hinzugefügt (`sqlite:///…`); eine File Geodatabase ist ein GDAL-Verzeichnis und hat ihren eigenen `geodatabase:`-Eintrag (oder `catalog.add_geodatabase(path)` in Python). Beide akzeptieren `include`-/`exclude`-Glob-Muster zum Filtern von Layern nach Name:

```yaml
add:
  - geodatabase: ./geodata/cadastre.gdb
    include: ["parcels_*"]      # nur Layer, die mit parcels_ beginnen
    exclude: ["*_tmp"]
```

Geodatenquellen funktionieren über [Remote-Speicher](#remote-storage) wie jede andere Datei: Entfernte Shapefiles laden automatisch ihre `.shx`-/`.dbf`-/`.prj`-Begleitdateien, und ein entferntes File-Geodatabase-Verzeichnis wird vor dem Scannen heruntergeladen.

## CSV-Optionen


Die temporäre UTF-8-Kopie vermeiden, wenn Dateien bereits lokal und UTF-8-kodiert sind (automatischer Fallback, falls die Kodierungserkennung fehlschlägt):

```yaml
csv_skip_copy: true
```

## Komprimierte CSV-Dateien

Eine gzip-komprimierte CSV-Datei (`.csv.gz`) wird wie jede andere Datei gescannt — auf jeder Quelle, in jeder [Tiefe](/de/scan-depth) — und katalogisiert sich exakt wie ihr unkomprimiertes Gegenstück (gleiche `id`/`name`, gleiche Variablen):

```yaml
add:
  - dataset: ./data/sales.csv.gz
  - folder: sftp://user@host/exports    # *.csv.gz neben *.csv
```

Für `.gz` wird nur Single-Stream-**gzip von CSV** unterstützt; `.parquet.gz` nicht — diese Datei zuerst entpacken. HTTP-Transportkompression (`Content-Encoding: gzip`) wird automatisch behandelt und erfordert nichts Besonderes.

## Zip-Archive

Eine `.zip`-Datei, die genau eine Datendatei enthält — eine CSV-, Excel-, ODS- oder Parquet-Datei oder ein Shapefile mit seinen Begleitdateien —, wird auf jeder Quelle als `dataset:` gescannt. Das enthaltene Element wird sicher extrahiert (geschützt gegen Zip Slip und Dekompressionsbomben) und wie eine gewöhnliche Datei gescannt, in jeder [Tiefe](/de/scan-depth):

```yaml
add:
  - dataset: https://data.example.org/exports/sales.csv.zip
```

Archive mit mehreren Datendateien sowie die automatische Erkennung von Zips innerhalb eines `folder:`-Scans werden nicht unterstützt — diese zuerst entpacken.

## Remote-Speicher {#remote-storage}

Dateien auf öffentlichen HTTP(S)-URLs, SFTP-Servern oder Cloud-Speicher (S3, Azure, GCS) scannen. Das `storage_options`-Dict wird direkt an [fsspec](https://filesystem-spec.readthedocs.io/) weitergereicht — verfügbare Optionen siehe Anbieter-Dokumentation:

- [SFTP](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.sftp.SFTPFileSystem)
- [S3](https://s3fs.readthedocs.io/en/latest/)
- [Azure](https://github.com/fsspec/adlfs)
- [GCS](https://gcsfs.readthedocs.io/en/latest/)

```yaml
env_file: .env               # SFTP_PASSWORD, AWS_KEY, AWS_SECRET, etc.
```

### SFTP

Erfordert `pip install datannurpy[ssh]`.

```yaml
add:
  - folder: sftp://user@host/path/to/data
    storage_options:
      password: ${SFTP_PASSWORD}   # oder key_filename: /path/to/key
```

### Amazon S3

Erfordert `pip install datannurpy[s3]`.

```yaml
add:
  - folder: s3://my-bucket/data
    storage_options:
      key: ${AWS_KEY}
      secret: ${AWS_SECRET}
```

### Azure Blob

Erfordert `pip install datannurpy[azure]`.

```yaml
add:
  - folder: az://container/data
    storage_options:
      account_name: ${AZURE_ACCOUNT}
      account_key: ${AZURE_KEY}
```

### Google Cloud Storage

Erfordert `pip install datannurpy[gcs]`.

```yaml
add:
  - folder: gs://my-bucket/data
    storage_options:
      token: /path/to/credentials.json
```

### Öffentliche HTTP(S)-URLs

Ein `dataset:` auf eine öffentliche URL zeigen lassen und dokumentierte Variablen, Statistiken und eine Vorschau erhalten — der natürliche Weg, bereits online verfügbare Open Data zu katalogisieren (Datenportale, Zenodo usw.). Funktioniert ohne weitere Installation (kein Extra nötig).

```yaml
add:
  - dataset: https://data.example.org/opendata/sales.csv
```

- Eine URL entspricht einer Datei — nur `dataset:`; `folder:` (Verzeichnisauflistung) wird über HTTP nicht unterstützt.
- Öffentlich, ohne Authentifizierung.
- Weiterleitungen werden verfolgt; `https://` wird empfohlen.

**Formaterkennung.** Eine URL mit erkannter Erweiterung (`.csv`, `.xlsx`, …) funktioniert direkt — ein Query-String wird ignoriert (`.../sales.csv?token=…`). API-Endpunkte haben oft keine Erweiterung; das Format wird dann automatisch erkannt, in dieser Reihenfolge: das letzte Pfadsegment als Token (`.../HCL_NOGA/multiplelevels/CSV`), ein `?format=`-Query-Parameter (oder WFS-`?outputFormat=`), der HTTP-`Content-Type` und schließlich Content-Sniffing der ersten Bytes (Best-Effort, mit Warnung protokolliert; bei `depth: dataset` übersprungen). Ist nichts eindeutig, schlägt der Lauf fehl mit der Aufforderung, `format:` zu setzen.

`format:` explizit setzen, um die Erkennung zu überschreiben (oder zu erzwingen, wenn die Signale widersprüchlich sind). Das spart zudem die Erkennungsanfrage — ein kleiner Geschwindigkeitsgewinn, wenn das Format bereits bekannt ist, der sich über viele Endpunkte derselben API lohnt:

```yaml
add:
  # CSV, ausgeliefert von einer API ohne Erweiterung in der URL
  - dataset: https://www.i14y.admin.ch/api/Nomenclatures/HCL_NOGA/multiplelevels/CSV?language=fr
    format: csv
    id: noga_08
  # Excel, ausgeliefert von einem ".../xls?SnapshotDate=..."-Endpunkt
  - dataset: https://www.agvchapp.bfs.admin.ch/fr/state/results/xls?SnapshotDate=31.12.2025
    format: excel
    id: commune_district
  # WFS-Layer: eine GetFeature-URL, die GeoJSON zurückliefert, ist ein gewöhnlicher Datensatz
  # (outputFormat=application/json oder json wird automatisch als geojson erkannt)
  - dataset: https://wfs.geo.example.ch/?service=WFS&version=2.0.0&request=GetFeature&typeName=ns:parcels&outputFormat=application/json
    id: parcels
```

Zulässige `format:`-Werte sind die Auslieferungsformate (`csv`, `excel`, `ods`, `parquet`, `sas`, `spss`, `stata`, `geojson`, `shapefile`, `gml`, `kml`, `gpx`, `geotiff`) oder eine passende Erweiterungsschreibweise (`xlsx`, `xls`, `pq`, …).
- Eine fehlende URL (404, DNS-Fehler, Timeout) lässt den Lauf mit einem Exit-Code ungleich null fehlschlagen, genau wie eine fehlende lokale Datei — ein CI-Build schlägt rot fehl, statt einen unvollständigen Katalog zu veröffentlichen.
- Inkrementelle Scans nutzen den `Last-Modified`-Header des Servers: Eine unveränderte URL wird übersprungen, und ihr Datum füllt `last_update_date`. Ein Server ohne `Last-Modified` (z. B. ein dynamischer Endpunkt) wird bei jedem Lauf neu gescannt.

### Einzelne entfernte Datei

```yaml
add:
  - dataset: s3://my-bucket/data/sales.parquet
    storage_options:
      key: ${AWS_KEY}
      secret: ${AWS_SECRET}
```

## Stichproben (Sampling)


Standardmäßig ist `sample_size` `100000`. Alle Einträge erben diesen Wert. Er kann pro Eintrag überschrieben oder mit `null` deaktiviert werden:

```yaml
sample_size: 100000               # Standard

add:
  - folder: ./data                # erbt 100000

  - folder: ./small
    sample_size: null             # kein Sampling

  - database: postgresql://localhost/mydb
    sample_size: 50000            # Überschreibung
```

Sampling global deaktivieren:

```yaml
sample_size: null
```

Hat ein Datensatz mehr Zeilen als `sample_size`, wird für Häufigkeitszählungen und automatische Enumerationserkennung eine gleichverteilte Zufallsstichprobe verwendet. Alle anderen Statistiken (`nb_row`, `nb_missing`, `nb_distinct`, `min`, `max`, `mean`, `std`) werden auf dem vollständigen Datensatz berechnet.

Mit `auto_enumerations: false` bleiben die Häufigkeitstabellen von `depth: value` erhalten, ohne dass automatische Enumerationsentitäten oder generierte Variablenverknüpfungen erzeugt werden. Das ist nützlich, wenn Enumerationen manuell über `metadata_path` bereitgestellt werden. Wie `sample_size` kann die Option global oder pro `add`-Eintrag gesetzt werden.

Die tatsächliche Anzahl gesampelter Zeilen wird in `Dataset.sample_size` festgehalten (`null`, wenn kein Sampling angewendet wurde).

## Datensatz-Vorschauen

Standardmäßig ist `preview_rows` `100`. Bei den Tiefen `stat` und `value` exportiert jeder gescannte Datensatz bis zu dieser Anzahl Zeilen nach `preview/<dataset_id>.json` und `preview/<dataset_id>.json.js`. Diese Zeilen stammen nach Möglichkeit aus Daten, die bereits während des Scans gelesen wurden, einschließlich der Reservoir-Stichproben für die Häufigkeitserkennung.

Das Limit kann pro Dateiquelle überschrieben oder mit `false` deaktiviert werden, um Vorschauen für eine Quelle abzuschalten und den globalen Standard beizubehalten:

```yaml
preview_rows: 100

add:
  - folder: ./data/public
    preview_rows: 50

  - dataset: ./data/private.csv
    preview_rows: false
```

Vorschauen sind Scan-Zeit-Daten. Sie werden bei den Tiefen `dataset` oder `variable` nicht erzeugt, und Export-Befehle haben keine eigene `preview_rows`-Überschreibung.
