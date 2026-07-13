# Scansione dei file

Usare la [Profondità di scansione](/it/scan-depth) per scegliere quanti metadati datannurpy deve estrarre. La stessa impostazione `depth` si applica a `add_folder`, `add_dataset`, `add_database` e `add_geodatabase`, a livello globale o per singola voce `add`.

## Scansione dei file


```yaml
add:
  # Scansiona una cartella (CSV, Excel, SAS)
  - folder: ./data

  # Con metadati personalizzati per la cartella
  - folder: ./data
    id: prod
    name: Production

  # Con opzioni di filtraggio
  - folder: ./data
    include: ["*.csv", "*.xlsx"]
    exclude: ["**/tmp/**"]
    recursive: true
    csv_encoding: utf-8        # oppure cp1252, iso-8859-1 (rilevato automaticamente per impostazione predefinita)

  # Più cartelle con opzioni condivise
  - folder: [./data/sales, ./data/hr]
    include: ["*.csv"]

  # Un singolo file
  - dataset: ./data/sales.csv

  # Più file
  - dataset:
      - ./data/sales.csv
      - ./data/products.csv
```

### Pattern di filtraggio

I pattern `include` ed `exclude` vengono confrontati con i percorsi relativi normalizzati rispetto alla cartella scansionata. I percorsi usano il separatore `/` su ogni piattaforma, e un `/` o `./` iniziale nei pattern viene ignorato. Il filtraggio prima mantiene i file che corrispondono ad almeno un pattern `include` quando `include` è impostato, poi rimuove i file che corrispondono a un qualsiasi pattern `exclude`.

Esempi:

| Pattern | Significato |
| ------- | ----------- |
| `name.csv` | File esatto nella radice della cartella scansionata |
| `subdir/name.csv` | Percorso relativo esatto del file |
| `*.csv` | Qualsiasi file CSV a qualsiasi profondità |
| `subdir/*.csv` | File CSV direttamente dentro `subdir` |
| `**/tmp/**` | File sotto qualsiasi directory `tmp` |
| `tmp/` | Tutto ciò che è sotto la directory `tmp` nella radice |
| `**/tmp/` | Tutto ciò che è sotto qualsiasi directory chiamata `tmp` |

### Rilevamento delle serie temporali

Quando `time_series: true` (predefinito), i file con pattern temporali nei loro nomi o nelle cartelle padre vengono raggruppati automaticamente in un unico dataset:

```
data/
├── enquete_2020.csv    ─┐
├── enquete_2021.csv     ├─→ Un unico dataset "enquete" con nb_resources=3
├── enquete_2022.csv    ─┘
└── reference.csv       ─→ Dataset separato "reference"
```

Il dataset risultante include `nb_resources`, `start_date` ed `end_date`. Le variabili tengono traccia dei propri `start_date` ed `end_date` quando la loro presenza cambia tra i periodi.

Impostare `time_series: false` per trattare ogni file come un dataset separato.

Vedere [Raggruppamento di serie temporali](/it/time-series) per i pattern supportati, il raggruppamento delle tabelle di database, l'evoluzione dello schema e le regole contro i falsi positivi.

## Formati Parquet


Sono supportati sia file Parquet semplici sia dataset partizionati (Delta, Hive, Iceberg):

```yaml
add:
  # add_folder rileva automaticamente tutti i formati
  - folder: ./data             # scansiona *.parquet + directory Delta/Hive/Iceberg

  # Singola directory partizionata con override dei metadati
  - dataset: ./data/sales_delta
    name: Sales Data
    description: Monthly sales
    folder:
      id: sales
      name: Sales
```

Con gli extra `[delta]` e `[iceberg]`, i metadati (nome, descrizione, documentazione delle colonne) vengono estratti quando disponibili.

## Formati geospaziali {#geospatial-formats}

datannurpy scansiona file geospaziali vettoriali e raster e arricchisce ogni dataset con metadati spaziali. L'extra `geo` fornisce il lettore vettoriale (pyogrio), il lettore raster (rasterio), la riproiezione CRS (pyproj) e le librerie per le esportazioni ISO 19139 / STAC (pygeometa, pystac):

```bash
pip install datannurpy[geo]
```

GeoPackage e GeoParquet vengono letti senza l'extra; il loro `bbox` viene riproiettato in WGS84 solo quando pyproj è disponibile.

```yaml
add:
  # I file vettoriali e raster vengono rilevati automaticamente da add_folder, come ogni altro formato
  - folder: ./geodata          # *.geojson, *.shp, *.gml, *.kml, *.gpx, *.tif, *.parquet, *.gpkg, *.zip

  # Un singolo file geospaziale
  - dataset: ./geodata/parcels.shp

  # Un GeoPackage è un contenitore SQLite — un dataset per layer/tabella. Rilevato
  # anche dalle scansioni folder; una voce esplicita (ad esempio con metadati)
  # prevale sul rilevamento, qualunque sia l'ordine delle voci.
  - database: sqlite:///./geodata/cadastre.gpkg

  # Un ESRI File Geodatabase è un contenitore multi-layer — un dataset per layer
  - geodatabase: ./geodata/cadastre.gdb
```

| Formato | Estensione | Aggiunto con |
| ------- | ---------- | ------------ |
| GeoJSON | `.geojson` | `folder` / `dataset` |
| Shapefile | `.shp` (+ `.shx`/`.dbf`/`.prj`) | `folder` / `dataset` |
| Shapefile zippato | `.zip` (un `.shp` + file collaterali) | `folder` / `dataset` |
| GML | `.gml` | `folder` / `dataset` |
| KML | `.kml` | `folder` / `dataset` |
| GPX | `.gpx` | `folder` / `dataset` |
| GeoTIFF (raster) | `.tif`, `.tiff` | `folder` / `dataset` |
| GeoParquet | `.parquet` | `folder` / `dataset` |
| GeoPackage | `.gpkg` (anche zippato) | `folder` / `database: sqlite:///…` |
| ESRI File Geodatabase | `.gdb` (anche zippata) | `geodatabase` / `folder` (zippata) |

Uno Shapefile viene spesso distribuito come un unico `.zip` (IGN, Census TIGER, Eurostat …). È sufficiente puntare una voce `dataset:` al file zip: lo Shapefile interno viene estratto e scansionato come uno normale — riproiezione CRS inclusa — su qualsiasi sorgente:

```yaml
add:
  - dataset: https://data.example.org/ADMIN-EXPRESS_FRA.zip
```

Lo stesso funziona per qualsiasi [archivio zip](#archivi-zip) contenente esattamente un file scansionabile.

I file GPX, KML e GML possono contenere più layer (un GPX espone sempre waypoint, route e track); scansionati come singolo `dataset:`, viene letto il primo layer non vuoto — una registrazione di traccia senza waypoint restituisce quindi comunque i suoi track.

### Metadati spaziali

Ogni dataset spaziale acquisisce questi campi (lasciati null per i dati non spaziali):

| Campo | Descrizione |
| ----- | ----------- |
| `crs` | Sistema di riferimento delle coordinate nativo, ad es. `EPSG:2056` |
| `geometry_type` | Tipo di geometria OGC (`point`, `polygon`, …); null per raster e layer misti |
| `bbox` | Bounding box `west,south,east,north` in WGS84 (EPSG:4326), ordine lon/lat |
| `spatial_resolution` | Dimensione del pixel raster in metri (solo CRS proiettati); null per i vettoriali |

Per i layer vettoriali, le colonne attributo diventano variabili con lo schema e le statistiche consueti; la geometria stessa è mantenuta come variabile binaria non profilata. Per i raster, ogni banda diventa una variabile (`type: band`) con i propri min/max/mean/std.

### Contenitori multi-layer

GeoPackage e File Geodatabase contengono più layer, quindi ogni layer diventa un dataset a sé sotto una cartella contenitore — esattamente come le tabelle di database. Un GeoPackage è un file SQLite, quindi viene aggiunto tramite `database:` (`sqlite:///…`); un File Geodatabase è una directory GDAL, quindi ha la propria voce `geodatabase:` (oppure `catalog.add_geodatabase(path)` in Python). Entrambi accettano pattern glob `include`/`exclude` per filtrare i layer per nome:

```yaml
add:
  - geodatabase: ./geodata/cadastre.gdb
    include: ["parcels_*"]      # solo i layer che iniziano con parcels_
    exclude: ["*_tmp"]
```

Le sorgenti geospaziali funzionano sullo [storage remoto](#remote-storage) come qualsiasi altro file: gli Shapefile remoti recuperano automaticamente i file collaterali `.shx`/`.dbf`/`.prj`, e una directory File Geodatabase remota viene scaricata prima della scansione.

## Opzioni CSV


Evitare la copia temporanea UTF-8 quando i file sono già locali e in UTF-8 (fallback automatico se il rilevamento dell'encoding fallisce):

```yaml
csv_skip_copy: true
```

### CSV sporchi

Quando una colonna non può essere convertita nel tipo rilevato (una riga di nota a piè di pagina in una colonna numerica, formati di data misti), la scansione degrada invece di fallire. Prima riprova scartando le righe non convertibili — accettato solo se marginale (al massimo 10 righe o lo 0,1%, e mai l'intero file), con un avviso che indica il conteggio esatto — poi ripiega sulla lettura di tutte le colonne come testo: tutte le righe analizzabili sopravvivono, le statistiche tipizzate sono semplicemente assenti. I valori non vengono mai alterati: un dataset degradato è incompleto o non tipizzato, mai sbagliato. Un file che fallisce anche questo viene saltato con un avviso, come prima.

## CSV compressi

Un CSV compresso con gzip (`.csv.gz`) viene scansionato come qualsiasi altro file — su qualsiasi sorgente, a qualsiasi [profondità](/it/scan-depth) — e viene catalogato esattamente come il suo gemello non compresso (stessi `id`/`name`, stesse variabili):

```yaml
add:
  - dataset: ./data/sales.csv.gz
  - folder: sftp://user@host/exports    # *.csv.gz accanto a *.csv
```

Per `.gz` è supportato soltanto il **gzip di CSV** a flusso singolo; `.parquet.gz` non lo è — estrarlo prima. La compressione a livello di trasporto HTTP (`Content-Encoding: gzip`) è gestita automaticamente e non richiede nulla di particolare.

## Archivi zip

Un `.zip` contenente esattamente un file di dati — un file CSV, Excel, ODS o Parquet, oppure uno Shapefile con i suoi file collaterali — viene scansionato come `dataset:` su qualsiasi sorgente, e le scansioni `folder:` rilevano questi archivi automaticamente. Il membro viene estratto in modo sicuro (con protezione contro Zip Slip e bombe di decompressione) e scansionato come un file normale, a qualsiasi [profondità](/it/scan-depth):

```yaml
add:
  - dataset: https://data.example.org/exports/sales.csv.zip
```

I file di accompagnamento non-dati (una `license.txt`, un PDF, file di stile o di metadati) sono tollerati senza rendere ambiguo l'archivio. Un unico membro chiamato `*.json` il cui contenuto è GeoJSON — la convenzione comune dei portali — viene scansionato come GeoJSON; gli altri JSON vengono saltati silenziosamente.

Le scansioni `folder:` scoprono anche i **container multi-layer zippati**: un archivio contenente un solo albero File Geodatabase (`*.gdb/`) o un solo membro `.gpkg` diventa un dataset per layer sotto una cartella container, esattamente come [GeoPackage e File Geodatabase](#contenitori-multi-layer) stessi. Un archivio invariato viene saltato per mtime senza essere estratto.

Gli archivi con più file di dati non sono supportati — una scansione `folder:` li salta con un avviso per file; in quei casi estrarre prima gli archivi. Usare `exclude: ["*.zip"]` per tenere gli archivi fuori da una scansione.

## Storage remoto {#remote-storage}

Scansione di file su URL HTTP(S) pubblici, server SFTP o cloud storage (S3, Azure, GCS). Il dizionario `storage_options` viene passato direttamente a [fsspec](https://filesystem-spec.readthedocs.io/) — consultare la documentazione del provider per le opzioni disponibili:

- [SFTP](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.sftp.SFTPFileSystem)
- [S3](https://s3fs.readthedocs.io/en/latest/)
- [Azure](https://github.com/fsspec/adlfs)
- [GCS](https://gcsfs.readthedocs.io/en/latest/)

```yaml
env_file: .env               # SFTP_PASSWORD, AWS_KEY, AWS_SECRET, ecc.
```

### SFTP

Richiede `pip install datannurpy[ssh]`.

```yaml
add:
  - folder: sftp://user@host/path/to/data
    storage_options:
      password: ${SFTP_PASSWORD}   # oppure key_filename: /path/to/key
```

### Amazon S3

Richiede `pip install datannurpy[s3]`.

```yaml
add:
  - folder: s3://my-bucket/data
    storage_options:
      key: ${AWS_KEY}
      secret: ${AWS_SECRET}
```

### Azure Blob

Richiede `pip install datannurpy[azure]`.

```yaml
add:
  - folder: az://container/data
    storage_options:
      account_name: ${AZURE_ACCOUNT}
      account_key: ${AZURE_KEY}
```

### Google Cloud Storage

Richiede `pip install datannurpy[gcs]`.

```yaml
add:
  - folder: gs://my-bucket/data
    storage_options:
      token: /path/to/credentials.json
```

### URL HTTP(S) pubblici

Puntare una voce `dataset:` a un URL pubblico per ottenere variabili documentate, statistiche e un'anteprima — il modo naturale per catalogare open data già online (portali di dati, Zenodo, ecc.). Funziona subito, senza extra da installare.

```yaml
add:
  - dataset: https://data.example.org/opendata/sales.csv
```

- Un URL corrisponde a un file — solo `dataset:`; `folder:` (elenco di directory) non è supportato via HTTP.
- Pubblico, senza autenticazione.
- I redirect vengono seguiti; `https://` è consigliato.

**Rilevamento del formato.** Un URL con un'estensione riconosciuta (`.csv`, `.xlsx`, …) funziona senza altro — l'eventuale query string viene ignorata (`.../sales.csv?token=…`). Gli endpoint API spesso non hanno estensione, quindi il formato viene rilevato automaticamente, nell'ordine: l'ultimo segmento del percorso usato come token (`.../HCL_NOGA/multiplelevels/CSV`), un parametro di query `?format=` (o WFS `?outputFormat=`), il `Content-Type` HTTP e infine lo sniffing dei primi byte del contenuto (best-effort, registrato con un avviso; saltato a `depth: dataset`). Quando nulla è conclusivo, l'esecuzione fallisce chiedendo di impostare `format:`.

Impostare `format:` esplicitamente per sovrascrivere il rilevamento (o per forzarlo quando i segnali sono discordanti). Salta inoltre la richiesta di rilevamento — un piccolo guadagno di velocità quando il formato è già noto, che si somma su molti endpoint della stessa API:

```yaml
add:
  # CSV servito da un'API senza estensione nell'URL
  - dataset: https://www.i14y.admin.ch/api/Nomenclatures/HCL_NOGA/multiplelevels/CSV?language=fr
    format: csv
    id: noga_08
  # Excel servito da un endpoint ".../xls?SnapshotDate=..."
  - dataset: https://www.agvchapp.bfs.admin.ch/fr/state/results/xls?SnapshotDate=31.12.2025
    format: excel
    id: commune_district
  # Layer WFS: un URL GetFeature che restituisce GeoJSON è un dataset normale
  # (outputFormat=application/json o json viene rilevato automaticamente come geojson)
  - dataset: https://wfs.geo.example.ch/?service=WFS&version=2.0.0&request=GetFeature&typeName=ns:parcels&outputFormat=application/json
    id: parcels
```

I valori accettati per `format:` sono i formati di consegna (`csv`, `excel`, `ods`, `parquet`, `sas`, `spss`, `stata`, `geojson`, `shapefile`, `gml`, `kml`, `gpx`, `geotiff`) oppure una grafia di estensione corrispondente (`xlsx`, `xls`, `pq`, …).
- Un URL mancante (404, errore DNS, timeout) fa fallire l'esecuzione con un codice di uscita diverso da zero, esattamente come un file locale mancante — una build CI fallisce in rosso invece di pubblicare un catalogo troncato.
- Le scansioni incrementali usano l'header `Last-Modified` del server: un URL viene saltato quando è invariato, e la sua data popola `last_update_date`. Un server che non invia `Last-Modified` (ad es. un endpoint dinamico) viene riscansionato a ogni esecuzione.

### Singolo file remoto

```yaml
add:
  - dataset: s3://my-bucket/data/sales.parquet
    storage_options:
      key: ${AWS_KEY}
      secret: ${AWS_SECRET}
```

## Campionamento


Per impostazione predefinita, `sample_size` è `100000`. Tutte le voci ereditano questo valore. È possibile sovrascriverlo per voce, o impostare `null` per disabilitarlo:

```yaml
sample_size: 100000               # predefinito

add:
  - folder: ./data                # eredita 100000

  - folder: ./small
    sample_size: null             # nessun campionamento

  - database: postgresql://localhost/mydb
    sample_size: 50000            # override
```

Per disabilitare il campionamento a livello globale:

```yaml
sample_size: null
```

Quando un dataset ha più righe di `sample_size`, per i conteggi delle frequenze e il rilevamento automatico delle enumerazioni viene usato un campione casuale uniforme. Tutte le altre statistiche (`nb_row`, `nb_missing`, `nb_distinct`, `min`, `max`, `mean`, `std`) vengono calcolate sull'intero dataset.

Impostare `auto_enumerations: false` per mantenere le tabelle di frequenza di `depth: value` senza creare entità enumerazione automatiche né collegamenti generati alle variabili. Utile quando le enumerazioni sono fornite manualmente tramite `metadata_path`. Come `sample_size`, può essere impostato a livello globale o per voce `add`.

Il numero effettivo di righe campionate viene registrato in `Dataset.sample_size` (`null` quando non è stato applicato alcun campionamento).

## Anteprime dei dataset

Per impostazione predefinita, `preview_rows` è `100`. Alle profondità `stat` e `value`, ogni dataset scansionato esporta fino a quel numero di righe in `preview/<dataset_id>.json` e `preview/<dataset_id>.json.js`. Queste righe provengono, quando possibile, da dati già letti durante la scansione, inclusi i campioni reservoir usati per il rilevamento delle frequenze.

Sovrascrivere il limite per singola sorgente di file, o impostare `false` per disabilitare le anteprime per una sorgente mantenendo il valore predefinito globale:

```yaml
preview_rows: 100

add:
  - folder: ./data/public
    preview_rows: 50

  - dataset: ./data/private.csv
    preview_rows: false
```

Le anteprime sono dati raccolti in fase di scansione. Non vengono generate alle profondità `dataset` o `variable`, e i comandi di esportazione non hanno un override `preview_rows` separato.
