# Python-API

## `Catalog`

```python
Catalog(
    *,
    app_path=None,
    metadata_path=None,
    depth="value",
    refresh=False,
    on_scan_error="warn",
    on_metadata_error="warn",
    freq_threshold=100,
    auto_enumerations=True,
    csv_encoding=None,
    sample_size=100_000,
    preview_rows=100,
    csv_skip_copy=False,
    app_config=None,
    quiet=False,
    verbose=False,
    log_file=None,
)
```

| Attribut       | Typ                               | Beschreibung                                       |
| -------------- | --------------------------------- | -------------------------------------------------- |
| app_path       | str \| Path \| None               | Pfad für den Export der eigenständigen App; enthält er bereits `data/db`, wird er für inkrementelle Scans wiederverwendet |
| metadata_path  | str \| Path \| list[str \| Path] \| None | Metadaten-Quellordner, Datenbank-URI oder Liste von Quellen |
| depth          | "dataset" \| "variable" \| "stat" \| "value" | Standard-Scan-Tiefe (Standard: "value")            |
| refresh        | bool                              | Vollständigen Rescan erzwingen, Cache ignorieren (Standard: False) |
| on_scan_error  | "warn" \| "fail"                  | Das Scannen ist immer Continue-on-Error (eine fehlgeschlagene Datei/Tabelle wird protokolliert und übersprungen). `"warn"` (Standard) beendet mit Exit-Code 0; `"fail"` lässt die CLI mit Exit-Code 2 enden, wenn ein Datensatz fehlgeschlagen ist, damit CI keinen unvollständigen Katalog grün veröffentlicht |
| on_metadata_error | "warn" \| "fail"               | Das Laden von Metadaten ist ebenfalls Continue-on-Error (eine ungültige Tabelle wird protokolliert und übersprungen, gültige Tabellen werden weiterhin angewendet). `"warn"` (Standard) beendet mit Exit-Code 0; `"fail"` lässt die CLI mit Exit-Code 3 enden, wenn eine Metadaten-Tabelle die Validierung nicht bestanden hat, damit CI keinen Katalog ohne seine Kuratierung grün veröffentlicht |
| freq_threshold | int                               | Maximale Anzahl unterschiedlicher Werte für Häufigkeits- und automatische Enumerationserkennung. Strings über diesem Schwellenwert erhalten stattdessen Muster-Häufigkeiten |
| auto_enumerations | bool                           | Automatische Enumerationen aus Wert-Häufigkeiten erzeugen und an Variablen anhängen (Standard: True) |
| csv_encoding   | str \| None                       | Standard-CSV-Kodierung (utf-8, cp1252 usw.)        |
| sample_size    | int \| None                       | Standard-Stichprobengröße für Häufigkeits- und automatische Enumerationserkennung (Standard: 100_000) |
| preview_rows   | int \| Literal[False]             | Standard-Maximum an Zeilen, die in Datensatz-Vorschauen bei den Tiefen `stat`/`value` exportiert werden (Standard: 100; 0 oder false deaktiviert) |
| csv_skip_copy      | bool                              | Temporäre UTF-8-Kopie für lokale CSV überspringen (Standard: False)|
| app_config     | dict[str, str] \| None            | Schlüssel-Wert-Konfiguration für die Web-App       |
| quiet          | bool                              | Fortschritts-Logging unterdrücken (Standard: False)|
| verbose        | bool                              | Vollständige Tracebacks bei Fehlern anzeigen (Standard: False) |
| log_file       | str \| Path \| None               | Vollständiges Scan-Log in Datei schreiben (bei jedem Lauf geleert) |
| folder         | Table[Folder]                     | Ordner-Tabelle (`.all()`, `.count`, `.get_by(...)`) |
| dataset        | Table[Dataset]                    | Datensatz-Tabelle                                  |
| variable       | Table[Variable]                   | Variablen-Tabelle                                  |
| enumeration    | Table[Enumeration]                | Enumerations-Tabelle                               |
| value          | Table[Value]                      | Tabelle der Enumerationswerte                      |
| frequency      | Table[Frequency]                  | Häufigkeitstabelle (berechnet)                     |
| organization   | Table[Organization]               | Organisations-Tabelle |
| tag            | Table[Tag]                        | Tag-Tabelle                                        |
| doc            | Table[Doc]                        | Dokument-Tabelle                                   |
| concept        | Table[Concept]                    | Tabelle der Fachglossar-Konzepte                   |
| config         | Table[Config]                     | Schlüssel-Wert-Tabelle der Web-App-Konfiguration   |

## `Catalog.add_folder()`

```python
catalog.add_folder(
    path,
    metadata=None,
    *,
    depth=None,
    include=None,
    exclude=None,
    recursive=True,
    csv_encoding=None,
    sample_size=None,
    auto_enumerations=None,
    preview_rows=None,
    csv_skip_copy=None,
    storage_options=None,
    refresh=None,
    quiet=None,
    time_series=True,
    create_folders=True,
    on_unmatched="warn",
)
```

| Parameter       | Typ                                       | Standard | Beschreibung                                  |
| --------------- | ----------------------------------------- | -------- | --------------------------------------------- |
| path            | str \| Path \| list[str \| Path]           | erforderlich | Zu scannendes Verzeichnis oder Liste von Verzeichnissen |
| metadata        | EntityMetadata \| None                    | None     | Identität, Elternverknüpfung und Metadaten für den Wurzelordner |
| depth           | "dataset" \| "variable" \| "stat" \| "value" \| None | None     | Scan-Tiefe (verwendet catalog.depth bei None) |
| include         | list[str] \| None                         | None     | Einzuschließende Glob-Muster                  |
| exclude         | list[str] \| None                         | None     | Auszuschließende Glob-Muster                  |
| recursive       | bool                                      | True     | Unterverzeichnisse scannen                    |
| csv_encoding    | str \| None                               | None     | CSV-Kodierung überschreiben                   |
| sample_size     | int \| None                               | None     | Stichprobenzeilen für Häufigkeits- und automatische Enumerationserkennung (überschreibt Katalog) |
| auto_enumerations | bool \| None                            | None     | Automatische Enumerationen aus Wert-Häufigkeiten für diesen Ordner erzeugen (überschreibt Katalog) |
| preview_rows    | int \| Literal[False] \| None              | None     | Maximale Vorschauzeilen für in diesem Ordner gefundene Datensätze (überschreibt Katalog; 0 oder false deaktiviert) |
| csv_skip_copy       | bool \| None                              | None     | Temporäre UTF-8-Kopie überspringen (überschreibt Katalog) |
| storage_options | dict \| None                              | None     | Optionen für Remote-Speicher (an fsspec weitergereicht) |
| refresh         | bool \| None                              | None     | Rescan erzwingen (überschreibt Katalog-Einstellung) |
| quiet           | bool \| None                              | None     | quiet-Einstellung des Katalogs überschreiben  |
| time_series     | bool                                      | True     | Dateien mit zeitlichen Mustern gruppieren     |
| create_folders  | bool                                      | True     | Bei False keine Ordner vom Datenträger erzeugen; Struktur kommt aus `metadata_path` (Metadata-first) |
| on_unmatched    | "skip" \| "warn" \| "error"               | "warn"   | Verhalten, wenn eine gescannte Datei keinen Metadaten-Treffer hat (nur bei `create_folders=False`) |

## `Catalog.add_dataset()`

```python
catalog.add_dataset(
    path,
    *,
    metadata=None,
    depth=None,
    csv_encoding=None,
    sample_size=None,
    auto_enumerations=None,
    preview_rows=None,
    csv_skip_copy=None,
    storage_options=None,
    refresh=None,
    quiet=None,
)
```

| Parameter       | Typ                                       | Standard | Beschreibung                                  |
| --------------- | ----------------------------------------- | -------- | --------------------------------------------- |
| path            | str \| Path \| list[str \| Path]           | erforderlich | Datei(en) oder partitioniertes Verzeichnis (lokal/remote) |
| metadata        | EntityMetadata \| None                    | None     | Datensatz-Identität, Elternverknüpfung und Metadaten |
| depth           | "dataset" \| "variable" \| "stat" \| "value" \| None | None     | Scan-Tiefe (verwendet catalog.depth bei None) |
| csv_encoding    | str \| None                               | None     | CSV-Kodierung überschreiben                   |
| sample_size     | int \| None                               | None     | Stichprobenzeilen für Häufigkeits- und automatische Enumerationserkennung (überschreibt Katalog) |
| auto_enumerations | bool \| None                            | None     | Automatische Enumerationen aus Wert-Häufigkeiten für diesen Datensatz erzeugen (überschreibt Katalog) |
| preview_rows    | int \| Literal[False] \| None              | None     | Maximale Vorschauzeilen für diesen Datensatz (überschreibt Katalog; 0 oder false deaktiviert) |
| csv_skip_copy       | bool \| None                              | None     | Temporäre UTF-8-Kopie überspringen (überschreibt Katalog) |
| storage_options | dict \| None                              | None     | Optionen für Remote-Speicher (an fsspec weitergereicht) |
| refresh         | bool \| None                              | None     | Rescan erzwingen (überschreibt Katalog-Einstellung) |
| quiet           | bool \| None                              | None     | quiet-Einstellung des Katalogs überschreiben  |

## `Catalog.add_database()`

```python
catalog.add_database(
    connection,
    metadata=None,
    *,
    depth=None,
    schema=None,
    include=None,
    exclude=None,
    sample_size=None,
    auto_enumerations=None,
    preview_rows=None,
    group_by_prefix=True,
    prefix_min_tables=2,
    time_series=True,
    storage_options=None,
    refresh=None,
    quiet=None,
    oracle_client_path=None,
    ssh_tunnel=None,
)
```

| Parameter          | Typ                                             | Standard | Beschreibung                               |
| ------------------ | ----------------------------------------------- | -------- | ------------------------------------------ |
| connection         | str \| ibis.BaseBackend                          | erforderlich | Verbindungszeichenfolge oder ibis-Backend-Objekt |
| metadata           | EntityMetadata \| None                          | None     | Identität, Elternverknüpfung und Metadaten für den Wurzelordner |
| depth              | \"dataset\" \| \"variable\" \| \"stat\" \| \"value\" \| None | None     | Scan-Tiefe (verwendet catalog.depth bei None) |
| schema             | str \| list[str] \| None                         | None     | Zu scannende(s) Schema(ta)                 |
| include            | list[str] \| None                               | None     | Gegen Tabellennamen abgeglichene Glob-Muster zum Einschließen |
| exclude            | list[str] \| None                               | None     | Gegen Tabellennamen abgeglichene Glob-Muster zum Ausschließen |
| sample_size        | int \| None                                     | None     | Stichprobenzeilen für Häufigkeits- und automatische Enumerationserkennung (überschreibt Katalog) |
| auto_enumerations  | bool \| None                                    | None     | Automatische Enumerationen aus Wert-Häufigkeiten für gescannte Tabellen erzeugen (überschreibt Katalog) |
| preview_rows       | int \| Literal[False] \| None                    | None     | Maximale Vorschauzeilen für gescannte Tabellen-Datensätze (überschreibt Katalog; 0 oder false deaktiviert) |
| group_by_prefix    | bool \| str                                     | True     | Tabellen nach Präfix in Unterordner gruppieren |
| prefix_min_tables  | int                                             | 2        | Mindestanzahl Tabellen für eine Präfix-Gruppe |
| time_series        | bool                                            | True     | Zeitliche Tabellenmuster erkennen          |
| storage_options    | dict \| None                                    | None     | Optionen für entferntes SQLite/GeoPackage  |
| refresh            | bool \| None                                    | None     | Rescan erzwingen (überschreibt Katalog-Einstellung) |
| quiet              | bool \| None                                    | None     | quiet-Einstellung des Katalogs überschreiben |
| oracle_client_path | str \| None                                     | None     | Pfad zu den Oracle-Instant-Client-Bibliotheken |
| ssh_tunnel         | dict \| None                                    | None     | SSH-Tunnel-Konfiguration (host, user, port usw.) |


## `Catalog.add_geodatabase()`

```python
catalog.add_geodatabase(
    path,
    metadata=None,
    *,
    depth=None,
    include=None,
    exclude=None,
    auto_enumerations=None,
    preview_rows=None,
    quiet=None,
    refresh=None,
    storage_options=None,
)
```

| Parameter         | Typ                                             | Standard | Beschreibung                               |
| ----------------- | ----------------------------------------------- | -------- | ------------------------------------------ |
| path              | str \| Path                                      | erforderlich | Pfad oder Remote-URL des `.gdb`-Verzeichnisses |
| metadata          | EntityMetadata \| None                          | None     | Identität, Elternverknüpfung und Metadaten für den Container-Ordner |
| depth             | "dataset" \| "variable" \| "stat" \| "value" \| None | None     | Scan-Tiefe (verwendet catalog.depth bei None) |
| include           | list[str] \| None                               | None     | Gegen Layer-Namen abgeglichene Glob-Muster zum Einschließen |
| exclude           | list[str] \| None                               | None     | Gegen Layer-Namen abgeglichene Glob-Muster zum Ausschließen |
| auto_enumerations | bool \| None                                    | None     | Automatische Enumerationen aus Wert-Häufigkeiten für gescannte Layer erzeugen (überschreibt Katalog) |
| preview_rows      | int \| Literal[False] \| None                    | None     | Maximale Vorschauzeilen für gescannte Layer-Datensätze (überschreibt Katalog; 0 oder false deaktiviert) |
| quiet             | bool \| None                                    | None     | quiet-Einstellung des Katalogs überschreiben |
| refresh           | bool \| None                                    | None     | Rescan erzwingen (überschreibt Katalog-Einstellung) |
| storage_options   | dict \| None                                    | None     | Optionen für Remote-Speicher (an fsspec weitergereicht) |

## `Catalog.export_db()`

```python
catalog.export_db(
    output_dir=None,
    *,
    track_evolution=True,
    copy_assets=None,
    base_dir=None,
    export_size_report=False,
    quiet=None,
)
```

| Parameter       | Typ              | Standard | Beschreibung                               |
| --------------- | ---------------- | ------- | ------------------------------------------ |
| output_dir      | str \| Path \| None | None    | Ausgabeverzeichnis (verwendet app_path bei None) |
| track_evolution | bool             | True    | Änderungen zwischen Exporten verfolgen     |
| copy_assets     | dict \| list[dict] \| None | None    | Zusätzliche lokale Dateien/Verzeichnisse mit denselben Regeln wie `copy_assets()` in den Export kopieren |
| base_dir        | str \| Path \| None | None    | Basisverzeichnis für relative `copy_assets.from`-Pfade (Standard: aktuelles Arbeitsverzeichnis) |
| export_size_report | bool         | False   | Export-Größenbericht pro Tabelle ausgeben, einschließlich geschätzter gzip-komprimierter `.json`-Größen |
| quiet           | bool \| None     | None    | quiet-Einstellung des Katalogs überschreiben |

Exportiert JSON-Metadatendateien. Ruft `finalize()` automatisch auf, wenn Daten gescannt wurden.

## `Catalog.export_app()`

```python
catalog.export_app(
    output_dir=None,
    *,
    open_browser=False,
    track_evolution=True,
    update_app=False,
    copy_assets=None,
    base_dir=None,
    export_size_report=False,
    quiet=None,
)
```

| Parameter       | Typ                 | Standard | Beschreibung                               |
| --------------- | ------------------- | ------- | ------------------------------------------ |
| output_dir      | str \| Path \| None | None    | Ausgabeverzeichnis (verwendet app_path bei None) |
| open_browser    | bool                | False   | App nach dem Export im Browser öffnen      |
| track_evolution | bool                | True    | Änderungen zwischen Exporten verfolgen     |
| update_app      | bool                | False   | Gebündelte Frontend-App-Dateien aktualisieren, wenn die App bereits existiert |
| copy_assets     | dict \| list[dict] \| None | None    | Zusätzliche lokale Dateien/Verzeichnisse mit denselben Regeln wie `copy_assets()` in die exportierte App kopieren |
| base_dir        | str \| Path \| None | None    | Basisverzeichnis für relative `copy_assets.from`-Pfade (Standard: aktuelles Arbeitsverzeichnis) |
| export_size_report | bool            | False   | Export-Größenbericht pro Tabelle für `data/db` ausgeben, einschließlich geschätzter gzip-komprimierter `.json`-Größen |
| quiet           | bool \| None        | None    | quiet-Einstellung des Katalogs überschreiben |

Exportiert eine vollständige eigenständige datannur-App mit Daten. Verwendet standardmäßig `app_path`, sofern bei der Initialisierung gesetzt. Bestehende Apps aktualisieren standardmäßig `data/db`; mit `update_app=True` werden die gebündelten Frontend-Dateien aktualisiert.

## `Catalog.finalize()`

```python
catalog.finalize()
```

Fortgeschrittene Lifecycle-Methode. Entfernt Entitäten, die beim Scan nicht mehr gesehen wurden.

Im Normalfall ist ein direkter Aufruf nicht nötig: `export_db()` und `export_app()` rufen sie nach dem Scannen automatisch auf.


## `run_config()`

```python
from datannurpy import run_config

catalog = run_config(path)
```

| Parameter | Typ        | Standard | Beschreibung |
| --------- | ---------- | -------- | ----------- |
| path      | str \| Path | erforderlich | Zu ladende und auszuführende YAML-Konfigurationsdatei |

Führt einen `catalog.yml`-Workflow aus und gibt den resultierenden `Catalog` zurück.

Von `run_config()` erkannte YAML-Schlüssel auf oberster Ebene:

| Schlüssel | Typ | Beschreibung |
| --- | --- | --- |
| `add` | list[dict] | Auszuführende Scan-Schritte mit Kurzform-Einträgen wie `folder`, `dataset` oder `database` |
| `env` | dict[str, str] | Umgebungsvariablen, die vor der Expansion der YAML-Werte injiziert werden |
| `env_file` | str \| Path \| list[str \| Path] | Eine oder mehrere dotenv-Dateien, die vor der Expansion geladen werden |
| `output_dir` | str \| Path \| None | Nur JSON-Metadaten in dieses Verzeichnis exportieren, statt eine vollständige App zu exportieren |
| `open_browser` | bool | Die erzeugte App nach dem App-Export im Browser öffnen |
| `copy_assets` | dict \| list[dict] \| None | Zusätzliche lokale Dateien oder Verzeichnisse in den Export kopieren |
| `track_evolution` | bool | Erzeugung von `evolution.json` beim Export aktivieren oder deaktivieren |
| `export_size_report` | bool | Export-Größenbericht pro Tabelle nach dem Schreiben der Datenbank aktivieren |
| `update_app` | bool | Gebündelte Frontend-App-Dateien beim Export in einen bestehenden `app_path` aktualisieren |
| `post_export` | str \| list[str] \| None | Python-Skripte nach Abschluss des Exports ausführen |

Alle anderen nicht reservierten Schlüssel auf oberster Ebene werden mit denselben Parameternamen an `Catalog(...)` durchgereicht, etwa `app_path`, `metadata_path`, `depth`, `refresh`, `sample_size`, `auto_enumerations`, `preview_rows`, `app_config`, `quiet`, `verbose` und `log_file`.

Um diese API-Seite kompakt zu halten, ist das detaillierte YAML-Verhalten in den thematischen Anleitungen dokumentiert:

- Export-Optionen wie `output_dir`, `open_browser`, `track_evolution`, `update_app`, `copy_assets` und `post_export`: siehe [Ausgabe & Exporte](/de/output)
- Metadaten- und Konfigurationsoptionen wie `metadata_path`, `env`, `env_file` und `app_config`: siehe [Metadaten & Konfiguration](/de/metadata)

Pfadbasierte Werte werden relativ zum Verzeichnis der YAML-Datei aufgelöst, sofern in diesen Anleitungen nichts anderes dokumentiert ist.

## `copy_assets()`

```python
from datannurpy import copy_assets

copy_assets(output_dir, rules, *, base_dir=None, quiet=False)
```

| Parameter  | Typ                           | Standard | Beschreibung |
| ---------- | ----------------------------- | ------- | ----------- |
| output_dir | str \| Path                    | erforderlich | Zu befüllendes Exportverzeichnis |
| rules      | dict \| list[dict]             | erforderlich | Kopierregeln in derselben Form wie das YAML-`copy_assets` |
| base_dir   | str \| Path \| None           | None    | Basisverzeichnis für relative `from`-Pfade (Standard: aktuelles Arbeitsverzeichnis) |
| quiet      | bool                           | False   | Kopier-Fortschritts-Logging unterdrücken |

Jede Regel akzeptiert `from`, `to`, optional `include` und optional `clean`.

`Catalog.export_db()` und `Catalog.export_app()` akzeptieren `copy_assets=` und `base_dir=` ebenfalls als komfortable Wrapper um diesen Helfer.

## `EntityMetadata`

```python
EntityMetadata(
    id=None,
    parent_id=None,
    manager_organization_id=None,
    owner_organization_id=None,
    tag_ids=None,
    doc_ids=None,
    name=None,
    description=None,
    license=None,
    type=None,
    link=None,
    localisation=None,
    start_date=None,
    end_date=None,
    updating_each=None,
    no_more_update=None,
)
```

| Parameter      | Typ               | Beschreibung |
| -------------- | ----------------- | ----------- |
| id             | str \| None       | Explizite Entitäts-ID. Falls nicht angegeben, werden aus dem Scan abgeleitete Standardwerte verwendet. |
| parent_id      | str \| None       | ID des übergeordneten Ordners (`Folder.parent_id` für Ordner, `Dataset.folder_id` für Datensätze). |
| manager_organization_id | str \| None       | ID der verwaltenden Organisation. |
| owner_organization_id   | str \| None       | ID der besitzenden Organisation. |
| tag_ids        | list[str] \| None | IDs zugehöriger Tags. |
| doc_ids        | list[str] \| None | IDs zugehöriger Dokumente. |
| name           | str \| None       | Anzeigename. |
| description    | str \| None       | Beschreibungstext. |
| license        | str \| None       | Lizenz-String. |
| type           | str \| None       | Entitätstyp/-kategorie. |
| link           | str \| None       | Externe Referenz-URL. |
| localisation   | str \| None       | Geografische Abdeckung. |
| start_date     | str \| None       | Beginn des abgedeckten Zeitraums. |
| end_date       | str \| None       | Ende des abgedeckten Zeitraums. |
| updating_each  | str \| None       | Aktualisierungsfrequenz. |
| no_more_update | str \| None       | Kennzeichen, dass keine weiteren Aktualisierungen erwartet werden. |

In YAML-Konfigurationen werden dieselben Metadaten üblicherweise als Schlüssel auf oberster Ebene eines `add`-Eintrags geschrieben:

```yaml
add:
  - folder: ./data
    id: source
    name: Source data
    description: Curated files used by the analytics team.

  - dataset: ./data/sales.csv
    id: source---sales
    folder_id: source
    name: Sales
    description: Monthly sales by product and region.
```

`EntityMetadata` ist das Python-API-Äquivalent dieser YAML-Metadaten-Schlüssel.

## ID-Helfer

```python
from datannurpy import sanitize_id, build_dataset_id, build_variable_id
```

| Funktion                                        | Beschreibung               | Beispiel                                                    |
| ----------------------------------------------- | -------------------------- | ----------------------------------------------------------- |
| sanitize_id(s)                                  | String für die Verwendung als ID bereinigen | "My File (v2)" → "My File _v2_"                          |
| build_dataset_id(folder_id, dataset_name)       | Datensatz-ID erzeugen      | (\"src\", \"sales\") → \"src---sales\"                      |
| build_variable_id(folder_id, dataset_name, var) | Variablen-ID erzeugen      | (\"src\", \"sales\", \"amount\") → \"src---sales---amount\" |
