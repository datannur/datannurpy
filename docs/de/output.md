# Ausgabe & Exporte

## Katalog-Lebenszyklus {#catalog-lifecycle}

Das exportierte `data/db` ist immer verzichtbar — es wird bei jedem Lauf neu geschrieben, daher niemals von Hand bearbeiten. Bearbeitungen gehören in die **Quelle der Wahrheit**, und wo diese liegt, definiert den Workflow:

- **Artefakt** — die Quelle der Wahrheit sind externe Dateien (`metadata_path`, gescannte Ordner); die Ausgabe ist gitignored und wird von CI neu erzeugt. Kombinieren mit `refresh: true` und `on_scan_error: fail` / `on_metadata_error: fail`, damit ein fehlerhafter Lauf rot fehlschlägt, statt einen degradierten Katalog zu veröffentlichen. Das ist der GitHub-Pages-Template-Ablauf.
- **Installation** — die App lebt langfristig am selben Ort; Bearbeitungen erfolgen darin (`db-ui`), und die gebündelten Dateien werden mit `update_app: true` aktualisiert. Lokaler App-Zustand unter `data/` bleibt über Läufe hinweg erhalten (siehe [Inkrementeller Scan](#incremental-scan)).

Beides lässt sich kombinieren — eine Installation kann weiterhin externe Metadaten einbinden (`db-source`), wie es `examples/demo_editorial.yml` tut. Nur eine Regel ist fest: `data/db` ist Ausgabe, Bearbeitungen gehören in `metadata_path`-Dateien oder das In-App-`db-ui`.

## Ausgabe


```yaml
# Vollständige eigenständige App
app_path: ./my-catalog
open_browser: true

# Nur JSON-Metadaten (für bestehende datannur-Instanz)
output_dir: ./output
```

Export-Optionen auf oberster Ebene:

| Schlüssel | Typ | Standard | Gilt für | Beschreibung |
|---|---|---|---|---|
| `app_path` | Pfad | `None` | App-Export | Ausgabeverzeichnis für eine eigenständige datannur-App |
| `output_dir` | Pfad | `None` | DB-Export | Ausgabeverzeichnis nur für JSON-Metadaten |
| `open_browser` | bool | `false` | App-Export | Die erzeugte App nach dem Export im Browser öffnen |
| `refresh` | bool | `false` | Scan | Vollständigen Rescan erzwingen, statt unveränderte Dateien oder Tabellen wiederzuverwenden |
| `track_evolution` | bool | `true` | App- + DB-Export | `evolution.json` mit hinzugefügten, aktualisierten und gelöschten Entitäten schreiben |
| `update_app` | bool | `false` | App-Export | Gebündelte Frontend-App-Dateien aktualisieren, wenn `app_path` bereits existiert |
| `copy_assets` | Regel oder Liste von Regeln | `None` | App- + DB-Export | Zusätzliche lokale Dateien oder Verzeichnisse in den Export kopieren |
| `export_size_report` | bool | `false` | App- + DB-Export | Nach dem Schreiben der Datenbank einen Größenbericht pro Tabelle ausgeben |
| `post_export` | Skriptname, Pfad oder Liste | `None` | App- + DB-Export | Python-Skripte nach Abschluss des Exports ausführen |

Mit `export_size_report: true` wird nach dem Schreiben der Datenbank ein Größenbericht pro Tabelle ausgegeben. Der Bericht enthält rohe `.json`-, rohe `.json.js`- und geschätzte gzip-komprimierte `.json`-Größen mit Prozentangaben, was hilft, die Tabellen zu identifizieren, die das Katalog-Gewicht dominieren.

### Große Exporte

Große Kataloge werden meist von `frequency` und `value` dominiert, weil diese Tabellen wiederholte Werte für viele Variablen speichern. `export_size_report` aktivieren, um vor Änderungen an den Scan-Einstellungen zu prüfen, welche Tabellen relevant sind.

Fällt der Export größer aus als erwartet, sind die Haupthebel Scan-Tiefe, Häufigkeitserzeugung, automatische Enumerationserzeugung und Sampling. `depth: stat` behält Variablenstatistiken, ohne Häufigkeitstabellen oder Enumerationen zu schreiben; `depth: variable` behält nur Metadaten auf Schema-Ebene; `auto_enumerations: false` behält die `depth: value`-Häufigkeitstabellen, überspringt aber automatische Enumerationsentitäten und generierte Variablenverknüpfungen; `freq_threshold` steuert, wann String-Spalten mit hoher Kardinalität von Wert-Häufigkeiten auf Muster-Häufigkeiten umschalten; `sample_size` begrenzt die für Häufigkeitszählungen und automatische Enumerationserkennung verwendeten Zeilen, während die Kernstatistiken auf dem vollständigen Datensatz berechnet werden.

`.json.js` entspricht lokaler oder Netzlaufwerk-Nutzung (`file://`), `.json` unkomprimiertem HTTP und `.json.gz` HTTP mit gzip-Auslieferung.

### Datensatz-Vorschauen

Bei den Tiefen `stat` und `value` exportiert datannurpy standardmäßig kleine Datensatz-Vorschauen. Reine Datenbank-Exporte schreiben sie nach `<output_dir>/preview/<dataset_id>.json` und `<output_dir>/preview/<dataset_id>.json.js`; App-Exporte legen dieselben Dateien unter `data/db/preview/` ab. Die JSON-Datei ist ein Array von Zeilenobjekten, und die JSON-JS-Datei verwendet `jsonjs.data['<dataset_id>']`, entsprechend der Konvention der Metadaten-Tabellen.

Mit `preview_rows` wird die maximale Zeilenzahl pro Datensatz gesteuert. Der Standard ist `100`; `preview_rows: 0` oder `preview_rows: false` global oder auf einem einzelnen `add`-Eintrag setzen, um Vorschauen für sensible Quellen zu deaktivieren. Vorschauen werden bei den Tiefen `dataset` oder `variable` nicht erhoben, da diese Modi keine Datenzeilen lesen.

## Inkrementeller Scan {#incremental-scan}


Ein erneuter Lauf mit demselben `app_path` scannt nur geänderte Dateien (Vergleich der mtime) oder Tabellen (Vergleich von Schema + Zeilenzahl):

```yaml
app_path: ./my-catalog

add:
  - folder: ./data               # überspringt unveränderte Dateien
```

Mit `refresh: true` wird ein vollständiger Rescan erzwungen.

Datensätze mit fehlgeschlagenem Scan werden nach einem datannurpy-Upgrade einmal neu gescannt — Scanner-Fixes erreichen sie so ohne `refresh`.

**Jeder Lauf spiegelt die aktuellen Quellen.** Der Export gibt exakt wieder, was der aktuelle Scan und die Metadaten erzeugen: Eine Entität, die in einem früheren Lauf vorhanden war, aber nicht mehr gescannt wird und nicht mehr in den Metadaten steht, wird entfernt, nicht behalten. Aus dem Scan abgeleitete Zeilen werden unter `data/db/_scan/` zwischengespeichert, damit unveränderte Dateien übersprungen werden können; das exportierte `data/db` wird dann bei jeder Veröffentlichung aus dieser Scan-Basis plus den aktuellen Metadaten neu aufgebaut — es ist eine verzichtbare Materialisierung, kein Akkumulationsspeicher, sodass Metadaten-Overlays eines früheren Exports niemals in die Basis des nächsten Laufs durchsickern. `_scan` ist ein interner Cache (nie eine App-Tabelle); ihn zu löschen kostet nur einen vollständigen Rescan. Wird eine Quelle nicht mehr gescannt, verschwinden ihre Datensätze beim nächsten Lauf. Siehe [Löschsemantik](/de/metadata#deletion) für die Unterschiede zwischen Eltern-Entitäten (Datensätze, Ordner, …) und Kind-Entitäten (Variablen, Werte, Häufigkeiten).

Bestehende App-Exporte aktualisieren standardmäßig `data/db` und erhalten den lokalen App-Zustand unter `data/`. Um die gebündelten Frontend-App-Dateien nach einem Upgrade von datannurpy zu aktualisieren, `update_app: true` setzen oder `catalog.export_app(update_app=True)` aufrufen.

Existiert `app_path/data/db-ui`, wird es vor dem Export automatisch als letzte Metadatenquelle geladen. Siehe [Manuelle Metadaten](/de/metadata) für Merge-Reihenfolge und Overlay-Anweisungen.

## Änderungsverfolgung


Änderungen zwischen Exporten werden automatisch in `evolution.json` festgehalten:

- **add**: neuer Ordner, Datensatz, Variable, Enumeration usw.
- **update**: geändertes Feld (zeigt alten und neuen Wert)
- **delete**: entfernte Entität

Kaskadenfilterung: Wird eine Eltern-Entität hinzugefügt oder gelöscht, werden ihre Kinder automatisch herausgefiltert, um Rauschen zu reduzieren. Beispielsweise erzeugt das Hinzufügen eines neuen Datensatzes keine separaten Einträge für jede Variable.

Verfolgung deaktivieren:

```yaml
track_evolution: false
```

## copy_assets


Lokale Dateien oder Verzeichnisse während des Exports in den exportierten Katalog kopieren:

```yaml
copy_assets:
  - from: ./staging/docs
    to: data/doc
    include: "*.pdf"
    clean: true

  - from: ./data
    to: data/source
```

Regeln:

- `from` wird relativ zum Verzeichnis der YAML-Konfiguration aufgelöst
- `to` wird relativ zum Exportverzeichnis aufgelöst und muss innerhalb davon bleiben
- Verzeichnisse werden rekursiv kopiert; einzelne Dateien werden in das Zielverzeichnis kopiert
- `include` ist optional und akzeptiert einen Glob-String oder eine Liste von Globs
- `clean: true` entfernt Zieldateien, die in der gefilterten Quellmenge nicht vorhanden sind
- Kopien sind inkrementell: Eine Datei wird nur aktualisiert, wenn sie fehlt, ihre Größe sich geändert hat oder die `mtime` der Quelle neuer ist

Funktioniert sowohl mit `app_path`- als auch mit `output_dir`-Exporten.

## post_export


Python-Skripte automatisch nach dem Export ausführen:

```yaml
# Einzelnes Skript (bloßer Name → app/scripts/python/start_app.py, wenn gebündelt,
# mit Fallback auf python-scripts/start_app.py)
post_export: start_app

# Mehrere Skripte
post_export:
  - export_dcat
  - start_app
```

Skript-Auflösung:

| Format | Aufgelöster Pfad |
|---|---|
| `start_app` | `{output}/app/scripts/python/start_app.py`, falls vorhanden, sonst `{output}/python-scripts/start_app.py` |
| `hook.py` | `{config_dir}/hook.py` |
| `scripts/hook.py` | `{config_dir}/scripts/hook.py` |
| `/absolute/path.py` | `/absolute/path.py` |

Explizite Skriptpfade werden relativ zum Verzeichnis der YAML-Konfigurationsdatei aufgelöst, wie die anderen pfadbasierten Optionen.

Bei App-Exporten läuft `copy_assets`, nachdem die App-Hülle installiert wurde und bevor `data/db` geschrieben wird, sodass kopierte Dateien in den finalen Datenbank-Export einfließen können. Es läuft außerdem vor `post_export`, sodass eigene Skripte kopierte Dateien nutzen können.

Das gebündelte `export_dcat`-Skript schreibt semantische Export-Artefakte nach `data/db-semantic/`, darunter DCAT-RDF-Dateien, `validation.json` und `dcat-report.html`, sofern von der App-Version unterstützt.

Funktioniert sowohl mit `app_path`- als auch mit `output_dir`-Exporten.
