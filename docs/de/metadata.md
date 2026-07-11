# Metadaten & Konfiguration

## Manuelle Metadaten


```yaml
# Aus einem Ordner mit Metadaten-Dateien laden
metadata_path: ./metadata

# Oder aus einer Datenbank
metadata_path: sqlite:///metadata.db

# Oder eine Liste von Quellen, die der Reihe nach angewendet werden (spätere überschreiben/ergänzen frühere)
metadata_path:
  - ./metadata-base        # gemeinsame oder generierte Metadaten
  - ./metadata-overlay     # lokale Überschreibungen (z. B. Policy-Tags ergänzen)
```

Kann allein oder in Kombination mit automatisch gescannten Quellen (`add_folder`, `add_database`) verwendet werden. Metadaten werden vor dem Export automatisch angewendet.

**Erwartete Struktur:** Eine Datei/Tabelle pro Entität, benannt nach dem Entitätstyp:

```
metadata/
├── variable.csv      # Variablen (Beschreibungen, Tags...)
├── dataset.xlsx      # Datensätze
├── organization.json # Organisationen (Eigentümer, Verwalter)
├── tag.csv           # Tags
├── concept.csv       # Konzepte des Fachglossars
├── enumeration.csv   # Enumerationen
├── value.csv         # Enumerationswerte
├── config.csv        # Web-App-Konfiguration (siehe app_config)
└── ...
```

**Unterstützte Formate:** CSV, Excel (.xlsx), JSON, SAS (.sas7bdat) oder Datenbanktabellen.

**Dateiformat:** Standardmäßige Tabellenstruktur gemäß den [datannur-Schemas](https://github.com/datannur/datannur/tree/main/public/schemas).

```csv
# variable.csv
id,description,tag_ids
source---employees_csv---salary,"Monthly gross salary in euros","finance,hr"
source---employees_csv---department,"Department code","hr"
```

Die gebräuchlichsten Metadaten-Spalten sind dieselben, unabhängig davon, ob die Quelle CSV, Excel, JSON, SAS oder eine Datenbanktabelle ist:

| Entität/Tabelle | Nützliche Spalten |
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

Lokalisierte Textfelder können mit der `field:lang`-Konvention der App bereitgestellt werden. Beispielsweise `name` und `description` als Standard-/Fallback-Sprache belassen und dann `name:fr` oder `description:fr` für französischen Text bzw. `name:de` oder `description:de` für deutschen Text ergänzen. Datannurpy erhält lokalisierte Spalten für jedes Entitätsfeld, das bereits im Schema existiert, etwa `name:fr`, `name:de` und `description:fr` bei Datensätzen, Variablen, Tags, Organisationen, Konzepten, Enumerationen, Werten, Dokumenten und App-Filterbezeichnungen. Eine leere lokalisierte Zelle trägt aus dieser Quelle nichts bei (der Wert fällt auf eine niedrigere Quelle oder den Scan zurück); mit `!` wird ein lokalisierter Wert explizit geleert.

Bei ordnerbasierten Metadatenquellen wird jede Datei nach der Entität benannt (`folder.csv`, `folder.xlsx`, `folder.json` usw.). Listenfelder wie `tag_ids`, `doc_ids` und `enumeration_ids` können in tabellarischen Dateien als kommagetrennte Werte oder in JSON als Arrays geschrieben werden. Für vollständige Schema-Details dienen die verlinkten datannur-Schemas als Referenz.

**Merge-Verhalten:**

- Bestehende Entitäten werden aktualisiert (manuelle Werte überschreiben automatisch gescannte Werte)
- Neue Entitäten werden angelegt
- Listenfelder (`tag_ids`, `doc_ids` usw.) werden zusammengeführt
- Eine leere Zelle, ein leeres JSON-Array oder eine fehlende Spalte bedeutet „kein aktueller Wert aus dieser Metadatenquelle" — sie trägt nichts bei, sodass der Wert auf das zurückfällt, was eine niedrigere Metadatenquelle oder der Scan liefert. Der Wert des vorherigen Exports wird dadurch **nicht** eingefroren. Eine Zelle leeren, um die eigene Überschreibung zu entfernen und zum gescannten Wert zurückzukehren; mit `!` wird der finale Wert erzwungen geleert, selbst wenn der Scan einen liefert

**Reihenfolge:** Metadaten werden vor dem Export/der Finalisierung automatisch angewendet, nach allen `add_folder`-, `add_dataset`- und `add_database`-Aufrufen, sodass manuelle Werte Vorrang haben. Existiert `app_path/data/db-ui`, wird es automatisch als letzte Metadatenquelle geladen, nach den konfigurierten `metadata_path`-Quellen. So können lokale App-Bearbeitungen gescannte Metadaten überschreiben, ohne `data/db-ui` zur Konfiguration hinzuzufügen. `data/db-ui` verwendet dieselben unterstützten Metadatenformate wie jede andere Metadatenquelle; `data/db` bleibt der generierte Export.

### Overlay-Anweisungen

Metadatenquellen können kleine Anweisungen enthalten, um Felder zu leeren, einzelne Relationen zu entfernen oder Entitäten vor dem Export zu löschen. Diese Anweisungen werden vom Builder konsumiert und nicht nach `data/db` geschrieben.

`!` als exakten Wert eines Skalarfelds verwenden, um es zu leeren:

```csv
id,description
source---employees_csv,!
```

`!` als exakten Wert eines Relationsfelds verwenden, um alle akkumulierten Relationen dieses Felds zu entfernen:

```csv
id,tag_ids
source---employees_csv,!
```

`!id` innerhalb einer Relationsliste verwenden, um eine akkumulierte Relation zu entfernen und andere zu behalten oder hinzuzufügen:

```json
[
  {
    "id": "source---employees_csv---salary",
    "tag_ids": ["finance", "!auto---numeric"]
  }
]
```

Relationsentfernungen werden in der Reihenfolge der Metadatenquellen angewendet. Enthält eine Zeile sowohl `id` als auch `!id`, gewinnt für diese Relation die Entfernung. Unterstützte Relationsfelder sind `tag_ids`, `doc_ids`, `enumeration_ids` und `source_variable_ids`.

### Löschung {#deletion}

Der Export [spiegelt die aktuellen Quellen](/de/output#incremental-scan), aber wie eine Entität verschwindet, hängt von ihrer Art ab:

- **Eltern-Entitäten** (`folder`, `dataset`, `enumeration`, `organization`, `tag`, `doc`, `concept`) werden automatisch abgeglichen: Eine, die vom Scan nicht mehr erzeugt wird und in den Metadaten fehlt, wird beim nächsten Lauf entfernt.
- **Kind-Entitäten** (`variable`, `value`, `frequency`) werden nicht einzeln verfolgt. Das Entfernen ihrer Zeile aus einer Metadatendatei löscht sie **nicht** — sie verschwinden nur, wenn ihr Elternteil neu gescannt oder entfernt wird (Kaskade), oder, bei Variablen, über ein explizites `_delete`. Als Sicherheitsnetz wird eine Variable, deren Datensatz nicht mehr existiert, beim Export entfernt, damit der Katalog referenziell konsistent bleibt.

Mit `_delete: true` wird eine Entität sofort aus dem finalen Katalog entfernt:

```json
[
  {
    "id": "source---old_dataset",
    "_delete": true
  }
]
```

`_delete` wird für alle ID-basierten Katalog-Entitäten unterstützt (einschließlich `variable`). Zusammengesetzte Tabellen (`value` und `frequency`) können nicht direkt gelöscht werden; sie werden über Kaskaden von ihrer übergeordneten Enumeration, Variablen, ihrem Datensatz oder Ordner entfernt.

Kaskaden werden vor dem Export angewendet:

- das Löschen eines Ordners entfernt seine untergeordneten Ordner, enthaltene Datensätze, Datensatz-Variablen, Häufigkeiten, Vorschauen und enthaltene Enumerationen;
- das Löschen eines Datensatzes entfernt seine Variablen, Häufigkeiten und Vorschauen;
- das Löschen einer Variablen entfernt ihre Häufigkeiten;
- das Löschen von Enumerationen, Tags, Dokumenten, Konzepten oder Organisationen bereinigt bei Bedarf auch zugehörige Referenzen.

## Metadata-first-Muster

Wenn die Struktur des Katalogs (Ordner, Datensatz-IDs, Hierarchie) vollständig durch `metadata_path` definiert ist und das Dateisystem nur Dateien für technische Informationen liefert (Variablen, Statistiken, Größen, Formate), wird `create_folders=False` verwendet:

```yaml
metadata_path: ./metadata
add:
  - folder: ./data/parquet
    create_folders: false
```

In diesem Modus:

- Aus dem gescannten Verzeichnis wird kein Ordner erzeugt; die Hierarchie stammt aus `metadata/folder.csv`.
- Jede gescannte Datei wird ihrem Metadateneintrag über `_match_path` zugeordnet, sofern vorhanden, sonst über `data_path` als Fallback. Der Scan übernimmt die in den Metadaten definierten `id` und `folder_id`.
- Dateien ohne Metadaten-Treffer werden gemäß `on_unmatched` gemeldet: `"warn"` (Standard), `"skip"` oder `"error"`.

**Zuordnung:** `_match_path` ist ein optionaler technischer Schlüssel, der nur dazu dient, gescannte Dateien an Metadatenzeilen zu binden. Er wird verwendet, wenn der Scan-Schlüssel vom öffentlichen `data_path` abweicht, etwa bei entfernten Quellen oder logischen Zeitreihen-Datensätzen. Fehlt `_match_path`, wird `data_path` als Zuordnungsschlüssel verwendet.

Bei entfernten Pfaden gehören Zugangsdaten nicht zur Zuordnungsidentität: `sftp://user@example.org/path/file.csv` und `sftp://example.org/path/file.csv` treffen denselben Scan-Schlüssel. Die kanonische Remote-Identität behält Protokoll, Host, optionalen Port und Pfad.

Bei Zeitreihen-Datensätzen kann `_match_path` dieselbe normalisierte Perioden-Syntax verwenden, die datannurpy erzeugt: `[YYYY]`, `[YYYY/MM]`, `[YYYY]Q[N]` oder `[YYYY/MM/DD]`. Damit wird die logische Serie getroffen, nicht nur die letzte physische Datei.

**Beispiel** — `./metadata/dataset.csv`:

```csv
id,name,folder_id,data_path
sales-2024,Sales 2024,finance,../data/parquet/sales-2024.parquet
hr-headcount,HR headcount,hr,../data/parquet/hr-headcount.parquet
```

In Kombination mit `add_folder('./data/parquet', create_folders=False)` hängt der Scan Variablen und Statistiken an `sales-2024` und `hr-headcount` (die in den Metadaten deklarierten IDs), ohne einen Ordner aus dem Datenträger-Layout zu erzeugen.

**Beispiel mit Remote und Zeitreihen** — `./metadata/dataset.csv`:

```csv
id,name,folder_id,data_path,_match_path
sales,Sales,finance,sftp://example.org/shared/data/sales_2024.csv,sftp://example.org/shared/data/sales_[YYYY].csv
traffic,Traffic,transport,sftp://example.org/shared/data/traffic_2024-03.csv,sftp://example.org/shared/data/traffic_[YYYY/MM].csv
```

Hier bleibt `data_path` der öffentliche Pfad, der im Katalog angezeigt wird, während `_match_path` nur dazu dient, das Scan-Ergebnis der Metadatenzeile zuzuordnen. Ist der SFTP-Scan als `sftp://user@example.org/shared/data` konfiguriert, wird der Benutzeranteil für die Zuordnung ignoriert.

**Einschränkungen:**

- `create_folders=False` ist inkompatibel mit `metadata=` in `add_folder()` (es wird kein Ordner erzeugt).
- Der in `metadata/dataset.csv` eingetragene `data_path` wird unverändert in die Ausgabe exportiert. `_match_path` verwenden, wenn der technische Scan-Schlüssel vom im Frontend angezeigten öffentlichen Pfad abweichen soll.

## Umgebungsvariablen


Umgebungsvariablen (`$VAR` oder `${VAR}`) werden in allen YAML-Werten expandiert. Alle Quellen werden geladen — `env:`, `env_file` und `.env` neben der YAML-Datei:

```yaml
env:
  data_dir: /shared/data
  db_host: db.example.com
env_file: /secure/path/.env    # Geheimnisse: DB_USER, DB_PASSWORD

add:
  - folder: ${data_dir}/sales
  - folder: ${data_dir}/hr
  - database: oracle://${DB_USER}:${DB_PASSWORD}@${db_host}:1521/ORCL
```

`env_file` unterstützt eine Liste von Pfaden (spätere überschreiben frühere):

```yaml
env_file:
  - /shared/common.env         # Standardwerte
  - /secure/credentials.env    # überschreibt common.env
```

Priorität (der erste gesetzte Wert gewinnt): System-Umgebungsvariablen > `env:` in YAML > `env_file` > lokale `.env`.

## app_config


Die Web-App mit Schlüssel-Wert-Einträgen konfigurieren (geschrieben als `config.json`):

```yaml
app_path: ./my-catalog
app_config:
  contact_email: contact@example.com
  more_info: "Data from [open data portal](https://example.com)."
```

Wenn `app_config` nicht angegeben ist, wird stattdessen `config.csv`/`config.xlsx`/`config.json` (Spalten `id`, `value`) aus `metadata_path` verwendet. Ist keines von beiden vorhanden, wird kein `config.json` erzeugt.
