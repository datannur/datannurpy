# Scan-Tiefe

Der Parameter `depth` steuert, wie viele Metadaten aus Dateien, Ordnern, Datensätzen und Datenbanken extrahiert werden. Er lässt sich einmal als globaler Standard setzen oder für jeden `add`-Eintrag überschreiben.

```yaml
depth: variable                   # globaler Standard

add:
  - folder: ./data                # erbt "variable"

  - folder: ./big
    depth: stat                   # Überschreibung für diesen Eintrag

  - database: sqlite:///db.sqlite
    depth: dataset
```

| Funktion                             | `dataset` | `variable` | `stat`         | `value` (Standard) |
| ------------------------------------ | :-------: | :--------: | :------------: | :----------------: |
| Ordner                               | ✓         | ✓          | ✓              | ✓                  |
| Datensätze (Format, Pfad, mtime)     | ✓         | ✓          | ✓              | ✓                  |
| Variablen (Namen, Typen)             |           | ✓          | ✓              | ✓                  |
| DB-Introspektion (PK, FK, Kommentare)|           | ✓          | ✓              | ✓                  |
| Zeilenzahl, Statistiken              |           |            | ✓              | ✓                  |
| Enumerationen, Häufigkeiten, Muster  |           |            |                | ✓                  |
| Auto-Tagging (Format, Sicherheit, Text)|         |            |                | ✓                  |

> **Hinweis:** Bei `depth="variable"` extrahieren CSV- und Excel-Dateien nur die Spalten-**Namen** (Typen erfordern das Lesen von Daten und sind ab `depth="stat"` verfügbar). Alle anderen Formate liefern Typen bereits auf dieser Stufe.

**Typische Anwendungsfälle:**

- **`dataset`** — schnelles Inventar der verfügbaren Dateien/Tabellen ohne Datenlesen
- **`variable`** — leichtgewichtige Schema-Erkennung (Spaltennamen und -typen)
- **`stat`** — Daten-Profiling ohne Enumerationserkennung (schneller als `value`)
- **`value`** — vollständiger Katalog mit Häufigkeitstabellen und Enumerationszuordnung (Standard)

Wer Wert-Häufigkeiten benötigt, aber Enumerationen manuell über `metadata_path` verwaltet, verwendet `auto_enumerations: false` zusammen mit `depth: value`. Häufigkeiten bleiben verfügbar, automatische Enumerationsentitäten und generierte Variablenverknüpfungen werden jedoch übersprungen.

## Auto-Tagging

Bei `depth="value"` (Standard) werden String-Spalten automatisch nach Inhaltstyp getaggt. Tags verwenden eine zweistufige Hierarchie unter dem Eltern-Tag `auto`:

| Kategorie  | Tags                                                       |
| ---------- | ---------------------------------------------------------- |
| **Format** | `auto---email`, `auto---phone`, `auto---uuid`, `auto---iban` |
| **Sicherheit** | `auto---bcrypt`, `auto---argon2`, `auto---jwt`, `auto---secret` |
| **Text**   | `auto---structured`, `auto---semi-structured`, `auto---free-text` → `auto---natural-text` |

Jede Variable erhält höchstens **ein Blatt-Tag**. Das Frontend kann über `parent_id` nach Kategorie filtern (z. B. zeigt die Auswahl von `auto---security` alle bcrypt-/argon2-/jwt-/secret-Variablen).

**Sicherheitsgetaggte Spalten** (bcrypt, argon2, jwt, secret) unterdrücken ihre rohen Häufigkeitswerte — es werden nur Muster-Häufigkeiten ausgegeben, sodass keine echten Geheimnisse im exportierten Katalog erscheinen.

## Policy-Tags

Policy-Tags erlauben die manuelle Steuerung des Scan-Verhaltens für einzelne Variablen. Wie Auto-Tags liegen sie in der `scan`-Hierarchie und werden automatisch angelegt — kein `tag.csv`-Eintrag nötig.

| Tag                      | Wirkung                                                        |
| ------------------------ | -------------------------------------------------------------- |
| `policy---frequency-hidden`   | Unterdrückt alle Häufigkeits- und Enumerationsdaten (Statistiken bleiben sichtbar) |

Das Tag wird in den `variable.csv`-Metadaten zugewiesen:

```csv
id,tag_ids
source---employees_csv---first_name,"policy---frequency-hidden"
```

Das ist nützlich für sensible Spalten, die das Auto-Tagging nicht erkennt (z. B. Vornamen, interne IDs, Geschäftscodes).
