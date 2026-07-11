# Zeitreihen-Gruppierung

datannurpy kann wiederholte Dateien oder Datenbanktabellen zu einem Datensatz gruppieren, wenn sich ihre Namen nur durch eine Zeitperiode unterscheiden. Dies ist standardmäßig aktiviert, sowohl für `add_folder()` als auch für `add_database()` mit `time_series: true`.

## Unterstützte Muster {#supported-patterns}

Der Detektor erkennt Jahre, Quartale, Monate und vollständige Daten in Namen oder Pfaden:

| Periodentyp | Beispiele | Gespeichert als | Metadata-first-Muster |
| ----------- | -------- | --------- | ---------------------- |
| Jahr | `sales_2024.csv`, `stats_2024` | `2024` | `[YYYY]` |
| Quartal | `sales_2024Q1.csv`, `stats_2024T2` | `2024Q1`, `2024Q2` | `[YYYY]Q[N]` |
| Monat | `sales_2024-03.csv`, `sales_202403` | `2024/03` | `[YYYY/MM]` |
| Datum | `sales_2024-03-15.csv`, `sales_20240315` | `2024/03/15` | `[YYYY/MM/DD]` |

Teilperioden wie `01`, `02`, `Q1` oder `day15` können nur verwendet werden, wenn an anderer Stelle im selben Pfad oder Tabellennamen ein vollständiges vierstelliges Jahr vorhanden ist. Eine Sequenz wie `report_01.csv`, `report_02.csv` wird als separate Datensätze belassen, weil ihr der Jahreskontext fehlt.

## Dateien

Beim Scannen von Ordnern können einfache Datei-Datensätze mit demselben Format gruppiert werden:

```text
data/
+-- enquete_2020.csv
+-- enquete_2021.csv
+-- enquete_2022.csv
`-- reference.csv
```

Dies erzeugt einen Datensatz namens `enquete_[YYYY]` mit `nb_resources=3`; `reference.csv` bleibt ein eigener Datensatz.

Perioden können auch in Ordnern vorkommen:

```text
data/
+-- 2024/01/sales.csv
+-- 2024/02/sales.csv
`-- 2024/03/sales.csv
```

Dies erzeugt einen `sales`-Datensatz mit den Perioden `2024/01`, `2024/02` und `2024/03`. Temporale Ordner werden für die Serie nicht als Katalog-Ordner angelegt; nur nicht-temporale übergeordnete Ordner bleiben erhalten.

Partitionierte Datensätze wie Delta, Hive und Iceberg werden von ihren eigenen Datensatz-Scannern verarbeitet und von diesem Zeitreihen-Detektor nicht gruppiert.

## Datenbanktabellen

Beim Scannen von Datenbanken werden Tabellen pro Schema gruppiert, nachdem die `include`- und `exclude`-Filter angewendet wurden:

```text
sales_fact_202401
sales_fact_202402
sales_fact_202403
dim_customer
```

Dies erzeugt einen Datensatz namens `sales_fact_[YYYY/MM]` mit `nb_resources=3`; `dim_customer` bleibt ein eigener Datensatz.

Für Tabellen und Dateien gelten dieselben Validierungsregeln. Beispielsweise werden `dim_age_01`, `dim_age_02` und `dim_age_03` nicht gruppiert, weil kein vierstelliges Jahr vorhanden ist. Gemischte Granularitäten werden in getrennte Datensätze aufgeteilt: `data_2021`, `data_2022`, `data_2021Q1` und `data_2021Q2` werden zu einem jährlichen und einem quartalsweisen Datensatz.

## Resultierende Metadaten

Für eine gruppierte Serie erhält der Datensatz:

- `nb_resources`: Anzahl der Dateien oder Tabellen in der Serie
- `start_date`: erste erkannte Periode
- `end_date`: letzte erkannte Periode
- `data_path`: die neueste Datei oder Tabelle, verwendet als kanonische Ressource

In Metadata-first-Scans (`create_folders=False`) kann der technische `_match_path` in `metadata/dataset.csv` die normalisierte Serien-Syntax aus der Spalte [Metadata-first-Muster](#supported-patterns) oben verwenden — zum Beispiel `sales_[YYYY].csv` oder `sales_[YYYY/MM].csv` —, um die logische Serie statt einer konkreten neuesten Datei zu treffen.

Bei `depth="stat"` oder `depth="value"` werden Statistiken und Häufigkeitstabellen nur aus der neuesten Periode berechnet. Ältere Perioden werden im Nur-Schema-Modus gescannt, damit datannurpy die Verfügbarkeit von Variablen über die Zeit erkennen kann.

Variablen erhalten ebenfalls `start_date` und `end_date`, wenn sich ihr Vorkommen über die Perioden hinweg ändert. Eine Variable, die in jeder Periode vorhanden ist, hat beide Felder leer. Eine nach der ersten Periode hinzugekommene Variable erhält `start_date`; eine vor der letzten Periode entfernte Variable erhält `end_date`.

## Vermeidung von Fehlerkennungen

datannurpy belässt Kandidaten als separate Datensätze, wenn eine Gruppierung mehrdeutig oder unvollständig wäre:

- weniger als zwei passende Dateien oder Tabellen
- kein vierstelliges Jahr irgendwo in der Kandidatengruppe
- Monats- oder Tagesfragmente ohne ausreichenden Jahres-/Monatskontext
- Tabellen oder Dateien, deren gesamter Name nur ein temporales Token ist
- gemischte Perioden-Granularitäten, die als getrennte Serien dargestellt werden sollten

Konstante, temporal wirkende Token, etwa ein Lieferdatums-Ordner oder ein festes numerisches Präfix, bleiben als Literaltext erhalten, wenn eine andere, variierende Periode die Serie identifiziert.

## Erkennung deaktivieren

Mit `time_series: false` wird jede Datei oder Tabelle unabhängig behandelt:

```yaml
add:
  - folder: ./data
    time_series: false

  - database: postgresql://localhost/mydb
    time_series: false
```
