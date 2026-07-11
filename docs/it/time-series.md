# Raggruppamento di serie temporali

datannurpy può raggruppare file ripetuti o tabelle di database in un unico dataset quando i loro nomi differiscono soltanto per un periodo temporale. Questa funzionalità è abilitata per impostazione predefinita sia per `add_folder()` sia per `add_database()` con `time_series: true`.

## Pattern supportati {#supported-patterns}

Il rilevatore riconosce anni, trimestri, mesi e date complete nei nomi o nei percorsi:

| Tipo di periodo | Esempi | Memorizzato come | Pattern metadata-first |
| --------------- | ------ | ---------------- | ---------------------- |
| Anno | `sales_2024.csv`, `stats_2024` | `2024` | `[YYYY]` |
| Trimestre | `sales_2024Q1.csv`, `stats_2024T2` | `2024Q1`, `2024Q2` | `[YYYY]Q[N]` |
| Mese | `sales_2024-03.csv`, `sales_202403` | `2024/03` | `[YYYY/MM]` |
| Data | `sales_2024-03-15.csv`, `sales_20240315` | `2024/03/15` | `[YYYY/MM/DD]` |

I periodi parziali come `01`, `02`, `Q1` o `day15` possono essere usati soltanto quando un anno completo a 4 cifre è disponibile altrove nello stesso percorso o nome di tabella. Una sequenza come `report_01.csv`, `report_02.csv` viene mantenuta come dataset separati perché priva di contesto sull'anno.

## File

Durante la scansione delle cartelle, i dataset di file semplici con lo stesso formato possono essere raggruppati:

```text
data/
+-- enquete_2020.csv
+-- enquete_2021.csv
+-- enquete_2022.csv
`-- reference.csv
```

Questo crea un dataset chiamato `enquete_[YYYY]` con `nb_resources=3`; `reference.csv` resta un dataset separato.

I periodi possono comparire anche nelle cartelle:

```text
data/
+-- 2024/01/sales.csv
+-- 2024/02/sales.csv
`-- 2024/03/sales.csv
```

Questo crea un unico dataset `sales` con i periodi `2024/01`, `2024/02` e `2024/03`. Le cartelle temporali non vengono create come cartelle del catalogo per la serie; vengono mantenute soltanto le cartelle padre non temporali.

I dataset partizionati come Delta, Hive e Iceberg sono gestiti dai rispettivi scanner di dataset e non vengono raggruppati da questo rilevatore di serie temporali.

## Tabelle di database

Durante la scansione dei database, le tabelle vengono raggruppate per schema, dopo l'applicazione dei filtri `include` ed `exclude`:

```text
sales_fact_202401
sales_fact_202402
sales_fact_202403
dim_customer
```

Questo crea un dataset chiamato `sales_fact_[YYYY/MM]` con `nb_resources=3`; `dim_customer` resta un dataset separato.

Le stesse regole di validazione si applicano a tabelle e file. Ad esempio, `dim_age_01`, `dim_age_02` e `dim_age_03` non vengono raggruppate perché manca un anno a 4 cifre. Le granularità miste vengono suddivise in dataset distinti, quindi `data_2021`, `data_2022`, `data_2021Q1` e `data_2021Q2` diventano un dataset annuale e uno trimestrale.

## Metadati risultanti

Per una serie raggruppata, il dataset riceve:

- `nb_resources`: numero di file o tabelle nella serie
- `start_date`: primo periodo rilevato
- `end_date`: ultimo periodo rilevato
- `data_path`: il file o la tabella più recente, usato come risorsa canonica

Nelle scansioni metadata-first (`create_folders=False`), il campo tecnico `_match_path` in `metadata/dataset.csv` può usare la sintassi normalizzata delle serie della colonna [Pattern metadata-first](#supported-patterns) qui sopra — ad esempio `sales_[YYYY].csv` o `sales_[YYYY/MM].csv` — per corrispondere alla serie logica invece che a uno specifico file più recente.

A `depth="stat"` o `depth="value"`, le statistiche e le tabelle di frequenza vengono calcolate soltanto sull'ultimo periodo. I periodi più vecchi vengono scansionati in modalità solo-schema, così datannurpy può rilevare la disponibilità delle variabili nel tempo.

Anche le variabili ricevono `start_date` ed `end_date` quando la loro presenza cambia tra i periodi. Una variabile presente in tutti i periodi ha entrambi i campi vuoti. Una variabile aggiunta dopo il primo periodo riceve `start_date`; una variabile rimossa prima dell'ultimo periodo riceve `end_date`.

## Evitare i falsi positivi

datannurpy mantiene i candidati come dataset separati quando il raggruppamento sarebbe ambiguo o incompleto:

- meno di due file o tabelle corrispondenti
- nessun anno a 4 cifre in alcun punto del gruppo candidato
- frammenti di mese o giorno senza sufficiente contesto anno/mese
- tabelle o file il cui intero nome è soltanto un token temporale
- granularità di periodo miste che dovrebbero essere rappresentate come serie separate

I token dall'aspetto temporale ma costanti, come una cartella con data di consegna o un prefisso numerico fisso, vengono preservati come testo letterale quando un altro periodo variabile identifica la serie.

## Disabilitare il rilevamento

Impostare `time_series: false` per trattare ogni file o tabella in modo indipendente:

```yaml
add:
  - folder: ./data
    time_series: false

  - database: postgresql://localhost/mydb
    time_series: false
```
