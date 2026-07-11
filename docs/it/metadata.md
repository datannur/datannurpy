# Metadati & configurazione

## Metadati manuali


```yaml
# Caricamento da una cartella contenente file di metadati
metadata_path: ./metadata

# Oppure da un database
metadata_path: sqlite:///metadata.db

# Oppure una lista di sorgenti applicate in ordine (le successive sovrascrivono/estendono le precedenti)
metadata_path:
  - ./metadata-base        # metadati condivisi o generati
  - ./metadata-overlay     # override locali (ad es. aggiunta di tag di policy)
```

Può essere usato da solo o combinato con sorgenti scansionate automaticamente (`add_folder`, `add_database`). I metadati vengono applicati automaticamente prima dell'esportazione.

**Struttura attesa:** un file/tabella per entità, con il nome del tipo di entità:

```
metadata/
├── variable.csv      # Variabili (descrizioni, tag...)
├── dataset.xlsx      # Dataset
├── organization.json # Organizzazioni (proprietari, gestori)
├── tag.csv           # Tag
├── concept.csv       # Concetti del glossario di business
├── enumeration.csv   # Enumerazioni
├── value.csv         # Valori di enumerazione
├── config.csv        # Configurazione della web app (vedere app_config)
└── ...
```

**Formati supportati:** CSV, Excel (.xlsx), JSON, SAS (.sas7bdat) oppure tabelle di database.

**Formato dei file:** struttura tabellare standard secondo gli [schemi datannur](https://github.com/datannur/datannur/tree/main/public/schemas).

```csv
# variable.csv
id,description,tag_ids
source---employees_csv---salary,"Monthly gross salary in euros","finance,hr"
source---employees_csv---department,"Department code","hr"
```

Le colonne di metadati più comuni sono le stesse indipendentemente dalla sorgente — CSV, Excel, JSON, SAS o tabella di database:

| Entità/tabella | Colonne utili |
| -------------- | ------------- |
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

I campi di testo localizzati possono essere forniti con la convenzione `field:lang` dell'app. Ad esempio, mantenere `name` e `description` come lingua predefinita/di fallback, quindi aggiungere `name:fr` o `description:fr` per il testo francese, oppure `name:de` o `description:de` per quello tedesco. Datannurpy conserva le colonne localizzate per ogni campo di entità già presente nello schema, come `name:fr`, `name:de` e `description:fr` su dataset, variabili, tag, organizzazioni, concetti, enumerazioni, valori, documenti ed etichette dei filtri dell'app. Una cella localizzata vuota non contribuisce nulla da quella sorgente (il valore ricade su una sorgente inferiore o sulla scansione); usare `!` per svuotare esplicitamente un valore localizzato.

Per le sorgenti di metadati basate su cartella, dare a ogni file il nome dell'entità (`folder.csv`, `folder.xlsx`, `folder.json`, ecc.). I campi lista come `tag_ids`, `doc_ids` ed `enumeration_ids` possono essere scritti come valori separati da virgola nei file tabellari, o come array in JSON. Per i dettagli completi degli schemi, usare come riferimento gli schemi datannur linkati sopra.

**Comportamento di merge:**

- Le entità esistenti vengono aggiornate (i valori manuali sovrascrivono quelli ottenuti dalla scansione)
- Le nuove entità vengono create
- I campi lista (`tag_ids`, `doc_ids`, ecc.) vengono uniti
- Una cella vuota, un array JSON vuoto o una colonna mancante significano "nessun valore corrente da questa sorgente di metadati" — non contribuiscono nulla, quindi il valore ricade su ciò che fornisce una sorgente di metadati inferiore o la scansione. **Non** congela il valore dell'esportazione precedente. Svuotare una cella per rimuovere il proprio override e tornare al valore scansionato; usare `!` per forzare il valore finale a vuoto anche quando la scansione ne fornisce uno

**Ordinamento:** i metadati vengono applicati automaticamente prima dell'esportazione/finalizzazione, dopo tutte le chiamate `add_folder`, `add_dataset` e `add_database`, così i valori manuali hanno la precedenza. Se `app_path/data/db-ui` esiste, viene caricato automaticamente come ultima sorgente di metadati, dopo le sorgenti `metadata_path` configurate. Questo permette alle modifiche locali fatte nell'app di sovrascrivere i metadati scansionati senza aggiungere `data/db-ui` alla configurazione. `data/db-ui` usa gli stessi formati di metadati supportati di qualsiasi altra sorgente; `data/db` resta l'esportazione generata.

### Istruzioni di overlay

Le sorgenti di metadati possono includere piccole istruzioni per svuotare campi, rimuovere singole relazioni o eliminare entità prima dell'esportazione. Queste istruzioni vengono consumate dal builder e non vengono scritte in `data/db`.

Usare `!` come valore esatto di un campo scalare per svuotarlo:

```csv
id,description
source---employees_csv,!
```

Usare `!` come valore esatto di un campo di relazione per rimuovere tutte le relazioni accumulate per quel campo:

```csv
id,tag_ids
source---employees_csv,!
```

Usare `!id` all'interno di una lista di relazioni per rimuovere una relazione accumulata mantenendone o aggiungendone altre:

```json
[
  {
    "id": "source---employees_csv---salary",
    "tag_ids": ["finance", "!auto---numeric"]
  }
]
```

Le rimozioni di relazioni vengono applicate nell'ordine delle sorgenti di metadati. Se una riga contiene sia `id` sia `!id`, per quella relazione prevale la rimozione. I campi di relazione supportati sono `tag_ids`, `doc_ids`, `enumeration_ids` e `source_variable_ids`.

### Eliminazione {#deletion}

L'esportazione [rispecchia le sorgenti correnti](/it/output#incremental-scan), ma il modo in cui un'entità scompare dipende dal suo tipo:

- **Entità padre** (`folder`, `dataset`, `enumeration`, `organization`, `tag`, `doc`, `concept`): vengono riconciliate automaticamente — un'entità non più prodotta dalla scansione e assente dai metadati viene eliminata alla prossima esecuzione.
- **Entità figlie** (`variable`, `value`, `frequency`): non vengono tracciate singolarmente. Rimuovere la loro riga da un file di metadati **non** le elimina — scompaiono soltanto quando il loro padre viene riscansionato o rimosso (cascata), oppure, per le variabili, tramite un `_delete` esplicito. Come rete di sicurezza, una variabile il cui dataset non esiste più viene rimossa in fase di esportazione, così il catalogo resta referenzialmente coerente.

Usare `_delete: true` per rimuovere immediatamente un'entità dal catalogo finale:

```json
[
  {
    "id": "source---old_dataset",
    "_delete": true
  }
]
```

`_delete` è supportato per tutte le entità del catalogo con chiave ID (inclusa `variable`). Le tabelle composite (`value` e `frequency`) non possono essere eliminate direttamente; vengono rimosse tramite le cascate dalla loro enumerazione, variabile, dataset o cartella padre.

Le cascate vengono applicate prima dell'esportazione:

- l'eliminazione di una cartella rimuove le sue cartelle discendenti, i dataset contenuti, le variabili dei dataset, le frequenze, le anteprime e le enumerazioni contenute;
- l'eliminazione di un dataset rimuove le sue variabili, le frequenze e le anteprime;
- l'eliminazione di una variabile rimuove le sue frequenze;
- l'eliminazione di enumerazioni, tag, documenti, concetti o organizzazioni ripulisce anche i riferimenti correlati dove necessario.

## Pattern metadata-first

Quando la struttura del catalogo (cartelle, ID dei dataset, gerarchia) è definita interamente da `metadata_path` e il filesystem fornisce soltanto i file da scansionare per le informazioni tecniche (variabili, statistiche, dimensioni, formati), usare `create_folders=False`:

```yaml
metadata_path: ./metadata
add:
  - folder: ./data/parquet
    create_folders: false
```

In questa modalità:

- Nessuna cartella viene creata dalla directory scansionata; la gerarchia è presa da `metadata/folder.csv`.
- Ogni file scansionato viene associato alla propria voce di metadati tramite `_match_path` quando presente, o tramite `data_path` come fallback. La scansione riutilizza `id` e `folder_id` definiti nei metadati.
- I file senza corrispondenza nei metadati vengono segnalati secondo `on_unmatched`: `"warn"` (predefinito), `"skip"` o `"error"`.

**Corrispondenza:** `_match_path` è una chiave tecnica opzionale usata soltanto per associare i file scansionati alle righe di metadati. Usarla quando la chiave di scansione differisce dal `data_path` pubblico, ad esempio per sorgenti remote o dataset logici di serie temporali. Se `_match_path` è omesso, come chiave di corrispondenza viene usato `data_path`.

Per i percorsi remoti, le credenziali non fanno parte dell'identità di corrispondenza: `sftp://user@example.org/path/file.csv` e `sftp://example.org/path/file.csv` corrispondono alla stessa chiave di scansione. L'identità remota canonica mantiene il protocollo, l'host, la porta opzionale e il percorso.

Per i dataset di serie temporali, `_match_path` può usare la stessa sintassi normalizzata dei periodi prodotta da datannurpy: `[YYYY]`, `[YYYY/MM]`, `[YYYY]Q[N]` o `[YYYY/MM/DD]`. In questo modo corrisponde alla serie logica, non soltanto all'ultimo file fisico.

**Esempio** — `./metadata/dataset.csv`:

```csv
id,name,folder_id,data_path
sales-2024,Sales 2024,finance,../data/parquet/sales-2024.parquet
hr-headcount,HR headcount,hr,../data/parquet/hr-headcount.parquet
```

Combinata con `add_folder('./data/parquet', create_folders=False)`, la scansione associa variabili e statistiche a `sales-2024` e `hr-headcount` (gli ID dichiarati nei metadati) senza creare alcuna cartella dalla struttura su disco.

**Esempio remoto e con serie temporali** — `./metadata/dataset.csv`:

```csv
id,name,folder_id,data_path,_match_path
sales,Sales,finance,sftp://example.org/shared/data/sales_2024.csv,sftp://example.org/shared/data/sales_[YYYY].csv
traffic,Traffic,transport,sftp://example.org/shared/data/traffic_2024-03.csv,sftp://example.org/shared/data/traffic_[YYYY/MM].csv
```

Qui `data_path` resta il percorso pubblico mostrato nel catalogo, mentre `_match_path` è usato soltanto per associare il risultato della scansione alla riga di metadati. Se la scansione SFTP è configurata come `sftp://user@example.org/shared/data`, la parte utente viene ignorata per la corrispondenza.

**Vincoli:**

- `create_folders=False` è incompatibile con `metadata=` in `add_folder()` (nessuna cartella viene creata).
- Il `data_path` scritto in `metadata/dataset.csv` viene esportato così com'è nell'output. Usare `_match_path` quando la chiave tecnica di scansione deve differire dal percorso pubblico mostrato nel front-end.

## Variabili d'ambiente


Le variabili d'ambiente (`$VAR` o `${VAR}`) vengono espanse in tutti i valori YAML. Tutte le sorgenti vengono caricate — `env:`, `env_file` e il file `.env` accanto al file YAML:

```yaml
env:
  data_dir: /shared/data
  db_host: db.example.com
env_file: /secure/path/.env    # segreti: DB_USER, DB_PASSWORD

add:
  - folder: ${data_dir}/sales
  - folder: ${data_dir}/hr
  - database: oracle://${DB_USER}:${DB_PASSWORD}@${db_host}:1521/ORCL
```

`env_file` supporta una lista di percorsi (l'ultimo sovrascrive il primo):

```yaml
env_file:
  - /shared/common.env         # valori predefiniti
  - /secure/credentials.env    # sovrascrive common.env
```

Priorità (vince il primo impostato): variabili d'ambiente di sistema > `env:` YAML > `env_file` > `.env` locale.

## app_config


Configurare la web app con voci chiave-valore (scritte come `config.json`):

```yaml
app_path: ./my-catalog
app_config:
  contact_email: contact@example.com
  more_info: "Data from [open data portal](https://example.com)."
```

Se `app_config` non è fornito, viene usato al suo posto `config.csv`/`config.xlsx`/`config.json` (colonne `id`, `value`) da `metadata_path`. Se non è fornito nessuno dei due, non viene generato alcun `config.json`.
