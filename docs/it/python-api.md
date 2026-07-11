# API Python

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

| Attributo      | Tipo                              | Descrizione                                        |
| -------------- | --------------------------------- | -------------------------------------------------- |
| app_path       | str \| Path \| None               | Percorso per l'esportazione dell'app autonoma; se contiene già `data/db`, viene riutilizzato per le scansioni incrementali |
| metadata_path  | str \| Path \| list[str \| Path] \| None | Cartella sorgente dei metadati, URI di database o lista di sorgenti |
| depth          | "dataset" \| "variable" \| "stat" \| "value" | Profondità di scansione predefinita (predefinito: "value") |
| refresh        | bool                              | Forza la riscansione completa ignorando la cache (predefinito: False) |
| on_scan_error  | "warn" \| "fail"                  | La scansione è sempre continue-on-error (un file/tabella fallito viene registrato e saltato). `"warn"` (predefinito) esce con 0; `"fail"` fa uscire la CLI con 2 quando un dataset è fallito, così la CI non pubblica in verde un catalogo troncato |
| on_metadata_error | "warn" \| "fail"               | Anche il caricamento dei metadati è continue-on-error (una tabella non valida viene registrata e saltata, le tabelle valide vengono comunque applicate). `"warn"` (predefinito) esce con 0; `"fail"` fa uscire la CLI con 3 quando una tabella di metadati ha fallito la validazione, così la CI non pubblica in verde un catalogo privato della sua curatela |
| freq_threshold | int                               | Numero massimo di valori distinti per il rilevamento delle frequenze e delle enumerazioni automatiche. Le stringhe oltre questa soglia ricevono frequenze dei pattern |
| auto_enumerations | bool                           | Crea enumerazioni automatiche dalle frequenze dei valori e le collega alle variabili (predefinito: True) |
| csv_encoding   | str \| None                       | Encoding CSV predefinito (utf-8, cp1252, ecc.)     |
| sample_size    | int \| None                       | Dimensione predefinita del campione per il rilevamento delle frequenze e delle enumerazioni automatiche (predefinito: 100_000) |
| preview_rows   | int \| Literal[False]             | Numero massimo predefinito di righe esportate nelle anteprime dei dataset alle profondità `stat`/`value` (predefinito: 100; 0 o false disabilita) |
| csv_skip_copy      | bool                              | Salta la copia temporanea UTF-8 per i CSV locali (predefinito: False) |
| app_config     | dict[str, str] \| None            | Configurazione chiave-valore per la web app        |
| quiet          | bool                              | Sopprime i log di avanzamento (predefinito: False) |
| verbose        | bool                              | Mostra i traceback completi in caso di errore (predefinito: False) |
| log_file       | str \| Path \| None               | Scrive il log completo di scansione su file (troncato a ogni esecuzione) |
| folder         | Table[Folder]                     | Tabella delle cartelle (`.all()`, `.count`, `.get_by(...)`) |
| dataset        | Table[Dataset]                    | Tabella dei dataset                                |
| variable       | Table[Variable]                   | Tabella delle variabili                            |
| enumeration    | Table[Enumeration]                | Tabella delle enumerazioni                         |
| value          | Table[Value]                      | Tabella dei valori di enumerazione                 |
| frequency      | Table[Frequency]                  | Tabella delle frequenze (calcolata)                |
| organization   | Table[Organization]               | Tabella delle organizzazioni |
| tag            | Table[Tag]                        | Tabella dei tag                                    |
| doc            | Table[Doc]                        | Tabella dei documenti                              |
| concept        | Table[Concept]                    | Tabella dei concetti del glossario di business     |
| config         | Table[Config]                     | Tabella chiave-valore di configurazione della web app |

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

| Parametro       | Tipo                                      | Predefinito | Descrizione                                   |
| --------------- | ----------------------------------------- | -------- | --------------------------------------------- |
| path            | str \| Path \| list[str \| Path]           | obbligatorio | Directory o lista di directory da scansionare |
| metadata        | EntityMetadata \| None                    | None     | Identità, collegamento al padre e metadati per la cartella radice |
| depth           | "dataset" \| "variable" \| "stat" \| "value" \| None | None     | Profondità di scansione (usa catalog.depth se None) |
| include         | list[str] \| None                         | None     | Pattern glob da includere                     |
| exclude         | list[str] \| None                         | None     | Pattern glob da escludere                     |
| recursive       | bool                                      | True     | Scansiona le sottodirectory                   |
| csv_encoding    | str \| None                               | None     | Override dell'encoding CSV                    |
| sample_size     | int \| None                               | None     | Righe campionate per il rilevamento delle frequenze e delle enumerazioni automatiche (sovrascrive il catalogo) |
| auto_enumerations | bool \| None                            | None     | Crea enumerazioni automatiche dalle frequenze dei valori per questa cartella (sovrascrive il catalogo) |
| preview_rows    | int \| Literal[False] \| None              | None     | Numero massimo di righe di anteprima per i dataset trovati in questa cartella (sovrascrive il catalogo; 0 o false disabilita) |
| csv_skip_copy       | bool \| None                              | None     | Salta la copia temporanea UTF-8 (sovrascrive il catalogo) |
| storage_options | dict \| None                              | None     | Opzioni per lo storage remoto (passate a fsspec) |
| refresh         | bool \| None                              | None     | Forza la riscansione (sovrascrive l'impostazione del catalogo) |
| quiet           | bool \| None                              | None     | Sovrascrive l'impostazione quiet del catalogo |
| time_series     | bool                                      | True     | Raggruppa i file con pattern temporali        |
| create_folders  | bool                                      | True     | Se False, non crea cartelle dal disco; la struttura è affidata a `metadata_path` (metadata-first) |
| on_unmatched    | "skip" \| "warn" \| "error"               | "warn"   | Politica quando un file scansionato non ha corrispondenza nei metadati (solo con `create_folders=False`) |

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

| Parametro       | Tipo                                      | Predefinito | Descrizione                                   |
| --------------- | ----------------------------------------- | -------- | --------------------------------------------- |
| path            | str \| Path \| list[str \| Path]           | obbligatorio | File (uno o più) o directory partizionata (locale/remota) |
| metadata        | EntityMetadata \| None                    | None     | Identità del dataset, collegamento al padre e metadati |
| depth           | "dataset" \| "variable" \| "stat" \| "value" \| None | None     | Profondità di scansione (usa catalog.depth se None) |
| csv_encoding    | str \| None                               | None     | Override dell'encoding CSV                    |
| sample_size     | int \| None                               | None     | Righe campionate per il rilevamento delle frequenze e delle enumerazioni automatiche (sovrascrive il catalogo) |
| auto_enumerations | bool \| None                            | None     | Crea enumerazioni automatiche dalle frequenze dei valori per questo dataset (sovrascrive il catalogo) |
| preview_rows    | int \| Literal[False] \| None              | None     | Numero massimo di righe di anteprima per questo dataset (sovrascrive il catalogo; 0 o false disabilita) |
| csv_skip_copy       | bool \| None                              | None     | Salta la copia temporanea UTF-8 (sovrascrive il catalogo) |
| storage_options | dict \| None                              | None     | Opzioni per lo storage remoto (passate a fsspec) |
| refresh         | bool \| None                              | None     | Forza la riscansione (sovrascrive l'impostazione del catalogo) |
| quiet           | bool \| None                              | None     | Sovrascrive l'impostazione quiet del catalogo |

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

| Parametro          | Tipo                                            | Predefinito | Descrizione                                |
| ------------------ | ----------------------------------------------- | -------- | ------------------------------------------ |
| connection         | str \| ibis.BaseBackend                          | obbligatorio | Stringa di connessione o oggetto backend ibis |
| metadata           | EntityMetadata \| None                          | None     | Identità, collegamento al padre e metadati per la cartella radice |
| depth              | \"dataset\" \| \"variable\" \| \"stat\" \| \"value\" \| None | None     | Profondità di scansione (usa catalog.depth se None) |
| schema             | str \| list[str] \| None                         | None     | Schema (uno o più) da scansionare          |
| include            | list[str] \| None                               | None     | Pattern glob confrontati con i nomi delle tabelle da includere |
| exclude            | list[str] \| None                               | None     | Pattern glob confrontati con i nomi delle tabelle da escludere |
| sample_size        | int \| None                                     | None     | Righe campionate per il rilevamento delle frequenze e delle enumerazioni automatiche (sovrascrive il catalogo) |
| auto_enumerations  | bool \| None                                    | None     | Crea enumerazioni automatiche dalle frequenze dei valori per le tabelle scansionate (sovrascrive il catalogo) |
| preview_rows       | int \| Literal[False] \| None                    | None     | Numero massimo di righe di anteprima per i dataset delle tabelle scansionate (sovrascrive il catalogo; 0 o false disabilita) |
| group_by_prefix    | bool \| str                                     | True     | Raggruppa le tabelle per prefisso in sottocartelle |
| prefix_min_tables  | int                                             | 2        | Numero minimo di tabelle per formare un gruppo di prefisso |
| time_series        | bool                                            | True     | Rileva i pattern temporali delle tabelle   |
| storage_options    | dict \| None                                    | None     | Opzioni per SQLite/GeoPackage remoti       |
| refresh            | bool \| None                                    | None     | Forza la riscansione (sovrascrive l'impostazione del catalogo) |
| quiet              | bool \| None                                    | None     | Sovrascrive l'impostazione quiet del catalogo |
| oracle_client_path | str \| None                                     | None     | Percorso delle librerie Oracle Instant Client |
| ssh_tunnel         | dict \| None                                    | None     | Configurazione del tunnel SSH (host, user, port, ecc.) |


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

| Parametro         | Tipo                                            | Predefinito | Descrizione                                |
| ----------------- | ----------------------------------------------- | -------- | ------------------------------------------ |
| path              | str \| Path                                      | obbligatorio | Percorso o URL remoto della directory `.gdb` |
| metadata          | EntityMetadata \| None                          | None     | Identità, collegamento al padre e metadati per la cartella contenitore |
| depth             | "dataset" \| "variable" \| "stat" \| "value" \| None | None     | Profondità di scansione (usa catalog.depth se None) |
| include           | list[str] \| None                               | None     | Pattern glob confrontati con i nomi dei layer da includere |
| exclude           | list[str] \| None                               | None     | Pattern glob confrontati con i nomi dei layer da escludere |
| auto_enumerations | bool \| None                                    | None     | Crea enumerazioni automatiche dalle frequenze dei valori per i layer scansionati (sovrascrive il catalogo) |
| preview_rows      | int \| Literal[False] \| None                    | None     | Numero massimo di righe di anteprima per i dataset dei layer scansionati (sovrascrive il catalogo; 0 o false disabilita) |
| quiet             | bool \| None                                    | None     | Sovrascrive l'impostazione quiet del catalogo |
| refresh           | bool \| None                                    | None     | Forza la riscansione (sovrascrive l'impostazione del catalogo) |
| storage_options   | dict \| None                                    | None     | Opzioni per lo storage remoto (passate a fsspec) |

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

| Parametro       | Tipo             | Predefinito | Descrizione                                |
| --------------- | ---------------- | ------- | ------------------------------------------ |
| output_dir      | str \| Path \| None | None    | Directory di output (usa app_path se None) |
| track_evolution | bool             | True    | Traccia le modifiche tra le esportazioni   |
| copy_assets     | dict \| list[dict] \| None | None    | Copia file/directory locali aggiuntivi nell'esportazione con le stesse regole di `copy_assets()` |
| base_dir        | str \| Path \| None | None    | Directory di base per i percorsi relativi `copy_assets.from` (predefinita: directory di lavoro corrente) |
| export_size_report | bool         | False   | Stampa un report delle dimensioni di esportazione per tabella, incluse le dimensioni stimate dei `.json` gzip |
| quiet           | bool \| None     | None    | Sovrascrive l'impostazione quiet del catalogo |

Esporta i file di metadati JSON. Chiama `finalize()` automaticamente quando sono stati scansionati dei dati.

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

| Parametro       | Tipo                | Predefinito | Descrizione                                |
| --------------- | ------------------- | ------- | ------------------------------------------ |
| output_dir      | str \| Path \| None | None    | Directory di output (usa app_path se None) |
| open_browser    | bool                | False   | Apre l'app nel browser dopo l'esportazione |
| track_evolution | bool                | True    | Traccia le modifiche tra le esportazioni   |
| update_app      | bool                | False   | Aggiorna i file front-end inclusi quando l'app esiste già |
| copy_assets     | dict \| list[dict] \| None | None    | Copia file/directory locali aggiuntivi nell'app esportata con le stesse regole di `copy_assets()` |
| base_dir        | str \| Path \| None | None    | Directory di base per i percorsi relativi `copy_assets.from` (predefinita: directory di lavoro corrente) |
| export_size_report | bool            | False   | Stampa un report delle dimensioni di esportazione per tabella per `data/db`, incluse le dimensioni stimate dei `.json` gzip |
| quiet           | bool \| None        | None    | Sovrascrive l'impostazione quiet del catalogo |

Esporta l'app datannur autonoma completa con i dati. Usa `app_path` per impostazione predefinita se impostato all'inizializzazione. Le app esistenti aggiornano `data/db` per impostazione predefinita; passare `update_app=True` per aggiornare i file front-end inclusi.

## `Catalog.finalize()`

```python
catalog.finalize()
```

Metodo avanzato del ciclo di vita. Rimuove le entità non più viste durante la scansione.

Nell'uso normale, di solito non è necessario chiamarlo direttamente: `export_db()` ed `export_app()` lo chiamano automaticamente dopo la scansione.


## `run_config()`

```python
from datannurpy import run_config

catalog = run_config(path)
```

| Parametro | Tipo       | Predefinito | Descrizione |
| --------- | ---------- | -------- | ----------- |
| path      | str \| Path | obbligatorio | File di configurazione YAML da caricare ed eseguire |

Esegue un flusso di lavoro `catalog.yml` e restituisce il `Catalog` risultante.

Chiavi YAML di primo livello riconosciute da `run_config()`:

| Chiave | Tipo | Descrizione |
| --- | --- | --- |
| `add` | list[dict] | Passi di scansione da eseguire con voci abbreviate come `folder`, `dataset` o `database` |
| `env` | dict[str, str] | Variabili d'ambiente da iniettare prima dell'espansione dei valori YAML |
| `env_file` | str \| Path \| list[str \| Path] | Uno o più file dotenv da caricare prima dell'espansione |
| `output_dir` | str \| Path \| None | Esporta soltanto i metadati JSON in questa directory invece di esportare un'app completa |
| `open_browser` | bool | Apre l'app generata nel browser dopo l'esportazione dell'app |
| `copy_assets` | dict \| list[dict] \| None | Copia file o directory locali aggiuntivi nell'esportazione |
| `track_evolution` | bool | Abilita o disabilita la generazione di `evolution.json` durante l'esportazione |
| `export_size_report` | bool | Abilita il report delle dimensioni di esportazione per tabella dopo la scrittura del database |
| `update_app` | bool | Aggiorna i file front-end inclusi quando l'esportazione avviene su un `app_path` esistente |
| `post_export` | str \| list[str] \| None | Esegue script Python al termine dell'esportazione |

Tutte le altre chiavi di primo livello non riservate vengono passate a `Catalog(...)` con gli stessi nomi di parametro, come `app_path`, `metadata_path`, `depth`, `refresh`, `sample_size`, `auto_enumerations`, `preview_rows`, `app_config`, `quiet`, `verbose` e `log_file`.

Per mantenere concisa questa pagina API, il comportamento YAML dettagliato è documentato nelle guide tematiche:

- opzioni di esportazione come `output_dir`, `open_browser`, `track_evolution`, `update_app`, `copy_assets` e `post_export`: vedere [Output & esportazioni](/it/output)
- opzioni di metadati e configurazione come `metadata_path`, `env`, `env_file` e `app_config`: vedere [Metadati & configurazione](/it/metadata)

I valori basati su percorso sono risolti relativamente alla directory del file YAML, salvo diversa indicazione in quelle guide.

## `copy_assets()`

```python
from datannurpy import copy_assets

copy_assets(output_dir, rules, *, base_dir=None, quiet=False)
```

| Parametro  | Tipo                          | Predefinito | Descrizione |
| ---------- | ----------------------------- | ------- | ----------- |
| output_dir | str \| Path                    | obbligatorio | Directory di esportazione da popolare |
| rules      | dict \| list[dict]             | obbligatorio | Regole di copia con la stessa forma del `copy_assets` YAML |
| base_dir   | str \| Path \| None           | None    | Directory di base per i percorsi relativi `from` (predefinita: directory di lavoro corrente) |
| quiet      | bool                           | False   | Sopprime i log di avanzamento della copia |

Ogni regola accetta `from`, `to`, `include` opzionale e `clean` opzionale.

`Catalog.export_db()` e `Catalog.export_app()` accettano anche `copy_assets=` e `base_dir=` come wrapper di comodo attorno a questo helper.

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

| Parametro      | Tipo              | Descrizione |
| -------------- | ----------------- | ----------- |
| id             | str \| None       | ID esplicito dell'entità. Se omesso, vengono usati i valori derivati dalla scansione. |
| parent_id      | str \| None       | ID della cartella padre (`Folder.parent_id` per le cartelle, `Dataset.folder_id` per i dataset). |
| manager_organization_id | str \| None       | ID dell'organizzazione che gestisce. |
| owner_organization_id   | str \| None       | ID dell'organizzazione proprietaria. |
| tag_ids        | list[str] \| None | ID dei tag correlati. |
| doc_ids        | list[str] \| None | ID dei documenti correlati. |
| name           | str \| None       | Nome visualizzato. |
| description    | str \| None       | Testo della descrizione. |
| license        | str \| None       | Stringa di licenza. |
| type           | str \| None       | Tipo/categoria dell'entità. |
| link           | str \| None       | URL di riferimento esterno. |
| localisation   | str \| None       | Copertura geografica. |
| start_date     | str \| None       | Inizio del periodo coperto. |
| end_date       | str \| None       | Fine del periodo coperto. |
| updating_each  | str \| None       | Frequenza di aggiornamento. |
| no_more_update | str \| None       | Indicatore che non sono previsti ulteriori aggiornamenti. |

Nelle configurazioni YAML, gli stessi metadati si scrivono di solito come chiavi di primo livello su una voce `add`:

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

`EntityMetadata` è l'equivalente nell'API Python di quelle chiavi di metadati YAML.

## Helper per gli ID

```python
from datannurpy import sanitize_id, build_dataset_id, build_variable_id
```

| Funzione                                        | Descrizione                | Esempio                                                     |
| ----------------------------------------------- | -------------------------- | ----------------------------------------------------------- |
| sanitize_id(s)                                  | Pulisce una stringa per l'uso come ID | "My File (v2)" → "My File _v2_"                          |
| build_dataset_id(folder_id, dataset_name)       | Costruisce l'ID di un dataset | (\"src\", \"sales\") → \"src---sales\"                      |
| build_variable_id(folder_id, dataset_name, var) | Costruisce l'ID di una variabile | (\"src\", \"sales\", \"amount\") → \"src---sales---amount\" |
