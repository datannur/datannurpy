# Output & esportazioni

## Ciclo di vita del catalogo {#catalog-lifecycle}

Il `data/db` esportato è sempre usa e getta — riscritto a ogni esecuzione, quindi non va mai modificato a mano. Le modifiche vanno mantenute nella **fonte di verità**, e dove questa risiede definisce il flusso di lavoro:

- **Artefatto** — la fonte di verità sono file esterni (`metadata_path`, cartelle scansionate); l'output è nel gitignore e viene rigenerato dalla CI. Da abbinare a `refresh: true` e `on_scan_error: fail` / `on_metadata_error: fail`, così un'esecuzione difettosa fallisce in rosso invece di pubblicare un catalogo degradato. È il flusso del template GitHub Pages.
- **Installazione** — l'app vive in loco a lungo termine; le modifiche avvengono al suo interno (`db-ui`) e i file inclusi vengono aggiornati con `update_app: true`. Lo stato locale dell'app sotto `data/` viene preservato tra le esecuzioni (vedere [Scansione incrementale](#incremental-scan)).

I due approcci si combinano — un'installazione può comunque unire metadati esterni (`db-source`), come fa `examples/demo_editorial.yml`. Una sola regola è fissa: `data/db` è output, le modifiche appartengono ai file di `metadata_path` o al `db-ui` interno all'app.

## Output


```yaml
# App autonoma completa
app_path: ./my-catalog
open_browser: true

# Solo metadati JSON (per un'istanza datannur esistente)
output_dir: ./output
```

Opzioni di esportazione di primo livello:

| Chiave | Tipo | Predefinito | Si applica a | Descrizione |
|---|---|---|---|---|
| `app_path` | path | `None` | esportazione app | Directory di output per un'app datannur autonoma |
| `output_dir` | path | `None` | esportazione db | Directory di output per i soli metadati JSON |
| `open_browser` | bool | `false` | esportazione app | Apre l'app generata nel browser dopo l'esportazione |
| `refresh` | bool | `false` | scansione | Forza una riscansione completa invece di riutilizzare file o tabelle invariati |
| `track_evolution` | bool | `true` | esportazione app + db | Scrive `evolution.json` con le entità aggiunte, aggiornate ed eliminate |
| `update_app` | bool | `false` | esportazione app | Aggiorna i file front-end inclusi quando `app_path` esiste già |
| `copy_assets` | regola o lista di regole | `None` | esportazione app + db | Copia file o directory locali aggiuntivi nell'esportazione |
| `export_size_report` | bool | `false` | esportazione app + db | Stampa un report delle dimensioni per tabella dopo la scrittura del database |
| `post_export` | nome di script, percorso o lista | `None` | esportazione app + db | Esegue script Python al termine dell'esportazione |

Impostare `export_size_report: true` per stampare un report delle dimensioni per tabella dopo la scrittura del database. Il report include le dimensioni dei `.json` grezzi, dei `.json.js` grezzi e dei `.json` gzip stimati, con percentuali, il che aiuta a identificare le tabelle che dominano il peso del catalogo.

### Esportazioni di grandi dimensioni

I cataloghi grandi sono di solito dominati da `frequency` e `value`, perché queste tabelle memorizzano valori ripetuti per molte variabili. Abilitare `export_size_report` per verificare quali tabelle contano prima di cambiare le impostazioni di scansione.

Se l'esportazione è più grande del previsto, le leve principali sono la profondità di scansione, la generazione delle frequenze, la generazione automatica delle enumerazioni e il campionamento. `depth: stat` mantiene le statistiche delle variabili senza scrivere tabelle di frequenza o enumerazioni; `depth: variable` mantiene soltanto i metadati a livello di schema; `auto_enumerations: false` mantiene le tabelle di frequenza di `depth: value` ma salta le entità enumerazione automatiche e i collegamenti generati alle variabili; `freq_threshold` controlla quando le colonne stringa ad alta cardinalità passano dalle frequenze dei valori alle frequenze dei pattern; `sample_size` limita le righe usate per i conteggi delle frequenze e il rilevamento automatico delle enumerazioni, mantenendo le statistiche di base sull'intero dataset.

`.json.js` riflette l'uso locale o da cartella condivisa (`file://`), `.json` riflette l'HTTP non compresso e `.json.gz` riflette l'HTTP servito con gzip.

### Anteprime dei dataset

Alle profondità `stat` e `value`, datannurpy esporta per impostazione predefinita piccole anteprime dei dataset. Le esportazioni solo database le scrivono in `<output_dir>/preview/<dataset_id>.json` e `<output_dir>/preview/<dataset_id>.json.js`; le esportazioni app collocano gli stessi file sotto `data/db/preview/`. Il file JSON è un array di oggetti riga, e il file JSON-JS usa `jsonjs.data['<dataset_id>']`, in linea con la convenzione delle tabelle di metadati.

Usare `preview_rows` per controllare il numero massimo di righe per dataset. Il predefinito è `100`; impostare `preview_rows: 0` o `preview_rows: false` a livello globale o su una singola voce `add` per disabilitare le anteprime per le sorgenti sensibili. Le anteprime non vengono raccolte alle profondità `dataset` o `variable`, perché queste modalità non leggono righe di dati.

## Scansione incrementale {#incremental-scan}


Rieseguire con lo stesso `app_path` per riscansionare soltanto i file modificati (confronto del mtime) o le tabelle modificate (confronto di schema + conteggio righe):

```yaml
app_path: ./my-catalog

add:
  - folder: ./data               # salta i file invariati
```

Usare `refresh: true` per forzare una riscansione completa.

I dataset la cui scansione è fallita vengono riscansionati una volta dopo un aggiornamento di datannurpy, così le correzioni dello scanner li raggiungono senza `refresh`.

**Ogni esecuzione rispecchia le sorgenti correnti.** L'esportazione riflette esattamente ciò che producono la scansione e i metadati correnti: un'entità presente in un'esecuzione precedente ma non più scansionata né presente nei metadati viene rimossa, non conservata. Le righe derivate dalla scansione sono memorizzate in cache sotto `data/db/_scan/`, così i file invariati possono essere saltati; il `data/db` esportato viene poi ricostruito a ogni pubblicazione a partire da quella base di scansione più i metadati correnti — è una materializzazione usa e getta, non un archivio ad accumulo, quindi gli overlay di metadati di un'esportazione precedente non filtrano mai nella base dell'esecuzione successiva. `_scan` è una cache interna (mai una tabella dell'app); eliminarla costa soltanto una riscansione completa. Se si smette di scansionare una sorgente, i suoi dataset scompaiono alla prossima esecuzione. Vedere le [semantiche di eliminazione](/it/metadata#deletion) per come questo differisce tra entità padre (dataset, cartelle, …) ed entità figlie (variabili, valori, frequenze).

Le esportazioni app esistenti aggiornano `data/db` per impostazione predefinita e preservano lo stato locale dell'app sotto `data/`. Per aggiornare i file front-end inclusi dopo un upgrade di datannurpy, impostare `update_app: true` o chiamare `catalog.export_app(update_app=True)`.

Quando `app_path/data/db-ui` esiste, viene caricato automaticamente come ultima sorgente di metadati prima dell'esportazione. Vedere [Metadati manuali](/it/metadata) per l'ordine di merge e le istruzioni di overlay.

## Tracciamento dell'evoluzione


Le modifiche tra un'esportazione e l'altra vengono tracciate automaticamente in `evolution.json`:

- **add**: nuova cartella, dataset, variabile, enumerazione, ecc.
- **update**: campo modificato (mostra il valore vecchio e quello nuovo)
- **delete**: entità rimossa

Filtraggio a cascata: quando un'entità padre viene aggiunta o eliminata, i suoi figli vengono automaticamente filtrati per ridurre il rumore. Ad esempio, l'aggiunta di un nuovo dataset non genera voci separate per ciascuna variabile.

Per disabilitare il tracciamento:

```yaml
track_evolution: false
```

## copy_assets


Copia file o directory locali nel catalogo esportato durante l'esportazione:

```yaml
copy_assets:
  - from: ./staging/docs
    to: data/doc
    include: "*.pdf"
    clean: true

  - from: ./data
    to: data/source
```

Regole:

- `from` è risolto relativamente alla directory del file di configurazione YAML
- `to` è risolto relativamente alla directory di esportazione e deve restare al suo interno
- le directory vengono copiate ricorsivamente; i file singoli vengono copiati nella directory di destinazione
- `include` è opzionale e accetta una stringa glob o una lista di glob
- `clean: true` rimuove i file di destinazione non presenti nell'insieme sorgente filtrato
- le copie sono incrementali: un file viene aggiornato soltanto quando è mancante, la sua dimensione è cambiata o il `mtime` della sorgente è più recente

Funziona sia con le esportazioni `app_path` sia con quelle `output_dir`.

## post_export


Esegue automaticamente script Python dopo l'esportazione:

```yaml
# Script singolo (nome semplice → app/scripts/python/start_app.py quando incluso,
# con fallback su python-scripts/start_app.py)
post_export: start_app

# Script multipli
post_export:
  - export_dcat
  - start_app
```

Risoluzione degli script:

| Formato | Percorso risolto |
|---|---|
| `start_app` | `{output}/app/scripts/python/start_app.py` se presente, altrimenti `{output}/python-scripts/start_app.py` |
| `hook.py` | `{config_dir}/hook.py` |
| `scripts/hook.py` | `{config_dir}/scripts/hook.py` |
| `/absolute/path.py` | `/absolute/path.py` |

I percorsi di script espliciti sono risolti relativamente alla directory del file di configurazione YAML, come le altre opzioni basate su percorso.

Per le esportazioni app, `copy_assets` viene eseguito dopo l'installazione della shell dell'app e prima della scrittura di `data/db`, così i file copiati possono partecipare all'esportazione finale del database. Viene inoltre eseguito prima di `post_export`, così gli script personalizzati possono consumare i file copiati.

Lo script incluso `export_dcat` scrive gli artefatti di esportazione semantica in `data/db-semantic/`, inclusi i file RDF DCAT, `validation.json` e `dcat-report.html` quando supportati dalla versione dell'app.

Funziona sia con le esportazioni `app_path` sia con quelle `output_dir`.
