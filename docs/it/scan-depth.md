# Profondità di scansione

Il parametro `depth` controlla quanti metadati vengono estratti da file, cartelle, dataset e database. Può essere impostato una sola volta come valore predefinito globale, oppure sovrascritto per ogni voce `add`.

```yaml
depth: variable                   # predefinito globale

add:
  - folder: ./data                # eredita "variable"

  - folder: ./big
    depth: stat                   # override per questa voce

  - database: sqlite:///db.sqlite
    depth: dataset
```

| Funzionalità                          | `dataset` | `variable` | `stat`         | `value` (predefinito) |
| ------------------------------------- | :-------: | :--------: | :------------: | :-------------------: |
| Cartelle                               | ✓         | ✓          | ✓              | ✓                     |
| Dataset (formato, percorso, mtime)     | ✓         | ✓          | ✓              | ✓                     |
| Variabili (nomi, tipi)                 |           | ✓          | ✓              | ✓                     |
| Introspezione DB (PK, FK, commenti)    |           | ✓          | ✓              | ✓                     |
| Conteggio righe, statistiche           |           |            | ✓              | ✓                     |
| Enumerazioni, frequenze, pattern       |           |            |                | ✓                     |
| Auto-tagging (formato, sicurezza, testo) |         |            |                | ✓                     |

> **Nota:** a `depth="variable"`, i file CSV ed Excel estraggono soltanto i **nomi** delle colonne (i tipi richiedono la lettura dei dati, disponibile da `depth="stat"`). Tutti gli altri formati forniscono i tipi a questo livello.

**Casi d'uso tipici:**

- **`dataset`** — inventario rapido dei file/tabelle disponibili senza leggere i dati
- **`variable`** — scoperta leggera dello schema (nomi e tipi delle colonne)
- **`stat`** — profilazione dei dati senza rilevamento delle enumerazioni (più veloce di `value`)
- **`value`** — catalogo completo con tabelle di frequenza e assegnazione delle enumerazioni (predefinito)

Se servono le frequenze dei valori ma le enumerazioni sono gestite manualmente tramite `metadata_path`, usare `auto_enumerations: false` con `depth: value`. Le frequenze restano disponibili, ma le entità enumerazione automatiche e i collegamenti generati alle variabili vengono omessi.

## Auto-tagging

A `depth="value"` (predefinito), le colonne stringa vengono automaticamente etichettate in base al tipo di contenuto. I tag usano una gerarchia a due livelli sotto il padre `auto`:

| Categoria  | Tag                                                        |
| ---------- | ---------------------------------------------------------- |
| **Formato** | `auto---email`, `auto---phone`, `auto---uuid`, `auto---iban` |
| **Sicurezza** | `auto---bcrypt`, `auto---argon2`, `auto---jwt`, `auto---secret` |
| **Testo**  | `auto---structured`, `auto---semi-structured`, `auto---free-text` → `auto---natural-text` |

Ogni variabile riceve al massimo **un tag foglia**. Il frontend può usare `parent_id` per filtrare per categoria (ad esempio, selezionando `auto---security` vengono mostrate tutte le variabili bcrypt/argon2/jwt/secret).

Le **colonne con tag di sicurezza** (bcrypt, argon2, jwt, secret) hanno i valori grezzi delle frequenze soppressi — vengono emesse soltanto le frequenze dei pattern, così nessun segreto reale compare nel catalogo esportato.

## Tag di policy

I tag di policy permettono di controllare manualmente il comportamento della scansione per variabili specifiche. Come gli auto-tag, vivono sotto la gerarchia `scan` e vengono creati automaticamente — non serve alcuna voce in `tag.csv`.

| Tag                      | Effetto                                                        |
| ------------------------ | -------------------------------------------------------------- |
| `policy---frequency-hidden`   | Sopprime tutti i dati di frequenza ed enumerazione (le statistiche restano visibili) |

Assegnare il tag nei metadati `variable.csv`:

```csv
id,tag_ids
source---employees_csv---first_name,"policy---frequency-hidden"
```

Utile per colonne sensibili che l'auto-tagging non rileva (ad esempio nomi di persona, ID interni, codici di business).
