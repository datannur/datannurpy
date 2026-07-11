# Scansione dei database

Usare la [Profondità di scansione](/it/scan-depth) per scegliere quanti metadati datannurpy deve estrarre. La stessa impostazione `depth` si applica a `add_database`, `add_folder` e `add_dataset`, a livello globale o per singola voce `add`.

## Stringhe di connessione

| Backend    | Formato                                             |
| ---------- | --------------------------------------------------- |
| SQLite     | `sqlite:///path/to/db.sqlite`                       |
| GeoPackage | `sqlite:///path/to/geodata.gpkg`                    |
| PostgreSQL | `postgresql://user:pass@host:5432/database`         |
| MySQL      | `mysql://user:pass@host:3306/database`              |
| Oracle     | `oracle://user:pass@host:1521/service_name`         |
| SQL Server | `mssql://user:pass@host:1433/database`              |
| SQLite remoto | `sftp://host/path/db.sqlite`                     |
| DuckDB     | passare direttamente un backend `ibis.duckdb.connect(...)` |

Un GeoPackage è un file SQLite, quindi viene scansionato come database — ogni layer diventa un dataset, ulteriormente arricchito con `crs`/`geometry_type`/`bbox`. Vedere [Formati geospaziali](/it/scanning-files#geospatial-formats).

## Uso di base

```yaml
add:
  - database: sqlite:///path/to/db.sqlite
  - database: postgresql://user:pass@host:5432/mydb
  - database: mysql://user:pass@host:3306/mydb
  - database: oracle://user:pass@host:1521/service_name
  - database: mssql://user:pass@host:1433/mydb
```

## SSL/TLS

```yaml
add:
  - database: postgresql://user:pass@host/db?sslmode=require
```

## SQL Server con autenticazione Windows

Richiede una configurazione Kerberos corretta.

```yaml
add:
  - database: mssql://host/db?TrustedConnection=yes
```

## Filtraggio e campionamento

```yaml
add:
  - database: postgresql://localhost/mydb
    schema: public
    include: ["sales_*"]
    exclude: ["*_tmp"]
    sample_size: 10000
    preview_rows: 25
    group_by_prefix: true       # raggruppa le tabelle per prefisso comune (predefinito)
    prefix_min_tables: 2        # numero minimo di tabelle per formare un gruppo
```

  `include` ed `exclude` vengono confrontati con i nomi delle tabelle dopo che il filtro opzionale `schema` ha selezionato quale/i schema scansionare. Usano pattern standard in stile glob (`*`, `?` e classi di caratteri come `[abc]`), non percorsi di filesystem. Il filtraggio prima mantiene le tabelle che corrispondono ad almeno un pattern `include` quando `include` è impostato, poi rimuove le tabelle che corrispondono a un qualsiasi pattern `exclude`.

  `sample_size` controlla le righe usate per il rilevamento delle frequenze e delle enumerazioni automatiche. Impostare `auto_enumerations: false` a livello globale o su una voce database per mantenere le frequenze di `depth: value` senza creare entità enumerazione automatiche né collegamenti generati alle variabili. `preview_rows` controlla il numero massimo di righe esportate per l'anteprima di ogni tabella alle profondità `stat` e `value`; il valore predefinito è `100`, e `0` o `false` disabilita le anteprime. I dataset di serie di tabelle usano come anteprima la tabella del periodo più recente, in coerenza con la scansione delle statistiche.

  Esempi:

  | Pattern | Significato |
  | ------- | ----------- |
  | `employees` | Nome esatto della tabella |
  | `sales_*` | Tabelle il cui nome inizia con `sales_` |
  | `*_tmp` | Tabelle il cui nome termina con `_tmp` |
  | `fact_????` | Tabelle come `fact_2024` |
  | `[de]*` | Tabelle che iniziano con `d` o `e` |

## Rilevamento delle serie temporali

Quando `time_series: true` (predefinito), le tabelle i cui nomi differiscono soltanto per un pattern temporale vengono raggruppate in un unico dataset, dopo l'applicazione dei filtri `include` ed `exclude`:

```text
sales_fact_202401
sales_fact_202402
sales_fact_202403
```

Questo crea un dataset chiamato `sales_fact_[YYYY/MM]` con `nb_resources=3`. Impostare `time_series: false` per trattare ogni tabella come un dataset separato.

Vedere [Raggruppamento di serie temporali](/it/time-series) per i pattern supportati, il raggruppamento dei file, l'evoluzione dello schema e le regole contro i falsi positivi.

## Schemi multipli

```yaml
add:
  - database: postgresql://localhost/mydb
    schema: [public, sales, hr]
```

## Tunnel SSH

Per database dietro un firewall. Richiede `pip install datannurpy[ssh]`.

```yaml
add:
  - database: mysql://user:pass@dbhost/mydb
    ssh_tunnel:
      host: ssh.example.com
      user: sshuser
```

Con più opzioni:

```yaml
add:
  - database: postgresql://user:pass@dbhost/mydb
    ssh_tunnel:
      host: bastion.example.com
      port: 2222
      user: admin
      key_file: ~/.ssh/id_rsa
```

## SQLite / GeoPackage remoti

I file SQLite e GeoPackage possono essere scansionati via SFTP o cloud storage. Il dizionario `storage_options` viene passato a [fsspec](https://filesystem-spec.readthedocs.io/).

```yaml
add:
  - database: sftp://host/path/to/db.sqlite
    storage_options:
      key_filename: /path/to/key

  - database: s3://bucket/geodata.gpkg
    storage_options:
      key: ${AWS_KEY}
      secret: ${AWS_SECRET}
```

Vedere [Storage remoto](/it/scanning-files#remote-storage) per l'elenco dei provider supportati e degli extra richiesti.

## Arricchimento dei metadati di database

Richiede [`depth: variable` o superiore](/it/scan-depth).

| Metadati                | Campo di destinazione | Backend            |
| ----------------------- | --------------------- | ------------------ |
| Chiavi primarie         | `Variable.key`        | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |
| Chiavi esterne          | `Variable.fk_variable_id`  | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |
| Commenti su tabelle/colonne | `description`     | PostgreSQL, MySQL, Oracle, SQL Server, DuckDB |
| NOT NULL, UNIQUE, INDEX | Tag automatici (`db---*`) | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |
| Auto-increment          | Tag automatico        | PostgreSQL, MySQL, Oracle, SQL Server, SQLite, DuckDB |

Questi metadati vengono sempre aggiornati, anche quando i dati della tabella sono invariati (cache hit).
