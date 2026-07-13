# Sortie & exports

## Cycle de vie du catalogue {#catalog-lifecycle}

Le `data/db` exporté est toujours jetable — réécrit à chaque exécution, donc à ne jamais éditer à la main. Conservez vos éditions dans la **source de vérité**, et l'endroit où elle vit définit le workflow :

- **Artefact** — la source de vérité est constituée de fichiers externes (`metadata_path`, dossiers scannés) ; la sortie est gitignorée et régénérée par la CI. À combiner avec `refresh: true` et `on_scan_error: fail` / `on_metadata_error: fail` pour qu'une exécution cassée échoue en rouge au lieu de publier un catalogue dégradé. C'est le flux du template GitHub Pages.
- **Installation** — l'app vit en place sur le long terme ; vous éditez à l'intérieur (`db-ui`) et rafraîchissez les fichiers embarqués avec `update_app: true`. L'état local de l'app sous `data/` est préservé entre les exécutions (voir [Scan incrémental](#incremental-scan)).

Les deux se combinent — une installation peut toujours fusionner des métadonnées externes (`db-source`), comme le fait `examples/demo_editorial.yml`. Une seule règle est fixe : `data/db` est une sortie, les éditions appartiennent aux fichiers `metadata_path` ou au `db-ui` dans l'app.

## Sortie


```yaml
# App autonome complète
app_path: ./my-catalog
open_browser: true

# Métadonnées JSON uniquement (pour une instance datannur existante)
output_dir: ./output
```

Options d'export de premier niveau :

| Clé | Type | Défaut | S'applique à | Description |
|---|---|---|---|---|
| `app_path` | path | `None` | export app | Répertoire de sortie pour une app datannur autonome |
| `output_dir` | path | `None` | export db | Répertoire de sortie pour les métadonnées JSON uniquement |
| `open_browser` | bool | `false` | export app | Ouvrir l'app générée dans le navigateur après l'export |
| `refresh` | bool | `false` | scan | Forcer un rescan complet au lieu de réutiliser les fichiers ou tables inchangés |
| `track_evolution` | bool | `true` | export app + db | Écrire `evolution.json` avec les entités ajoutées, mises à jour et supprimées |
| `update_app` | bool | `false` | export app | Rafraîchir les fichiers embarqués du front-end quand `app_path` existe déjà |
| `copy_assets` | règle ou liste de règles | `None` | export app + db | Copier des fichiers ou répertoires locaux supplémentaires dans l'export |
| `export_size_report` | bool | `false` | export app + db | Afficher un rapport de taille par table après l'écriture de la base |
| `post_export` | nom de script, chemin ou liste | `None` | export app + db | Exécuter des scripts Python après la fin de l'export |

Définissez `export_size_report: true` pour afficher un rapport de taille par table après l'écriture de la base. Le rapport inclut les tailles `.json` brutes, `.json.js` brutes et `.json` gzippées estimées avec pourcentages, ce qui aide à identifier les tables qui dominent le poids du catalogue.

### Exports volumineux

Les gros catalogues sont généralement dominés par `frequency` et `value`, parce que ces tables stockent des valeurs répétées pour de nombreuses variables. Activez `export_size_report` pour vérifier quelles tables comptent avant de modifier les réglages de scan.

Si l'export est plus gros que prévu, les principaux leviers sont la profondeur de scan, la génération des fréquences, la génération automatique d'énumérations et l'échantillonnage. `depth: stat` conserve les statistiques des variables sans écrire les tables de fréquences ni les énumérations ; `depth: variable` ne conserve que les métadonnées de niveau schéma ; `auto_enumerations: false` conserve les tables de fréquences de `depth: value` mais ignore les entités d'énumération automatiques et les liens de variables générés ; `freq_threshold` contrôle quand les colonnes de chaînes à haute cardinalité passent des fréquences de valeurs aux fréquences de motifs ; `sample_size` limite les lignes utilisées pour les comptages de fréquences et la détection automatique d'énumérations tout en gardant les statistiques de base sur le dataset complet.

`.json.js` reflète l'usage local ou en dossier partagé (`file://`), `.json` reflète HTTP non compressé, et `.json.gz` reflète HTTP servi avec gzip.

### Aperçus de datasets

Aux profondeurs `stat` et `value`, datannurpy exporte par défaut de petits aperçus de datasets. Les exports base-de-données seule les écrivent dans `<output_dir>/preview/<dataset_id>.json` et `<output_dir>/preview/<dataset_id>.json.js` ; les exports d'app placent les mêmes fichiers sous `data/db/preview/`. Le fichier JSON est un tableau d'objets ligne, et le fichier JSON-JS utilise `jsonjs.data['<dataset_id>']`, suivant la convention des tables de métadonnées.

Utilisez `preview_rows` pour contrôler le nombre maximum de lignes par dataset. La valeur par défaut est `100` ; définissez `preview_rows: 0` ou `preview_rows: false` globalement ou sur une entrée `add` individuelle pour désactiver les aperçus des sources sensibles. Les aperçus ne sont pas collectés aux profondeurs `dataset` ou `variable`, car ces modes ne lisent pas les lignes de données.

## Scan incrémental {#incremental-scan}


Relancez avec le même `app_path` pour ne rescanner que les fichiers modifiés (comparaison des mtime) ou les tables modifiées (comparaison schéma + nombre de lignes) :

```yaml
app_path: ./my-catalog

add:
  - folder: ./data               # ignore les fichiers inchangés
```

Utilisez `refresh: true` pour forcer un rescan complet.

Les datasets dont le scan a échoué sont rescannés une fois après une mise à jour de datannurpy, pour que les corrections du scanner les atteignent sans `refresh`.

**Chaque exécution reflète les sources courantes.** L'export reflète exactement ce que produisent le scan et les métadonnées courants : une entité présente lors d'une exécution précédente mais qui n'est plus scannée ni présente dans les métadonnées est supprimée, pas conservée. Les lignes dérivées du scan sont mises en cache sous `data/db/_scan/` pour pouvoir ignorer les fichiers inchangés ; le `data/db` exporté est ensuite reconstruit à chaque publication à partir de cette base de scan plus les métadonnées courantes — c'est une matérialisation jetable, pas un magasin d'accumulation, de sorte que les overlays de métadonnées d'un export précédent ne fuient jamais dans la base de l'exécution suivante. `_scan` est un cache interne (jamais une table de l'app) ; le supprimer ne coûte qu'un rescan complet. Cessez de scanner une source et ses datasets disparaissent à l'exécution suivante. Voir la [sémantique de suppression](/fr/metadata#deletion) pour la différence entre entités parentes (datasets, dossiers, …) et entités enfants (variables, valeurs, fréquences).

Les exports d'app existants mettent à jour `data/db` par défaut et préservent l'état local de l'app sous `data/`. Pour rafraîchir les fichiers embarqués du front-end après une mise à niveau de datannurpy, définissez `update_app: true` ou appelez `catalog.export_app(update_app=True)`.

Quand `app_path/data/db-ui` existe, il est chargé automatiquement comme dernière source de métadonnées avant l'export. Voir [Métadonnées manuelles](/fr/metadata) pour l'ordre de fusion et les instructions d'overlay.

## Suivi des évolutions


Les changements entre exports sont automatiquement suivis dans `evolution.json` :

- **add** : nouveau dossier, dataset, variable, énumération, etc.
- **update** : champ modifié (affiche l'ancienne et la nouvelle valeur)
- **delete** : entité supprimée

Filtrage en cascade : quand une entité parente est ajoutée ou supprimée, ses enfants sont automatiquement filtrés pour réduire le bruit. Par exemple, l'ajout d'un nouveau dataset ne génère pas d'entrées séparées pour chacune de ses variables.

Désactiver le suivi :

```yaml
track_evolution: false
```

## copy_assets


Copier des fichiers ou répertoires locaux dans le catalogue exporté pendant l'export :

```yaml
copy_assets:
  - from: ./staging/docs
    to: data/doc
    include: "*.pdf"
    clean: true

  - from: ./data
    to: data/source
```

Règles :

- `from` est résolu relativement au répertoire du fichier de configuration YAML
- `to` est résolu relativement au répertoire d'export et doit rester à l'intérieur
- les répertoires sont copiés récursivement ; les fichiers isolés sont copiés dans le répertoire de destination
- `include` est optionnel et accepte une chaîne glob ou une liste de globs
- `clean: true` supprime les fichiers de destination absents de l'ensemble source filtré
- les copies sont incrémentales : un fichier n'est mis à jour que s'il est manquant, si sa taille a changé ou si le `mtime` de sa source est plus récent

Fonctionne avec les exports `app_path` comme `output_dir`.

## post_export


Exécuter automatiquement des scripts Python après l'export :

```yaml
# Script unique (nom nu → app/scripts/python/start_app.py quand embarqué,
# avec repli sur python-scripts/start_app.py)
post_export: start_app

# Plusieurs scripts
post_export:
  - export_dcat
  - start_app
```

Résolution des scripts :

| Format | Chemin résolu |
|---|---|
| `start_app` | `{output}/app/scripts/python/start_app.py` s'il existe, sinon `{output}/python-scripts/start_app.py` |
| `hook.py` | `{config_dir}/hook.py` |
| `scripts/hook.py` | `{config_dir}/scripts/hook.py` |
| `/absolute/path.py` | `/absolute/path.py` |

Les chemins de scripts explicites sont résolus relativement au répertoire du fichier de configuration YAML, comme les autres options basées sur des chemins.

Pour les exports d'app, `copy_assets` s'exécute après l'installation de la coquille de l'app et avant l'écriture de `data/db`, de sorte que les fichiers copiés peuvent participer à l'export final de la base. Il s'exécute aussi avant `post_export`, afin que les scripts personnalisés puissent consommer les fichiers copiés.

Le script embarqué `export_dcat` écrit les artefacts d'export sémantique dans `data/db-semantic/`, y compris les fichiers RDF DCAT, `validation.json` et `dcat-report.html` quand la version de l'app le prend en charge.

Fonctionne avec les exports `app_path` comme `output_dir`.
