# Conventions

Index des règles du projet. Ce fichier ne contient que ce qui est **commun à toutes les couches** ;
le détail vit dans une page par couche.

| Tu écris… | Référence |
|---|---|
| un staging `stg_*` | [docs/conventions/staging.md](docs/conventions/staging.md) |
| un intermediate `int_*` | [docs/conventions/intermediate.md](docs/conventions/intermediate.md) |
| un mart `dim_*` / `fct_*` | [docs/conventions/marts.md](docs/conventions/marts.md) |
| un modèle de service d'application `app_*` | [docs/conventions/apps.md](docs/conventions/apps.md) |
| un seed ou un snapshot | [docs/conventions/seeds-snapshots.md](docs/conventions/seeds-snapshots.md) |
| un test de fraîcheur | [docs/freshness.md](docs/freshness.md) |
| rien, tu veux comprendre dev / CI / prod | [docs/environnements.md](docs/environnements.md) |

---

## Nommage

`<prefixe>_<source ou BU>__<entite>` : le double underscore sépare le périmètre de l'entité.

| Couche | Préfixe | Périmètre | Exemple |
|---|---|---|---|
| Staging | `stg_` | source | `stg_oracle_neshu__company` |
| Intermediate | `int_` | source | `int_oracle_lcdp__appro_tasks` |
| Dimension | `dim_` | **BU** | `dim_neshu__company` |
| Fait | `fct_` | **BU** | `fct_neshu__consommation` |
| Service d'application | `app_` | **application** | `app_cockpit__neshu_stock_photo` |
| Snapshot | `snap_` | source | `snap_oracle_neshu__device` |
| Seed | `ref_` | source | `ref_nesp_tech__key_facturation` |

| Fichier YAML | Contenu |
|---|---|
| `_<source>__sources.yml` | déclaration des sources et fraîcheur |
| `_<source>__models.yml` | doc et tests du staging |
| `_<source>__intermediate_models.yml` | doc et tests de l'intermediate |
| `_<bu>__marts_models.yml` | doc et tests des marts |
| `_<application>__app_models.yml` | doc et tests de la couche `apps` |
| `_<source>__seeds.yml` | doc, tests et `column_types` des seeds |
| `_<source>__snapshots.yml` | doc et tests des snapshots |

### Colonnes

| Règle | Exemple |
|---|---|
| snake_case | `company_name` |
| IDs : nom source en staging, `<entite>_id` en marts | `idcompany` → `company_id` |
| Booléens : `is_` / `has_` | `is_active` |
| Timestamps : `_at` · dates : `_date` | `created_at`, `start_date` |
| Mesures : préfixe de nature | `qty_`, `nb_`, `ca_`, `taux_` ([marts.md § 6](docs/conventions/marts.md#6-nommage-des-mesures)) |

---

## Matérialisation

| Couche | Défaut | Exception |
|---|---|---|
| Staging | `table` | `incremental` (`merge`) : grosses tables de tâches Oracle |
| Intermediate | `table` | `incremental` (`merge`) : modèles à gros volume |
| Marts | `table` | — |
| Apps | explicite par modèle | `table`, `incremental` sur gros volume, `view` pour l'inter-BU ([apps.md](docs/conventions/apps.md)) |

Partition sur la date filtrée (Power BI ou incrémental), cluster sur les FK les plus jointes
(4 au maximum). Pas de partition sur les petites dimensions.

---

## Tests

Les arguments d'un test générique s'écrivent sous `arguments:`, la sévérité sous `config:` :

```yaml
- relationships:
    arguments:
      to: ref('dim_neshu__company')
      field: company_id
    config:
      severity: warn
```

| Sévérité | Pour |
|---|---|
| `error` (défaut) | PK `unique` + `not_null`, FK obligatoires, clés composites, `accepted_values` en marts |
| `warn` | `relationships`, plages de valeurs, volumes, `accepted_values` en staging et intermediate (une nouvelle valeur source ne doit pas bloquer l'aval) |

Minimum par couche : voir la page de chaque couche. Paquets : `dbt_utils`, `dbt_expectations`.

---

## Lint SQL

`dbt lint`, intégré à dbt v2, lit `.sqlfluff`. Il ne se connecte pas à BigQuery.

| Règle | Valeur |
|---|---|
| Mots-clés, fonctions, types | minuscules |
| Alias | explicites (`as`) |
| Indentation | 4 espaces |
| Longueur de ligne | 120 |
| Virgule finale | interdite |

```bash
dbt lint models/chemin/            # analyser
dbt lint models/chemin/ --fix      # corriger, puis relire le git diff
dbt lint --changed                 # seulement les fichiers modifiés
```

Exception ponctuelle : `-- noqa: RF02` en fin de ligne.

> Dans `.sqlfluff`, `capitalisation.functions` et `capitalisation.types` prennent
> `extended_capitalisation_policy`. Avec `capitalisation_policy`, la règle est ignorée sans message.

---

## Tags

Posés par dossier dans `dbt_project.yml` (couche + source), **jamais** dans un `config()` :

```bash
dbt build -s tag:oracle_neshu     # toute une source
dbt build -s tag:marts            # toute une couche
```

---

## Commentaires et descriptions

Ils décrivent **l'état actuel et la règle**, pas l'histoire.

- **À écrire** : le pourquoi d'une règle non évidente, une règle métier et sa source, un piège qui
  se reproduirait. En 1 à 3 lignes.
- **À proscrire** : dates, numéros de PR, récits d'incident, volumes ou pourcentages relevés à un
  instant, « en cours », « temporaire », prénoms, « remplace… », « anciennement… ». L'historique
  va dans le message de commit et la description de PR.

Les descriptions YAML partent dans BigQuery (`persist_docs`) et sont lues par le data analyst et
par l'agent text-to-SQL : un chiffre périmé y devient une fausse information.

---

## Power BI

Le compte de service `powerbi` lit `prod_marts`, `prod_intermediate` et `prod_reference` (droits posés hors
Terraform). La cible est `prod_marts` seul : un rapport qui lit un `int_*` ou un seed est une dette à résorber
en mart. Les rapports joignent les tables sur `<entite>_id`. Chaque rapport consommateur est déclaré dans
`models/exposures/<bu>.yml`.
