# Conventions - EVS dbt Project

Index des conventions du projet. **Les règles détaillées vivent dans les docs par
couche** ci-dessous — c'est le premier endroit où regarder pour écrire un modèle.
Ce fichier ne garde que le **transversal** (commun à toutes les couches).

## Où trouver quoi

| Tu écris / modifies… | Doc de référence |
|---|---|
| un modèle staging (`stg_*`) | [`docs/conventions/staging.md`](docs/conventions/staging.md) |
| un modèle intermediate (`int_*`) | [`docs/conventions/intermediate.md`](docs/conventions/intermediate.md) |
| un mart (`dim_*` / `fct_*`) | [`docs/conventions/marts.md`](docs/conventions/marts.md) |
| un seed ou un snapshot | [`docs/conventions/seeds-snapshots.md`](docs/conventions/seeds-snapshots.md) |
| la fraîcheur d'une source | [`docs/freshness.md`](docs/freshness.md) |
| les environnements, la CI/CD, le defer | [`docs/environnements.md`](docs/environnements.md) |

---

## Nommage (transversal)

### Format des modèles

```
<couche>_<source>__<entite>.sql
```

Le double underscore `__` sépare la source (ou la BU pour les marts) de l'entité.

| Couche | Préfixe | Exemple |
|--------|---------|---------|
| Staging | `stg_` | `stg_oracle_neshu__company` |
| Intermediate | `int_` | `int_oracle_neshu__appro_tasks` |
| Marts - Dimension | `dim_` | `dim_neshu__company` |
| Marts - Fact | `fct_` | `fct_neshu__consommation` |
| Snapshot | `snap_` | `snap_oracle_neshu__device` |

> Staging/intermediate sont nommés **par source** ; les marts **par BU/domaine**
> (détail : [`docs/conventions/marts.md`](docs/conventions/marts.md) § Nommage).

### Fichiers YAML

| Fichier | Contenu |
|---------|---------|
| `_<source>__sources.yml` | Déclaration sources, freshness, colonnes brutes |
| `_<source>__models.yml` | Doc + tests des staging |
| `_<source>__intermediate_models.yml` | Doc + tests des intermediate |
| `_<bu>__marts_models.yml` / `_<bu>__marts_sources.yml` | Doc/tests des marts / tables externes Cloud Run |
| `_<source>__seeds.yml` | Doc + tests + `column_types` des seeds (un par source, dans `data/reference_data/<source>/`) |

### Colonnes

| Règle | Exemple |
|-------|---------|
| snake_case | `company_name`, `postal_code` |
| IDs : conserver le nom source en staging | `idcompany` (Oracle), `ticket_id` (Zoho) |
| IDs : `<entite>_id` en marts | `company_id`, `task_id` |
| Booléens : `is_` / `has_` | `is_active`, `has_contract` |
| Timestamps : suffixe `_at` | `created_at`, `updated_at` |
| Dates : suffixe `_date` | `start_date`, `end_date` |

> Détail du nommage des IDs par couche : staging (passthrough) →
> [`staging.md`](docs/conventions/staging.md) § 3 ; marts (`<entite>_id`) →
> [`marts.md`](docs/conventions/marts.md).

---

## Matérialisation (résumé)

| Couche | Défaut | Exception |
|--------|--------|-----------|
| Staging | `table` | `incremental` (merge) pour les grosses tables événementielles (tâches Oracle) |
| Intermediate | `table` | `incremental` (merge) pour les gros volumes (tâches, P&L) |
| Marts | `table` | — |
| Snapshots | `timestamp` (SCD2) | — |

Règles partition/cluster : partition sur la date filtre principale (Power BI /
incrémental), cluster sur les FK les plus jointes (≤ 4 colonnes). Détail dans
chaque doc de couche.

---

## Tests & sévérité (transversal)

> **Syntaxe obligatoire (dbt ≥ 1.11)** : tout test générique paramétré
> (`accepted_values`, `relationships`, `unique_combination_of_columns`,
> `expression_is_true`, tous les `dbt_expectations.*`) imbrique ses arguments sous
> `arguments:` et la severity sous `config:`. La forme à plat est **dépréciée**
> (warning aujourd'hui, erreur en 1.12). Le CI ne bloque pas dessus → vérifier en
> relecture de PR.

| Sévérité | Usage |
|----------|-------|
| `error` (défaut) | Unicité + not_null sur clés primaires, `accepted_values` |
| `warn` | Plages dates/numériques, volumes, `relationships`, fraîcheur |

Tests minimum **par couche** : voir le § Tests de chaque doc de couche.
Packages : `dbt_utils` (`unique_combination_of_columns`, `expression_is_true`,
`generate_surrogate_key`), `dbt_expectations` (row counts, plages, regex, recence).

---

## Lint SQL (`dbt lint`)

`dbt lint`, natif à dbt v2. Il lit la configuration `.sqlfluff` existante — mêmes codes de
règles, mêmes `-- noqa` — mais **ne se connecte pas à BigQuery** : il n'y a plus de templater
à choisir. SQLFluff est retiré du projet (incompatible v2).

| Règle | Paramètre |
|-------|-----------|
| Mots-clés / fonctions / types | `lowercase` |
| Alias tables & colonnes | explicites (`as`) |
| Virgule trailing | interdite |
| Indentation | 4 espaces |
| Longueur de ligne | 120 max |

```bash
dbt lint models/path/             # analyser
dbt lint models/path/ --fix       # corriger, puis vérifier avec git diff
dbt lint --changed                # seulement ce que le working tree a modifié
```

Désactiver une règle ponctuellement : `-- noqa: RF02` en fin de ligne.

> **Piège de configuration** : `capitalisation.functions` et `capitalisation.types` attendent
> `extended_capitalisation_policy`. `capitalisation_policy` n'est valide que pour `keywords` et
> `literals` ; sur les deux autres, la clé est ignorée et la règle retombe silencieusement sur
> `consistent`. Ce sont deux règles mortes sans le moindre message. Constaté le 2026-09-21 :
> 168 violations passaient la CI.

> **Parité** : `dbt lint` vise une couverture élevée de SQLFluff sans la garantir règle pour
> règle. Les règles de layout/indentation (LT02) sont la divergence connue.

---

## Tags

Chaque modèle est taggé par source (et par couche) pour l'exécution sélective,
via `dbt_project.yml` (hérité par dossier — pas de `tags=[...]` au niveau modèle) :

```bash
dbt build --select tag:oracle_neshu   # toute la chaîne d'une source
dbt build --select tag:staging        # toute une couche
```

---

## Source freshness

Tiers, mécanismes de monitoring (méthode A native / méthode B `dbt_expectations`)
et état par source : **[`docs/freshness.md`](docs/freshness.md)** (autorité
unique). Le placement du test côté staging est décrit dans
[`docs/conventions/staging.md`](docs/conventions/staging.md) § 8.

---

## Exposition Power BI

Les marts (`dim_*` / `fct_*`) sont la couche exposée à Power BI (`prod_marts`),
jointes via les colonnes `<entite>_id`. Déclarer chaque rapport consommateur dans
l'`exposure` de la BU (`models/exposures/<bu>.yml`). Détail :
[`docs/conventions/marts.md`](docs/conventions/marts.md).
