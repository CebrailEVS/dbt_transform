# Conventions - EVS dbt Project

Regles de nommage, qualite et style appliquees dans le projet.

---

## Nommage des modeles

### Format general

```
<couche>_<source>__<entite>.sql
```

Le double underscore `__` separe la source de l'entite.

### Par couche

| Couche | Prefixe | Exemple |
|--------|---------|---------|
| Staging | `stg_` | `stg_oracle_neshu__company.sql` |
| Intermediate | `int_` | `int_oracle_neshu__appro_tasks.sql` |
| Marts - Dimension | `dim_` | `dim_oracle_neshu__company.sql` |
| Marts - Fact | `fct_` | `fct_oracle_neshu__conso_business_review.sql` |
| Snapshot | `snap_` | `snap_oracle_neshu__device.sql` |

### Fichiers YAML

Chaque source a deux fichiers YAML dans son dossier staging :

| Fichier | Contenu |
|---------|---------|
| `_<source>__sources.yml` | Declaration des sources, freshness, colonnes brutes |
| `_<source>__models.yml` | Documentation des modeles staging, tests |

---

## Nommage des colonnes

### Conventions generales

| Regle | Exemple |
|-------|---------|
| snake_case | `company_name`, `postal_code` |
| IDs : `id<entite>` (staging) | `idcompany`, `idtask` |
| IDs : `<entite>_id` (marts) | `company_id`, `task_id` |
| Booleens : `is_` ou `has_` | `is_active`, `has_contract` |
| Dates : `_at` pour timestamps | `created_at`, `updated_at` |
| Dates : `_date` pour dates | `start_date`, `end_date` |

### Colonnes systeme (staging)

Chaque modele staging expose ces colonnes harmonisees :

| Colonne | Type | Description |
|---------|------|-------------|
| `created_at` | TIMESTAMP | Date de creation dans la source |
| `updated_at` | TIMESTAMP | Derniere modification (coalesce avec created_at) |
| `extracted_at` | TIMESTAMP | Date d'extraction Meltano (`_sdc_extracted_at`) |
| `deleted_at` | TIMESTAMP | Soft delete (`_sdc_deleted_at`) |

---

## Structure SQL

### Staging : pattern CTE standard

```sql
{{ config(materialized='table') }}

with source_data as (
    select * from {{ source('oracle_neshu', 'evs_company') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idcompany as int64) as idcompany,

        -- Colonnes texte
        code,
        name,

        -- Timestamps harmonises
        timestamp(creation_date) as created_at,
        timestamp(coalesce(modification_date, creation_date)) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at

    from source_data
)

select * from cleaned_data
```

### Intermediate / Marts : pattern CTE

- Utiliser des CTEs nommees pour chaque etape logique
- Preferer `ref()` vers staging (jamais `source()` en intermediate/marts)
- Commenter les sections avec `-- Identifiants`, `-- Metriques`, etc.

---

## Materialisation

| Couche | Materialisation | Raison |
|--------|----------------|--------|
| Staging | `table` | Exploration ad-hoc par les analystes |
| Intermediate | `table` | Idem, tables reutilisees par plusieurs marts |
| Marts | `table` | Exposition Power BI, performance requise |
| Snapshots | `timestamp` | SCD Type 2, suivi historique |

Certains modeles intermediate utilisent `incremental` pour les gros volumes (ex: `int_oracle_neshu__appro_tasks`).

---

## Tests de qualite

### Tests dbt natifs

| Test | Usage |
|------|-------|
| `unique` | Cle primaire unique |
| `not_null` | Colonne obligatoire |
| `relationships` | Integrite referentielle entre modeles |
| `accepted_values` | Valeurs autorisees (a ajouter progressivement) |

### dbt_utils

| Test | Usage |
|------|-------|
| `unique_combination_of_columns` | Cle composite unique |
| `expression_is_true` | Assertions metier personnalisees |

### dbt_expectations

Package pour des tests de qualite avances. Configuration dans les fichiers `_<source>__models.yml`.

| Test | Usage | Exemple |
|------|-------|---------|
| `expect_table_row_count_to_be_between` | Volume attendu | min: 100, max: 100000 |
| `expect_column_values_to_be_between` | Plage de valeurs | Dates entre 2015 et maintenant |
| `expect_column_values_to_not_be_null` | Taux de null acceptable | `mostly: 0.95` |
| `expect_column_values_to_match_regex` | Format de donnees | Codes, emails, SIRET |

Exemple YAML :

```yaml
models:
  - name: stg_oracle_neshu__company
    tests:
      - dbt_expectations.expect_table_row_count_to_be_between:
          arguments:
            min_value: 100
            max_value: 100000
    columns:
      - name: real_start_date
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              arguments:
                min_value: "timestamp('2015-01-01')"
                row_condition: "real_start_date is not null"
              config:
                severity: warn
```

> **Note dbt 1.11** : les parametres des tests generiques doivent etre enveloppes dans `arguments:`.

### Strategie de severite

| Severite | Usage |
|----------|-------|
| `error` (defaut) | Tests critiques : unicite, not_null sur cles primaires |
| `warn` | Tests informatifs : plages de dates, volumes, formats |

---

## Source freshness

Configuree dans les fichiers `_<source>__sources.yml` avec `loaded_at_field: _sdc_extracted_at`.

| Tier | warn_after | error_after | Sources |
|------|-----------|-------------|---------|
| Critique | 26h | 36h | oracle_neshu, oracle_lcdp |
| Standard | 26h | 48h | yuman, mssql_sage, nesp_tech |
| Relaxe | 7j | 14j | gac, yuman_gcs, oracle_neshu_gcs |

Commande : `dbt source freshness`

---

## SQLFluff

Le projet utilise [SQLFluff v4](https://sqlfluff.com/) avec le templater dbt. Configuration dans `.sqlfluff`.

### Regles principales

| Regle | Parametre |
|-------|-----------|
| Mots-cles | `lowercase` (select, from, where, join...) |
| Fonctions | `lowercase` (cast, coalesce, timestamp...) |
| Types | `lowercase` (int64, string, float64...) |
| Alias tables | Explicite (`AS t`, pas juste `t`) |
| Alias colonnes | Explicite (`AS nom`, pas juste `nom`) |
| Virgule trailing | Interdit dans SELECT |
| Indentation | 4 espaces |
| Longueur de ligne | 120 caracteres max |

### Commandes

```bash
# Analyser
sqlfluff lint models/staging/oracle_neshu/
sqlfluff lint models/

# Corriger automatiquement
sqlfluff fix models/staging/oracle_neshu/

# Toujours verifier apres un fix
git diff
```

### Fichiers exclus

Configure dans `.sqlfluffignore` : `target/`, `dbt_packages/`, `logs/`, `.venv/`, `venv_dbt/`.

### `-- noqa`

Pour desactiver une regle sur une ligne specifique (faux positifs) :

```sql
select max(updated_at) from {{ ref('stg_oracle_neshu__task') }}  -- noqa: RF02
```

---

## Seeds et Snapshots

### Seeds (`seeds/`)

Fichiers CSV charges via `dbt seed`. Utilises pour les donnees de reference statiques (mappings, parametres).

### Snapshots (`snapshots/`)

Suivi historique SCD Type 2 sur les entites qui evoluent :

| Snapshot | Entite | Strategie |
|----------|--------|-----------|
| `snap_oracle_neshu__company` | Clients | timestamp |
| `snap_oracle_neshu__device` | Machines | timestamp |
| `snap_oracle_neshu__valo_parc_machines` | Parc machines | timestamp |

Commande : `dbt snapshot`

---

## Tags

Chaque modele est tag par source pour permettre une execution selective :

```bash
dbt run --select tag:oracle_neshu    # Toute la chaine oracle_neshu
dbt run --select tag:yuman           # Toute la chaine yuman
dbt run --select tag:staging         # Tous les staging
dbt run --select tag:marts           # Tous les marts
```

Les tags sont definis dans `dbt_project.yml` et herites automatiquement par les modeles enfants.
