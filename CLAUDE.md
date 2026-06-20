# CLAUDE.md — dbt_warehouse (EVS Professionnelle France)

## Project overview
ELT data warehouse for EVS Professionnelle France.
**Stack:** Meltano + Cloud Run jobs (extract) → BigQuery `prod_raw` (lake) → dbt (transform) → GCP Cloud Workflows (orchestrate) → Power BI (viz)
**dbt version:** 1.11.7 / dbt-bigquery 1.11.1
**Team:** 1 Data Engineer (owner), 1 Data Analyst (contributes to marts)

---

## Local dev setup

Required env vars (set in `.env`):
```
DBT_BIGQUERY_PROJECT=evs-datastack-prod
DBT_BIGQUERY_KEYFILE=/path/to/keyfile.json
DBT_BIGQUERY_DATASET_DEV=dev_      # prefix — actual datasets are dev_staging, dev_intermediate, dev_marts
DBT_TARGET=dev                     # defaults to dev if not set
```

Default target is `dev`. Dev uses the same GCP project as prod but writes to `dev_*` datasets.
Never run against `prod` target unless explicitly asked.

---

## Architecture — 3 layers

| Layer | Schema | Materialization |
|---|---|---|
| `models/staging/` | `prod_staging` / `dev_staging` | table (one model = `incremental`) |
| `models/intermediate/` | `prod_intermediate` / `dev_intermediate` | table |
| `models/marts/` | `prod_marts` / `dev_marts` | table |

**10 sources** in `prod_raw`: `oracle_neshu`, `oracle_lcdp`, `yuman`, `nesp_tech`, `nesp_co`, `mssql_sage`, `gac`, `yuman_gcs`, `oracle_neshu_gcs`, `oracle_lcdp_gcs`

Seeds are in `data/reference_data/<source>/` and land in `prod_reference` / `dev_reference`.

---

## Naming conventions

- **Staging / intermediate** : `<prefix>_<source>__<entity>.sql` (par source)
- **Marts** : `<prefix>_<bu>__<entity>.sql` (par BU/domaine, **post-refacto by BU**)
  - BUs : `neshu`, `lcdp`, `technique`, `commerce`, `finance`, `services_generaux`, `supply_chain`
  - Entité **singulier**, snake_case, nom métier (pas le nom source, pas le nom du rapport BI)
- Prefixes: `stg_` staging · `int_` intermediate · `dim_` dimension · `fct_` fact · `snap_` snapshot
- YAML files: `_<source>__models.yml` (staging/intermediate) · `_<bu>__marts_models.yml` (marts) · `_<bu>__marts_sources.yml` (external Cloud Run tables)
- Seeds: `ref_<source>__<entity>.csv`
- Columns: snake_case · IDs as `id<entity>` in staging, `<entity>_id` in marts
- Booleans: `is_` / `has_` prefix · timestamps: `_at` suffix · dates: `_date` suffix
- Every staging model exposes: `created_at`, `updated_at`, `extracted_at`, `deleted_at`

Voir [`docs/conventions/marts.md`](docs/conventions/marts.md) § Nommage pour les règles complètes (suffixe de grain, suffixe de source si collision, etc.).

---

## Common commands

```bash
# Build a specific model and its dependencies
dbt build -s +dim_oracle_neshu__resources

# Build all models for a source (by tag)
dbt build --select tag:oracle_neshu

# Build a full layer
dbt build --select tag:staging
dbt build --select tag:intermediate
dbt build --select tag:marts

# Lint before committing
sqlfluff lint models/path/to/model.sql --templater jinja

# Fix lint issues automatically
sqlfluff fix models/path/to/model.sql --templater jinja

# Run source freshness
dbt source freshness

# List all exposures
dbt ls --select exposure:*

# Build all models feeding a specific BI report
dbt build -s +exposure:business_review
```

> Note: `sqlfluff` requires `--templater jinja` when `DBT_BIGQUERY_PROJECT` is not set,
> otherwise use the default dbt templater with env vars loaded.

---

## Workflow for new models

Always follow this order — never skip layers. Each layer has a dedicated convention doc — read it before writing:

1. **Staging** ([`docs/conventions/staging.md`](docs/conventions/staging.md)) — clean/cast columns (passthrough naming), harmonise timestamps, expose all source fields. One staging model = one source table.
2. **Intermediate** ([`docs/conventions/intermediate.md`](docs/conventions/intermediate.md)) — business logic, task-type splits, enrichment. **Source-aligned, NOT cross-source** — multi-source unification happens in marts.
3. **Marts** ([`docs/conventions/marts.md`](docs/conventions/marts.md)) — final dims and facts for BI consumption.

For each new model, create the SQL and its YAML entry in the same PR:
- Staging YAML: `_<source>__models.yml` in the same folder
- Marts YAML: `_<bu>__marts_models.yml` in the BU folder

### Staging pattern
```sql
{{ config(materialized='table') }}
with source_data as (select * from {{ source('...', '...') }}),
cleaned_data as (
    select
        cast(id as int64) as id,
        ...
        timestamp(creation_date) as created_at,
        timestamp(coalesce(modification_date, creation_date)) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
    from source_data
)
select * from cleaned_data
```

### Marts dim pattern (label pivot)
Oracle Neshu dims use an EAV label system. Standard pattern (staging refs stay per-source, output dim is per-BU):
```sql
with entity_labels as (
    select e.*, l.code as label_code, lf.code as label_family_code
    from {{ ref('stg_oracle_neshu__entity') }} as e
    left join {{ ref('stg_oracle_neshu__label_has_entity') }} as lhe
        on e.identity = lhe.identity and lhe.idlabel is not null
    left join {{ ref('stg_oracle_neshu__label') }} as l on lhe.idlabel = l.idlabel
    left join {{ ref('stg_oracle_neshu__label_family') }} as lf on l.idlabel_family = lf.idlabel_family
),
aggregated_labels as (
    select
        ...,
        max(case when label_family_code = 'ISACTIVE' then label_code end) as is_active
    from entity_labels
    group by ...
)
select
    ...,
    coalesce(lower(is_active) = 'yes', false) as is_active
from aggregated_labels
```
> File path: `models/marts/neshu/dim_neshu__<entity>.sql`.

### When creating or modifying a mart

Always follow [`docs/conventions/marts.md`](docs/conventions/marts.md) (§ Marts — pattern complet). 4 piliers :

1. **Description YAML en 4 blocs** : `[QUOI MÉTIER]` / `[COMMENT CONSTRUITE]` / `[GRAIN]` / `[NOTES]`. Grain obligatoire (1 ligne par X).
2. **Tests minimum** : Dim → `unique` + `not_null` sur PK (error) + `accepted_values` / row count range (warn). Fact → `not_null` + `relationships` sur chaque FK (warn) + `unique_combination_of_columns` sur clé composite + `expression_is_true` sur invariants.
3. **Config block hygiène** : `{{ config() }}` pour matérialisation uniquement. Description en YAML, pas en config (persist_docs déjà actif). Pas de `tags=[...]` model-level.
4. **Star schema strict** : pas de jointure fait-à-fait, pas de snowflake, pas d'OBT (cf. [`docs/conventions/marts.md`](docs/conventions/marts.md) § 1 pour le pattern hybride flatten/relations PBI).
5. **Ordre des colonnes (`select` final)** : règle **grain-first** — colonnes du grain en tête (`dimension temporelle → PK → FK`), puis FK restantes → attributs texte → dates secondaires → booléens → mesures → métadonnées (`*_at`) en dernier. Convention indicative, non lintée. Détail + exemple : [`docs/conventions/marts.md`](docs/conventions/marts.md) § 7.

**Avec le MCP BigQuery** : explorer la source upstream avant d'écrire le mart (`get_table_info` pour schéma, `SELECT DISTINCT` pour `accepted_values`, `COUNT(*)` pour les bornes `row_count_between`, `MIN/MAX(date)` pour les plages).

### Exposures (Power BI reports)

Exposures declare which Power BI reports consume which dbt models. One file per BU dans `models/exposures/` :
- `neshu.yml` · `lcdp.yml` · `finance.yml` · `services_generaux.yml` · `supply_chain.yml`
- `technique.yml`, `commerce.yml` à créer quand des rapports y seront affectés

Update l'exposure correspondante dès qu'un mart est créé/modifié et consommé par un rapport BI. `ref()` pour dbt models, `source()` pour tables externes Cloud Run.

### External marts sources

Tables dans `prod_marts` écrites directement par des Cloud Run jobs (hors dbt). Déclarées comme sources dans `_<bu>__marts_sources.yml` au sein du folder BU :
- `models/marts/neshu/_neshu__marts_sources.yml` — `fct_neshu__monitoring_passage_appro`
- `models/marts/lcdp/_lcdp__marts_sources.yml` — `fct_lcdp__monitoring_passage_appro`

Référencer via `source('marts_<bu>_external', '<table>')`. Ne jamais créer de modèle dbt wrappant ces tables.

---

## BigQuery configuration

### Partitioning
Partition on the **date/timestamp column used as the main filter in Power BI reports**.
- Fact tables → partition on the primary date dimension (e.g. `consumption_date`, `task_start_date`)
- Staging incremental models → partition on the timestamp used for the incremental filter
- Use `data_type: 'date'` for date columns, `data_type: 'timestamp'` for timestamps
- No partition needed on small dimension tables (company, product, etc.)

### Clustering
Cluster on **foreign key columns** used in JOINs or BI filters, up to 4 columns.
- Typical: `cluster_by: ['company_id', 'product_id', 'device_id']`
- For staging incremental: cluster on the FK columns most used in downstream joins

### Incremental strategy
Only `stg_oracle_neshu__task` is incremental today. Standard pattern:
```sql
{{ config(materialized='incremental', unique_key='id', incremental_strategy='merge') }}
...
{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
       or updated_at >= timestamp_sub(current_timestamp(), interval 7 day)
{% endif %}
```

---

## Documentation maintenance

After any model creation, deletion, or convention change, update the relevant docs **in the same work session**.

### What triggers an update

| Change | README.md | CONTRIBUTING.md | CONVENTIONS.md | Autre |
|---|---|---|---|---|
| New model added | — | — | — | — |
| New source added | Add row in Sources table | Add source to "Ajouter une nouvelle source" steps | — | — |
| New BI report / exposure added | — | — | — | Update `models/exposures/<bu>.yml` |
| New naming/column convention | — | — | Update relevant section | — |
| New SQLFluff rule | — | — | Update SQLFluff table | — |
| New materialization pattern | — | Update "Ajouter un nouveau modele" steps | Update Materialisation table | — |
| New mandatory test pattern | — | Update checklist | Update Tests section | Update `docs/conventions/marts.md` § 4 if marts test rule |
| New marts modeling rule | — | — | — | Update `docs/conventions/marts.md` |
| Workflow or PR process change | — | Update relevant section | — | — |
| BigQuery config change (partition/cluster) | — | — | Add/update BigQuery section | — |
| New BU / marts folder | — | — | — | Create `_<bu>__marts_models.yml` + exposure file; marts refacto by BU is DONE (no `docs/migration-marts/`) |

### What to update in each doc

**`README.md`** — high-level overview for anyone discovering the project:
- Sources table: when a new source is added or an existing one changes

**`CONTRIBUTING.md`** — practical workflow guide for the Data Analyst:
- Step-by-step model creation process if the workflow changes
- Checklist before merge if new quality gates are added

**`CONVENTIONS.md`** — now a **minimal global index**. Holds only transversal rules (naming format, columns, materialisation summary, test/severity strategy, SQLFluff, tags) + a router table to the per-layer docs. Layer-specific rules live in `docs/conventions/`, NOT here.

**`docs/conventions/{staging,intermediate,marts,seeds-snapshots}.md`** — the per-layer/-resource convention docs, loaded on demand. Each follows the same skeleton (rôle · nommage · colonnes · pattern SQL · matérialisation · description · tests minimum · freshness · anti-patterns · checklist PR). **Update the relevant layer doc** when a rule for that layer changes — that's the source of truth now:
- `staging.md` — passthrough naming rule, system columns, CTE pattern, incremental, tests, freshness method A/B
- `intermediate.md` — source-aligned (not cross-source), ref-only, incremental, tests
- `marts.md` — naming by BU, star schema, 4-block description trame, config hygiene, tests, anti-patterns, grain-first order
- `seeds-snapshots.md` — CSV seeds (column_types, BigQuery types, BOM), SCD2 snapshots

**`docs/freshness.md`** — source freshness authority: tiers, monitoring mechanisms (A/B), per-source target state. `CONVENTIONS.md § Source freshness` and `staging.md § 8` only point here.

---

## Hard rules

- **Snapshots strategy/columns inchangés** — gérés par GCP Cloud Workflows. **Exception** : mettre à jour les `ref()` à l'intérieur d'un snapshot est OK quand une dim référencée est renommée (cf. PR neshu : `snap_oracle_neshu__company` ref → `dim_neshu__company`). Ne jamais renommer le fichier snapshot ni sa table BQ (historique SCD2 perdu).
- **Never delete or drop tables** unless explicitly asked
- **Never run against prod target** unless explicitly asked
- **Never skip SQLFluff lint** before considering a model done
- **Marts must follow a star schema** — facts (`fct_`) reference dimensions (`dim_`) via `<entity>_id` foreign keys only. No fact-to-fact joins (un fait peut toutefois en **agréger** un autre à un grain plus grossier via `GROUP BY`, ou l'**étendre** à grain strictement identique 1:1 — cf. [`docs/conventions/marts.md`](docs/conventions/marts.md)), no snowflaked dimensions, no wide one-big-table marts. **Aplatir uniquement les attributs d'affichage du parent direct (1-3 colonnes max)**, jamais une dim parente entière. Voir [`docs/conventions/marts.md`](docs/conventions/marts.md) § Marts — pattern complet.
- **Description placement** : staging **doit** avoir `description='...'` dans `{{ config() }}` (cf. feedback memory, convention historique). Intermediate et **marts** : description en YAML uniquement, pas dans le config block (persist_docs gère BQ).
- All contributions go through PRs — DE owns staging/intermediate/snapshots, DA contributes/reviews marts.
  **Exception** : les changements docs-only (README, docs/, CONTRIBUTING) partent en push direct sur master, pas de PR. Master est protégée : le push direct passe avec les droits owner (sinon fallback PR + merge --admin).

---

## Key packages

- `dbt_utils` 1.3.3 — `unique_combination_of_columns`, `expression_is_true`, `generate_surrogate_key`
- `dbt_expectations` 0.10.10 — row count ranges, date ranges, regex, null rate checks

## SQLFluff rules (v4)

- Keywords, functions, types: **lowercase**
- Indent: **4 spaces**
- Max line length: **120 characters**
- **No trailing commas**
- Templater: `dbt` (requires env vars) or `jinja` as fallback
