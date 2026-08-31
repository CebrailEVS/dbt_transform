# CLAUDE.md — dbt_warehouse (EVS Professionnelle France)

## Project overview
ELT data warehouse for EVS Professionnelle France.
**Stack:** Meltano + Cloud Run jobs (extract) → BigQuery `prod_raw` (lake) → dbt (transform) → GCP Cloud Workflows (orchestrate) → Power BI (viz)
**dbt version:** 1.12.3 / dbt-bigquery 1.12.0
**Team:** 1 Data Engineer (owner), 1 Data Analyst (contributes to marts)

---

## Local dev setup

Required env vars (set in `.env`):
```
# prod target (Cloud Run runtime)
DBT_BIGQUERY_PROJECT=evs-datastack-prod
DBT_BIGQUERY_KEYFILE=/path/to/prod-keyfile.json
DBT_BIGQUERY_DATASET_PROD=prod_    # prefix — prod_staging, prod_intermediate, prod_marts

# dev target (default) — isolated GCP project since PR #165
DBT_BIGQUERY_PROJECT_DEV=evs-datastack-dev
DBT_BIGQUERY_KEYFILE_DEV=/path/to/dbt-dev-keyfile.json   # SA dbt-dev (dev write + prod_raw read-only)
DBT_BIGQUERY_DATASET_DEV=dev_      # prefix — dev_staging, dev_intermediate, dev_marts
DBT_TARGET=dev                     # defaults to dev if not set
```

Default target is `dev`. **Dev builds write to a dedicated GCP project `evs-datastack-dev`**
(not prod), authenticated with the `dbt-dev` service account. Sources are still read
cross-project from `evs-datastack-prod` `prod_raw` via `var('raw_project')` — no pipeline
duplication. Dev snapshots read from prod (PR #166).
Never run against `prod` target unless explicitly asked.

---

## CI/CD (GitHub Actions — `.github/workflows/dbt-ci.yml`)

One workflow, path-filtered on `models/**`, `data/**`, `snapshots/**`, `macros/**`, `tests/**`,
`dbt_project.yml`, `profiles.yml`, `packages.yml`, `selectors.yml`, `Dockerfile`, `entrypoint.sh`,
`requirements*.txt`, `.sqlfluff`. Three jobs:

**`pr-check`** — runs on `pull_request` → master (model paths only; `models/exposures/**` excluded):
- `dbt deps` + `dbt debug --target dev`
- SQLFluff lint on **changed** models only (git diff vs base ref)
- `dbt parse --target dev` (warnings surfaced, non-blocking)
- `dbt parse --use-v2-parser --target dev` — **veille dbt Core v2** (moteur Fusion/Rust),
  `continue-on-error: true`. Le binaire v2 est livré avec dbt-core 1.12 (dep
  `dbt-core-experimental-parser`), rien à installer. Ne bloque jamais la PR.
- Pulls prod `manifest.json` from the GCS state bucket, then a **deferred incremental** build:
  `dbt build --target dev --select state:modified+ --defer --state state/ --exclude resource_type:snapshot`
  → builds only modified+downstream in `evs-datastack-dev`; unbuilt refs & snapshots **defer to prod**.
  (No manifest → full `dbt build --target dev`, snapshots included.)

**`cd`** — runs on `push` → master (**including direct pushes that bypass the PR rule**):
- `dbt deps` + `dbt debug --target prod`
- Pulls prod manifest, then **state-based incremental** build **directly in prod**:
  `dbt build --target prod --select state:modified+ --exclude resource_type:snapshot --state state/`
  (No manifest → full build, snapshots excluded.)
- `dbt docs generate --target prod`, then **uploads `manifest.json` back to GCS** (state for next run)
- Builds & pushes `dbt-runner:latest` to Artifact Registry
  (`europe-west1-docker.pkg.dev/evs-datastack-prod/data-pipelines/dbt-runner`)

**`deploy-docs`** — `needs: cd`: deploys the generated dbt docs to GitHub Pages.

**Key facts (don't forget these):**
- Snapshots are **always excluded** from CI/CD builds — owned by Cloud Workflows only.
- Builds are **state-based incremental** (`state:modified+`) against the GCS manifest, **not** full rebuilds.
- A **direct push to master triggers `cd`** → the change builds in prod immediately, not only on PR merge.
  The rebuilt `dbt-runner` image is then picked up **per-execution** by scheduled Cloud Workflows runs.
- CI/CD (immediate build on push/PR) is **distinct from Cloud Workflows** (scheduled EL + transform orchestration).
- **dbt Core v2 (moteur Fusion) n'est PAS en prod** : l'adaptateur BigQuery y est en *Preview*
  (Snowflake seul est GA). On reste sur la ligne 1.x tant que BigQuery n'est pas GA. Le step de
  veille ci-dessus est là pour voir venir une incompatibilité, pas pour préparer une bascule.
  Au moment de basculer : `dbt docs generate` est déprécié en v2 (→ `dbt compile --write-catalog`),
  et le lint SQLFluff n'a pas encore d'équivalent en CI Fusion (`dbt lint` natif).

---

## Architecture — 3 layers

| Layer | Schema | Materialization |
|---|---|---|
| `models/staging/` | `prod_staging` / `dev_staging` | table (one model = `incremental`) |
| `models/intermediate/` | `prod_intermediate` / `dev_intermediate` | table |
| `models/marts/` | `prod_marts` / `dev_marts` | table |

**10 sources** in `prod_raw`: `oracle_neshu`, `oracle_lcdp`, `yuman`, `nesp_tech`, `nesp_co`, `mssql_sage`, `gac`, `yuman_evs_sftp`, `oracle_neshu_gcs`, `oracle_lcdp_gcs`

Seeds are in `data/reference_data/<source>/` and land in `prod_reference` / `dev_reference`.

---

## Naming conventions

- **Staging / intermediate** : `<prefix>_<source>__<entity>.sql` (par source)
- **Marts** : `<prefix>_<bu>__<entity>.sql` (par BU/domaine, **post-refacto by BU**)
  - BUs : `neshu`, `lcdp`, `technique`, `commerce`, `finance`, `services_generaux`, `supply_chain`
  - Entité **singulier**, snake_case, nom métier (pas le nom source, pas le nom du rapport BI)
- Prefixes: `stg_` staging · `int_` intermediate · `dim_` dimension · `fct_` fact · `snap_` snapshot
- YAML files: `_<source>__models.yml` (staging/intermediate) · `_<bu>__marts_models.yml` (marts) · `_<bu>__marts_sources.yml` (external Cloud Run tables) · `_<source>__seeds.yml` (seeds, one per source, co-located in `data/reference_data/<source>/`)
- Seeds: `ref_<source>__<entity>.csv` (no monolithic `data/schema.yml` — doc lives in the per-source `_<source>__seeds.yml`)
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

Update l'exposure correspondante dès qu'un mart est créé/modifié et consommé par un rapport BI. `ref()` pour dbt models.

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

## Frontières avec `ingestion/` et `infra/`

Les sources de ce repo sont produites par les pipelines dlt d'`ingestion/`, et
orchestrées par `infra/`. Deux points ont déjà coûté un incident.

**Un changement de type dans `prod_raw` peut casser un modèle INCRÉMENTAL, et
`--full-refresh` ne le révèle pas.** Il reconstruit la table, donc il n'exerce pas le
chemin que le job `cd` emprunte chaque nuit. Une colonne qui traverse un modèle
incrémental **sans cast** hérite du type du raw : si celui-ci change, le `MERGE`
échoue avec `Value of type X cannot be assigned to <col>, which has type Y`.
**Incident 2026-07-30** : le passage des `NUMBER` Oracle de `STRING` à `FLOAT64` a
cassé `stg_oracle_lcdp__task` sur la seule colonne `spantime`, passée sans cast. Le
build en `--full-refresh` était vert, l'incrémental non.

Parade : après toute évolution de type au raw, faire un build **sans**
`--full-refresh`, et caster explicitement toute colonne passée telle quelle — le
modèle devient indépendant du type de la source.

**Le raw est délibérément fidèle à la source.** `ingestion/` ne fait aucun typage
métier : les `NUMBER` Oracle sans précision atterrissent en `FLOAT64`, les colonnes de
clé en `NUMERIC`, les types inconnus en `STRING`. **Tous les casts se font ici.** Ne
pas demander de changement de type côté extraction pour éviter un cast dbt.

**Retirer un workflow dans `infra/` peut rendre un selector mort.** Les selectors de
`selectors.yml` sont appelés par les workflows Cloud Workflows. Quand un workflow
disparaît, vérifier si son selector a encore un appelant.

## Délégation aux subagents

Règle : **ce qui lit beaucoup pour ne conclure qu'un peu part en subagent.** La
session principale garde la décision, pas les 40 fichiers qui y mènent. Un
subagent a sa propre fenêtre de contexte et ne rend qu'un rapport.

| Situation | Délégation |
|---|---|
| « comprends comment marche X », recherche large, convention à retrouver | agent `Explore` (intégré) |
| un mart est écrit et semble fini | agent `mart-reviewer` — **avant** de le déclarer terminé |
| le changement touche un type de colonne, un nom de source, un selector, un tag | agent `boundary-impact` (global) |
| audit lourd sur BigQuery (`audit-docs`, `audit-sources`, `check-staging-relationships`) | lancer la **skill dans un subagent**, pas dans la session principale |
| review de diff générique, pré-PR | `/code-review` (intégré) |

**À ne pas déléguer** : l'écriture des modèles de staging (le DE les écrit
lui-même), les décisions d'architecture, les arbitrages métier.

**Modèle — doctrine** : le palier suit `enjeu irréversible × jugement ÷ fréquence`.
Les agents-gardes (`mart-reviewer`, `boundary-impact`, `tf-plan-reviewer`,
`pipeline-reviewer`) sont en **`opus` + `effort: high`** : ils tournent quelques
fois par semaine sur un diff borné, ils sont le **seul** relecteur, et ce qu'ils
laissent passer arrive en prod. Le gain d'un palier inférieur y serait
négligeable, le coût d'un défaut manqué non.
`sonnet` est réservé au travail de **volume** : audit d'une BU entière,
scaffolding, migration en fan-out — beaucoup de lignes, peu de jugement.

**Hygiène de contexte** : deux corrections ratées sur le même point → `/clear`
et reprompt plus précis, plutôt que continuer dans un contexte pollué.

## Hard rules

- **Snapshots strategy/columns inchangés** — gérés par GCP Cloud Workflows. **Exception** : mettre à jour les `ref()` à l'intérieur d'un snapshot est OK quand une dim référencée est renommée (cf. PR neshu : `snap_oracle_neshu__company` ref → `dim_neshu__company`). Ne jamais renommer le fichier snapshot ni sa table BQ (historique SCD2 perdu).
- **Never delete or drop tables** unless explicitly asked
- **Never run against prod target** unless explicitly asked
- **Never `git push` or create a PR without explicit user confirmation, every time** — local commits on a feature branch are fine, but anything that leaves the machine (push, `gh pr create`, direct push to master) waits for an explicit GO in the current exchange. A confirmation given earlier in the session does NOT carry over to the next push/PR.
- **Toujours créer une branche de feature depuis `master` à jour**, jamais depuis la branche courante :
  `git fetch origin master && git checkout -b feature/<nom> origin/master`.
  Ne jamais faire un `git checkout -b` sans avoir vérifié où pointe `HEAD` : partir d'une
  autre branche de feature embarque ses commits non poussés dans la PR. **Incident 2026-07-29** :
  la branche roadman LCDP a été créée depuis `feature/apptech-interventions-retraitees` → la PR
  aurait inclus le mart `fct_technique__intervention_retraitee` d'un autre chantier (5 fichiers,
  ~400 lignes), et vidé sa propre PR de son contenu.
  **Contrôle avant tout push** : `git log --oneline origin/master..HEAD` ne doit contenir que les
  commits du chantier en cours, et `git diff --stat origin/master...HEAD` que ses fichiers.
  Correction si la branche est déjà polluée : nouvelle branche depuis `origin/master` +
  `git cherry-pick` des seuls commits du chantier (non destructif, l'autre branche reste intacte).
- **Never skip SQLFluff lint** before considering a model done
- **Marts must follow a star schema** — facts (`fct_`) reference dimensions (`dim_`) via `<entity>_id` foreign keys only. No fact-to-fact joins (un fait peut toutefois en **agréger** un autre à un grain plus grossier via `GROUP BY`, ou l'**étendre** à grain strictement identique 1:1 — cf. [`docs/conventions/marts.md`](docs/conventions/marts.md)), no snowflaked dimensions, no wide one-big-table marts. **Aplatir uniquement les attributs d'affichage du parent direct (1-3 colonnes max)**, jamais une dim parente entière. Voir [`docs/conventions/marts.md`](docs/conventions/marts.md) § Marts — pattern complet.
- **Description placement** : staging **doit** avoir `description='...'` dans `{{ config() }}` (cf. feedback memory, convention historique). Intermediate et **marts** : description en YAML uniquement, pas dans le config block (persist_docs gère BQ).
- All contributions go through PRs — DE owns staging/intermediate/snapshots, DA contributes/reviews marts.
  **Exception** : les changements docs-only (README, docs/, CONTRIBUTING) partent en push direct sur master, pas de PR. Master est protégée : le push direct passe avec les droits owner (sinon fallback PR + merge --admin).

---

## Key packages

- `dbt_utils` 1.4.1 — `unique_combination_of_columns`, `expression_is_true`, `generate_surrogate_key`
- `dbt_expectations` 0.10.10 — row count ranges, date ranges, regex, null rate checks

## SQLFluff rules (v4)

- Keywords, functions, types: **lowercase**
- Indent: **4 spaces**
- Max line length: **120 characters**
- **No trailing commas**
- Templater: `dbt` (requires env vars) or `jinja` as fallback
