# Scheduling & Tagging Decisions
> Internal doc — architecture decisions made during migration planning. Last updated: 2026-03-31.
>
> **Decision log:** `cross_post_*` tag pattern was considered and dropped. Replaced by a dedicated
> T-only workflow (`transform-technique-daily`) with dbt source freshness as a safety gate.
> See section 7.

---

## Context

This document captures the decisions made around dbt tag conventions and pipeline scheduling,
specifically for cross-source models. It is a companion to `architecture_review.md`.

It also serves as the reference inventory for the BU-based marts restructuration
(`feature/refactor-marts-by-bu`).

---

## 1. Tag Conventions

### Two distinct purposes for tags — never conflate them

| Purpose | Where defined | Example |
|---|---|---|
| Business domain / discoverability | `dbt_project.yml` (folder-level) | `tag:technique`, `tag:oracle_neshu` |
| Scheduling ownership | Model config or YAML (model-level, exceptions only) | `tags = ['cross_post_yuman']` |

**Rule:** `dbt_project.yml` defines tags for entire folders. Model-level tags in SQL config
or YAML are reserved for exceptions — models that don't follow the folder's default schedule.

### Folder-level tags (dbt_project.yml)

Assigned to all models in a folder. Drive the standard pipeline trigger.

```yaml
# dbt_project.yml
models:
  dbt_warehouse:
    marts:
      technique:
        +tags: ['marts', 'technique']
      oracle_neshu:
        +tags: ['marts', 'oracle_neshu']
      # etc.
```

### Model-level tags (exceptions only)

Used when a model lives in a BU folder but its scheduling is owned by a different pipeline.
Only cross-source models need this.

```sql
{{ config(
    materialized = 'table',
    tags = ['cross_post_yuman'],   -- scheduling owner: explicit exception
    description = "..."
) }}
```

Or in the model YAML:
```yaml
models:
  - name: fct_technique__machines_maintenance_tracking
    config:
      tags: ['cross_post_yuman']
```

**Do NOT add `cross_post_*` tags to `dbt_project.yml`** — they are model-level exceptions,
not folder properties.

---

## 2. Single-Source Models — Standard Pattern

No special handling needed. Folder tag = scheduling tag.

```
pipeline-oracle-neshu (01:00 daily)
  └─ dbt build --select tag:oracle_neshu
       └─ builds all models in marts/oracle_neshu/, intermediate/oracle_neshu/, staging/oracle_neshu/
```

Everything in the folder is built together. No model-level overrides.

---

## 3. Cross-Source Models — The Problem

When a model reads from two sources with different pipeline schedules, tagging it with
one source creates a race condition or a freshness problem:

- Tag it `oracle_neshu` → builds before yuman pipeline finishes → stale yuman data
- Tag it `yuman` → yuman pipeline and oracle_neshu pipeline both run at 01:00, double rebuild
- Tag it both → double rebuild on weekdays, race condition if pipelines start simultaneously

**Solution: dedicated cross-source workflow + `cross_post_<source>` tag.**

---

## 4. Cross-Source Tag Convention

One tag per **scheduling boundary** — defined by the last upstream source to finish.

| Tag | Trigger time | Gate: last source to finish | Sources it covers |
|---|---|---|---|
| `cross_post_yuman` | Set based on observed yuman run time + buffer | yuman (starts 01:00 weekdays) | oracle_neshu + yuman |
| `cross_post_nesp_co` | Set based on observed nesp_co run time + buffer | nesp_co (starts 08:00 daily) | models needing nesp_co or nesp_tech |

**Naming rule:** `cross_post_<last_source>` — tells you exactly which pipeline gate
the model is waiting for without being tied to a hardcoded time.

**When adding a new cross-source model:**
1. Identify which source finishes last among its upstreams
2. Use the corresponding `cross_post_<source>` tag in the model config
3. If no tag exists yet for that boundary → create a new workflow + Cloud Scheduler job

---

## 5. Full Cross-Source Model Inventory

As of 2026-03-31. 7 cross-source models across all layers.

| Model | Layer | Current location | Sources | Scheduling risk | Tag needed |
|---|---|---|---|---|---|
| `int_oracle_neshu__machines_yuman_maintenance_base` | intermediate | `intermediate/oracle_neshu/` | oracle_neshu + yuman | High | `cross_post_yuman` |
| `fct_oracle_neshu__machines_maintenance_tracking` | marts | `marts/oracle_neshu/` | oracle_neshu + yuman | High | `cross_post_yuman` |
| `fct_nesp_co__machines_avec_interventions` | marts | `marts/nesp_co/` | nesp_co + nesp_tech | Medium | `cross_post_nesp_co` |
| `fct_technique__interventions` | marts | `marts/technique/` ✅ | nesp_tech + yuman | Medium | TBD — confirm which finishes last |
| `fct_oracle_neshu__supply_flux` | marts | `marts/oracle_neshu/` | oracle_neshu + oracle_neshu_gcs | Low | Likely none — confirm same pipeline window |
| `fct_oracle_neshu_gcs__stock_products` | marts | `marts/oracle_neshu_gcs/` | oracle_neshu + oracle_neshu_gcs | Low | Likely none — confirm same pipeline window |
| `int_mssql_sage__pnl_bu` | intermediate | `intermediate/mssql_sage/` | mssql_sage + historic (static) | Low | None — historic source is not a pipeline |

### Notes on low-risk models

**`oracle_neshu` + `oracle_neshu_gcs`** — both originate from the same Oracle system, extracted
via different paths (DB connector vs GCS CSV dump). If they run in the same Cloud Scheduler
window, no dedicated cross-source workflow is needed. Confirm before migration.

**`int_mssql_sage__pnl_bu`** — `source('historic', 'update_mssql_sage__analytique_2024')` is a
one-time historical mapping table, not a live pipeline. No scheduling concern.

### Known gap (pre-migration bug)

`marts/nesp_co/` has no entry under `marts:` in `dbt_project.yml`. `fct_nesp_co__machines_avec_interventions`
only receives `tag:marts`, never `tag:nesp_co`. It is invisible to any nesp_co pipeline selector today.
Fix this as part of the migration.

---

## 6. Impact of BU Migration on Workflows

### dbt_project.yml

Staging and intermediate layers are **unchanged** — they stay source-tagged.
Only the `marts/` folder structure changes (source folders → BU folders).

Before:
```yaml
marts:
  oracle_neshu:
    +tags: ['marts', 'oracle_neshu']
```

After:
```yaml
marts:
  technique:
    +tags: ['marts', 'technique']
  # etc. — BU folder names replace source folder names
```

### Cloud Workflows / Cloud Scheduler selectors

Current pipelines presumably select by source tag across all layers:
```
dbt build --select tag:oracle_neshu
# today: hits staging/oracle_neshu + intermediate/oracle_neshu + marts/oracle_neshu
```

After BU migration `tag:oracle_neshu` no longer selects the oracle_neshu marts (they moved
to a BU folder with a different tag). Each workflow selector must be updated to union the
source tag with the relevant BU mart tag:

```
# updated selector in pipeline-oracle-neshu workflow
dbt build --select tag:oracle_neshu tag:neshu
```

Or split into two steps: source/intermediate build first, then BU mart build.

**Update every workflow's `DBT_TAG_SELECTOR` environment variable as part of the migration.**

---

## 7. Chosen Solution: `transform-technique-daily`

### Naming convention: `transform-*` vs `pipeline-*`

- `pipeline-*` — EL + T workflows (Meltano extract + dbt build)
- `transform-*` — T-only workflows (dbt only, no extraction step)

### Why a dedicated workflow instead of appending to yuman or oracle_neshu

Both `pipeline-yuman` and `pipeline-oracle-neshu` start at 01:00. oracle_neshu finishes
first (~03:00), yuman finishes last (~04:00-05:00 by experience). Appending a technique
build step to oracle_neshu would race with yuman. Appending to yuman would mean a yuman
pipeline failure also breaks technique mart builds — wrong failure domain.

A dedicated T-only workflow at 03:00 with a source freshness gate is self-contained,
self-documenting, and fails independently.

### Why `tag:technique` and not `cross_post_yuman`

All technique models live in `marts/technique/` and share the same `tag:technique` folder tag.
Using `tag:technique` directly is simpler — no model-level tag overrides needed.

nesp_tech-dependent models (`fct_technique__interventions`, `fct_technique__machines_avec_interventions`)
rebuild daily with the previous Monday's nesp_tech data. This is intentional and acceptable:
yuman and nesp_co data still refresh daily. The Monday nesp_tech pipeline rebuilds them again
with fresh nesp_tech data. Idempotent, no data integrity issue.

### Workflow: `transform-technique-daily`

File: `/workflows/transform-technique-daily.yaml`
Schedule: daily at 03:00 Europe/Paris (`0 3 * * *`)

```
Step 1: dbt source freshness → source:oracle_neshu source:yuman_api
        fails fast if either upstream pipeline failed overnight

Step 2: dbt build → tag:technique
        builds all models in marts/technique/
```

### Terraform (in `infra/workflows.tf`)

```hcl
resource "google_workflows_workflow" "transform_technique_daily" {
  name            = "transform-technique-daily"
  region          = var.region
  service_account = google_service_account.meltano_runner.email
  source_contents = file("${path.module}/../workflows/transform-technique-daily.yaml")
}

resource "google_cloud_scheduler_job" "transform_technique_daily" {
  name      = "transform-technique-daily"
  schedule  = "0 3 * * *"
  time_zone = "Europe/Paris"

  http_target {
    uri         = "https://workflowexecutions.googleapis.com/v1/${google_workflows_workflow.transform_technique_daily.id}/executions"
    http_method = "POST"
    body        = base64encode("{}")
    oauth_token {
      service_account_email = google_service_account.meltano_runner.email
    }
  }
}
```

---

## 8. Migration Steps per Model

### `fct_oracle_neshu__machines_maintenance_tracking` (+ its intermediate)

1. Absorb `int_oracle_neshu__machines_yuman_maintenance_base` logic directly into the mart
   (single consumer — no reason to keep the intermediate)
2. Move file to `models/marts/technique/`
3. Rename to `fct_technique__machines_maintenance_tracking`
4. Add `tags = ['cross_post_yuman']` to model config
5. Add `tags = ['cross_post_yuman']` to `int_oracle_neshu__machines_yuman_maintenance_base` (until absorbed)
6. Delete `int_oracle_neshu__machines_yuman_maintenance_base`
7. Update `_oracle_neshu__marts_models.yml` → remove entry
8. Update `_technique__marts_models.yml` → add entry
9. Update `_oracle_neshu__intermediate_models.yml` → remove intermediate entry
10. Deploy `pipeline-cross-source-yuman` workflow + Cloud Scheduler

### `fct_nesp_co__machines_avec_interventions`

1. Move file to `models/marts/technique/`
2. Rename to `fct_technique__machines_avec_interventions`
3. Add `tags = ['cross_post_nesp_co']` to model config
4. Update `_nesp_co__marts_models.yml` → remove entry
5. Update `_technique__marts_models.yml` → add entry
6. Fix `dbt_project.yml` — add missing `nesp_co:` entry under `marts:` (or remove folder entirely after move)
7. Verify Power BI reports: table name changes → update BigQuery dataset reference in Power BI
8. Deploy `pipeline-cross-source-nesp-co` workflow + Cloud Scheduler

### `fct_technique__interventions` (already in `marts/technique/`)

1. Confirm which source finishes last (nesp_tech vs yuman) to pick the right `cross_post_*` tag
2. Add `tags = ['cross_post_<last_source>']` to model config
3. No file move needed

### `fct_oracle_neshu__supply_flux` and `fct_oracle_neshu_gcs__stock_products`

1. Confirm oracle_neshu and oracle_neshu_gcs run in the same pipeline window
2. If yes → no scheduling change needed, migration is folder/tag rename only
3. If no → treat as high-risk cross-source and assign a `cross_post_oracle_neshu_gcs` tag

---

## 9. Scaling Rules — Adding Future Cross-Source Models

1. **Identify the last upstream source to finish** among the model's dependencies
2. **Use the existing `cross_post_<source>` tag** if one already exists for that boundary
3. **If no tag exists yet:** create a new workflow YAML + Terraform Cloud Scheduler block
4. **Do not hardcode model names in workflows** — always use `tag:cross_post_<source>`
5. **Update `DBT_SOURCE_SELECTOR`** in the workflow to include all relevant upstream sources

---

## 10. What Big Companies Do (for context — not applicable here)

Large companies use orchestrators (Airflow, Dagster, Prefect) where cross-source dependencies
are handled natively via task DAGs. `depends_on: [oracle_neshu_pipeline, yuman_pipeline]`
replaces all the tag/schedule engineering above.

dbt Cloud (SaaS) adds job chaining natively but requires managed ingestion (Fivetran, Airbyte)
to fully replace an orchestrator. Custom ingestion code (Meltano + Cloud Run) still needs
a scheduler.

The `cross_post_<source>` pattern is the pragmatic equivalent for a time-based scheduler setup:
it approximates event-driven orchestration with time buffers, at near-zero infrastructure cost.
