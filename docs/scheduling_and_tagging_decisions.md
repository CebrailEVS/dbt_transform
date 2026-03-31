# Scheduling & Tagging Decisions
> Internal doc — architecture decisions made during migration planning. Last updated: 2026-03-30.

---

## Context

This document captures the decisions made around dbt tag conventions and pipeline scheduling,
specifically for cross-source models. It is a companion to `architecture_review.md`.

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
| `cross_post_nesp_co` | Set based on observed nesp_co run time + buffer | nesp_co (starts 08:00 daily) | models needing nesp_co or gac |

**Naming rule:** `cross_post_<last_source>` — tells you exactly which pipeline gate
the model is waiting for without being tied to a hardcoded time.

**When adding a new cross-source model:**
1. Identify which source finishes last among its upstreams
2. Use the corresponding `cross_post_<source>` tag in the model config
3. If no tag exists yet for that boundary → create a new workflow + Cloud Scheduler job

---

## 5. Dedicated Cross-Source Workflow (Option B — chosen)

### Why not append to an existing pipeline (Option A)?

Appending `tag:cross_post_yuman` to the end of `pipeline-yuman` would work today but:
- Couples cross-source logic to the yuman pipeline — a yuman failure blocks cross-source builds
- The yuman workflow accumulates responsibilities that don't belong to it
- Less self-documenting

### Dedicated workflow structure

One workflow file + one Cloud Scheduler job per scheduling boundary.

**`/workflows/pipeline-cross-source-yuman.yaml`** — fires after yuman is guaranteed done:

```yaml
# Schedule: weekdays, set time based on observed yuman run time + buffer
# cron: 0 <H> * * 1-5

main:
  steps:
    - init:
        assign:
          - project: "evs-datastack-prod"
          - region: "europe-west1"

    - run_dbt:
        call: run_cloud_run_job
        args:
          project: ${project}
          region: ${region}
          job_name: "dbt-runner"
          args_override: []
          env_override:
            - name: "DBT_SOURCE_SELECTOR"
              value: "source:oracle_neshu source:yuman_api"
            - name: "DBT_TAG_SELECTOR"
              value: "tag:cross_post_yuman"
        result: res_dbt

    - log_dbt:
        call: sys.log
        args:
          text: "dbt cross_post_yuman completed"
          severity: INFO

    - pipeline_done:
        return: "Pipeline cross-source-yuman completed successfully"

# + run_cloud_run_job and poll_lro subworkflows (copy from any existing pipeline)
```

**Note on `DBT_SOURCE_SELECTOR`:** `entrypoint.sh` always runs `dbt source freshness`
before building. For cross-source workflows there is no EL step, so pass all upstream
source names as a space-separated string. dbt treats this as a union selector.

### Terraform additions

```hcl
resource "google_workflows_workflow" "pipeline_cross_source_yuman" {
  name            = "pipeline-cross-source-yuman"
  region          = var.region
  service_account = google_service_account.meltano_runner.email
  source_contents = file("${path.module}/../workflows/pipeline-cross-source-yuman.yaml")
}

resource "google_cloud_scheduler_job" "pipeline_cross_source_yuman" {
  name      = "pipeline-cross-source-yuman"
  schedule  = "0 <H> * * 1-5"   # set based on observed yuman run time + buffer
  time_zone = "Europe/Paris"

  http_target {
    uri         = "https://workflowexecutions.googleapis.com/v1/${google_workflows_workflow.pipeline_cross_source_yuman.id}/executions"
    http_method = "POST"
    body        = base64encode("{}")
    oauth_token {
      service_account_email = google_service_account.meltano_runner.email
    }
  }
}
```

---

## 6. Current Cross-Source Models

### `fct_technique__machines_maintenance_tracking`

| Property | Value |
|---|---|
| Current location | `models/marts/oracle_neshu/` |
| Proposed location | `models/marts/technique/` |
| Sources | oracle_neshu (daily 01:00) + yuman (weekdays 01:00) |
| Scheduling tag | `cross_post_yuman` |
| Reason for move | Cross-source model, belongs in `technique/` BU folder |
| Uses `current_timestamp()` | Yes — must rebuild daily, weekly is not acceptable |

**Migration steps for this model:**
1. Absorb `int_oracle_neshu__machines_yuman_maintenance_base` logic directly into this mart
   (the intermediate has only one consumer — this model)
2. Move file to `models/marts/technique/`
3. Rename to `fct_technique__machines_maintenance_tracking` (name unchanged, only folder moves)
4. Add `tags = ['cross_post_yuman']` to model config
5. Delete `int_oracle_neshu__machines_yuman_maintenance_base`
6. Update `_oracle_neshu__marts_models.yml` → remove entry
7. Update `_technique__marts_models.yml` → add entry
8. Update `_oracle_neshu__intermediate_models.yml` → remove intermediate entry
9. Deploy `pipeline-cross-source-yuman` workflow + Cloud Scheduler

### `fct_nesp_co__machines_avec_interventions`

| Property | Value |
|---|---|
| Current location | `models/marts/nesp_co/` |
| Proposed location | `models/marts/technique/` |
| Proposed name | `fct_technique__machines_avec_interventions` |
| Sources | nesp_tech (Monday 07:30) + nesp_co (daily 08:00) |
| Scheduling tag | none — inherits `technique` from folder |
| Triggered by | `pipeline-nesp-tech` already runs `tag:technique` after nesp_tech refresh |
| Refresh cadence | Weekly (Monday only) — acceptable, intervention data is weekly by nature |

**Migration steps for this model:**
1. Move file to `models/marts/technique/`
2. Rename to `fct_technique__machines_avec_interventions`
3. No model-level tag needed — folder tag `technique` is correct
4. Update `_nesp_co__marts_models.yml` → remove entry
5. Update `_technique__marts_models.yml` → add entry
6. Verify Power BI reports: if they reference the BigQuery table by name, update the dataset reference
   (table name changes because the model is renamed)

---

## 7. Scaling Rules — Adding Future Cross-Source Models

1. **Identify the last upstream source to finish** among the model's dependencies
2. **Use the existing `cross_post_<source>` tag** if one already exists for that boundary
3. **If no tag exists yet:** create a new workflow YAML + Terraform Cloud Scheduler block
4. **Do not hardcode model names in workflows** — always use `tag:cross_post_<source>`
5. **Update `DBT_SOURCE_SELECTOR`** in the workflow to include all relevant upstream sources

---

## 8. What Big Companies Do (for context — not applicable here)

Large companies use orchestrators (Airflow, Dagster, Prefect) where cross-source dependencies
are handled natively via task DAGs. `depends_on: [oracle_neshu_pipeline, yuman_pipeline]`
replaces all the tag/schedule engineering above.

dbt Cloud (SaaS) adds job chaining natively but requires managed ingestion (Fivetran, Airbyte)
to fully replace an orchestrator. Custom ingestion code (Meltano + Cloud Run) still needs
a scheduler.

The `cross_post_<source>` pattern is the pragmatic equivalent for a time-based scheduler setup:
it approximates event-driven orchestration with time buffers, at near-zero infrastructure cost.
