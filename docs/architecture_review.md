# dbt_warehouse — Architecture Review & Migration Plan

> Internal doc — for team discussion. Last updated: 2026-03-26.

---

## 1. Current State Overview

### Sources & Pipelines

| Source | Pipeline Workflow | Cloud Scheduler | Frequency | dbt tag triggered |
|---|---|---|---|---|
| oracle_neshu | pipeline-oracle-neshu | `0 1 * * *` | Daily 01:00 Paris | `tag:oracle_neshu` |
| oracle_lcdp | pipeline-oracle-lcdp | `0 1 * * *` | Daily 01:00 Paris | `tag:oracle_lcdp` |
| mssql_sage | pipeline-mssql-sage | `0 1 * * *` | Daily 01:00 Paris | `tag:mssql_sage` |
| yuman | pipeline-yuman | `0 1 * * 1-5` | Weekdays 01:00 Paris | `tag:yuman` |
| yuman_gcs (SFTP) | pipeline-sftp-gcs-yuman | `0 6 * * 1-6` | Mon–Sat 06:00 Paris | `tag:yuman_gcs` |
| nesp_tech | pipeline-nesp-tech | `30 7 * * 1` | **Monday only 07:30** | `tag:nesp_tech` then `tag:technique` |
| nesp_co | pipeline-sftp-nesp-client | `0 8 * * *` | Daily 08:00 Paris | `tag:nesp_co` |
| oracle_neshu_gcs | pipeline-oracle-stock-theorique | `0 23 * * *` | Daily 23:00 Paris | `tag:oracle_neshu_gcs` |
| gac | pipeline-sftp-evs | `0 8 * * *` | Daily 08:00 Paris | `tag:gac` |
| passages_appro_neshu | (dedicated workflow) | `0 7-13,15 * * 1-5` | Weekdays 8×/day | `tag:oracle_neshu` (subset) |
| passages_appro_lcdp | (dedicated workflow) | `0 7,8,9,11,13,15,17,18 * * 1-5` | Weekdays 8×/day | `tag:oracle_lcdp` (subset) |

### dbt Project Layer Structure

```
models/
├── staging/        # 1:1 with raw source tables — type casting, null handling, dedup
│   └── {source}/   # stg_{source}__{entity}.sql
│
├── intermediate/   # Business logic, joins within a source domain
│   └── {source}/   # int_{source}__{process}.sql
│
└── marts/          # BI-ready dims and facts
    └── {source}/   # fct_{source}__{event}.sql / dim_{source}__{entity}.sql
```

### Data Freshness Configuration

| Source | freshness warn | freshness error | loaded_at field |
|---|---|---|---|
| oracle_neshu | 26h | 36h | `_sdc_extracted_at` |
| oracle_lcdp | 26h | 36h | `_sdc_extracted_at` |
| mssql_sage | 26h | 36h | `_sdc_received_at` |
| yuman_api | 26h | 36h | `_sdc_extracted_at` |
| nesp_tech | not configured | not configured | — |
| nesp_co | not configured | not configured | — |
| gac | disabled (null) | disabled (null) | — |
| yuman_gcs | not configured | not configured | — |
| oracle_neshu_gcs | not configured | not configured | — |

---

## 2. Problems Currently in Production

### Problem 1 — `fct_nesp_co__machines_avec_interventions` (Scheduling bug) 🔴

**Location:** `models/marts/nesp_co/fct_nesp_co__machines_avec_interventions.sql`
**Tag:** `marts:nesp_co`

**What this model does:**
Builds a list of Nespresso machines with their last 12-month intervention history,
joining nesp_tech interventions with nesp_co client enrichment.

**Cross-source dependencies:**
- `int_nesp_tech__interventions_dedup` ← **nesp_tech** source (weekly, Monday only)
- `int_nesp_co__clients_enrichis` ← nesp_co source (daily)

**The bug:**
The `nesp_co` pipeline runs every day at 08:00 and triggers `dbt build --select tag:nesp_co`.
This rebuilds `fct_nesp_co__machines_avec_interventions` daily — but the nesp_tech
upstream (`int_nesp_tech__interventions_dedup`) is only refreshed on Mondays at 07:30.

From Tuesday to Sunday, this mart is silently rebuilt using intervention data that is
**up to 6 days stale**. No error is raised. No warning is logged. The table looks fresh
(it was just rebuilt) but contains outdated nesp_tech data.

**If we keep going like this:**
Any BI report or alert consuming this model will show outdated machine intervention counts
from Tuesday to Sunday. If someone checks a machine's last intervention mid-week,
they may see data from the previous Monday at best.

---

### Problem 2 — `int_oracle_neshu__machines_yuman_maintenance_base` (Tag mismatch) 🟡

**Location:** `models/intermediate/oracle_neshu/int_oracle_neshu__machines_yuman_maintenance_base.sql`
**Tag:** `intermediate:oracle_neshu`

**What this model does:**
Links Oracle NESHU machines (devices) to their Yuman counterpart (materials),
enriched with client and site data from Yuman. Used downstream in machine
maintenance tracking marts.

**Cross-source dependencies:**
- `dim_oracle_neshu__device` ← oracle_neshu source (daily 01:00)
- `stg_yuman__materials` ← **yuman** source (weekdays 01:00)
- `stg_yuman__sites` ← **yuman** source
- `stg_yuman__clients` ← **yuman** source
- `stg_yuman__materials_categories` ← **yuman** source

**The issue:**
This model is tagged `oracle_neshu` only. When `dbt build --select tag:oracle_neshu`
runs (daily 01:00), it builds this model. In practice, yuman also runs at 01:00 daily
on weekdays, so it usually works fine.

However:
- On **weekends**, yuman does NOT run (schedule is Mon–Fri only). Oracle_neshu still
  runs daily including weekends. So Saturday and Sunday, this model rebuilds with
  Friday's yuman data — silently.
- If the **yuman pipeline fails** on a weekday, this model still runs successfully on
  its oracle_neshu trigger with stale yuman data. There is no dependency check between
  the two pipelines.
- The tag is **misleading**: the model has 4 yuman staging dependencies but will never
  appear in a `dbt ls --select tag:yuman` output. For any engineer looking at yuman
  impact, this model is invisible.

**If we keep going like this:**
Weekend refreshes of oracle_neshu produce a machine-to-yuman mapping based on
Friday's yuman state. If materials/sites are updated on weekends (unlikely but
possible), the mapping will be wrong until Monday. More importantly, as the project
grows, cross-source dependencies hidden under a single-source tag will make it
increasingly hard to reason about what a pipeline refresh actually does.

---

### Problem 3 — Freshness not configured on 5 sources 🟡

Sources `nesp_tech`, `nesp_co`, `gac`, `yuman_gcs`, `oracle_neshu_gcs` have no
freshness checks defined. `dbt source freshness` will not warn if these tables stop
being updated. Given that nesp_tech is critical (machine maintenance) and gac is
external (insurance claims), a silent ingestion failure would go undetected.

---

## 3. What is Working Well

- Naming conventions (`stg_`, `int_`, `fct_`, `dim_` with `{source}__` prefix) are
  clean and consistent throughout. No ambiguity on model provenance.
- Tag-per-source + layer approach is correct for single-source pipelines.
- `fct_technique__interventions` cross-source scenario is **already handled correctly**:
  the nesp_tech workflow runs `tag:nesp_tech` then `tag:technique` sequentially,
  and yuman runs at 01:00 so it's fresh by 07:30 Monday.
- Incremental merge strategy on high-frequency fact tables (oracle_neshu tasks,
  passages appro) is appropriate for BigQuery.
- Partitioning and clustering are applied correctly across the board.

---

## 4. Todo List — Short Term Fixes

### Fix 1 — Move `fct_nesp_co__machines_avec_interventions` out of nesp_co tag
- [ ] Move the model file to `models/marts/technique/`
- [ ] Update `dbt_project.yml`: remove from `nesp_co` tags, add `technique` tag
- [ ] The nesp_tech workflow already triggers `tag:technique` after `tag:nesp_tech`
      → this model will be built correctly on Monday after nesp_tech refresh
- [ ] Verify downstream BI reports still reference the same BigQuery table name
      (alias may need to stay the same or reports updated)

### Fix 2 — Add cross-source tags to `int_oracle_neshu__machines_yuman_maintenance_base`
- [ ] Add `cross_source` tag (or `technique`) in the model config or dbt_project.yml
- [ ] Document the yuman dependency clearly in the model description in `_models.yml`
- [ ] Consider whether this intermediate model should live under a shared folder
      (`intermediate/technique/` or `intermediate/cross/`)

### Fix 3 — Add freshness checks to unconfigured sources
- [ ] `nesp_tech`: add `warn_after: {count: 8, period: day}`, `error_after: {count: 10, period: day}`
      (weekly source, so warn if >8 days without refresh)
- [ ] `nesp_co`: add `warn_after: {count: 26, period: hour}`, `error_after: {count: 36, period: hour}`
- [ ] `gac`: decide if freshness should be enabled (currently null/disabled)
- [ ] `yuman_gcs`, `oracle_neshu_gcs`: add appropriate freshness based on ingestion cadence

---

## 5. Proposed Architecture — Marts by Business Unit (Migration Plan)

### Rationale

The current source-based mart organization works well as long as marts stay within
a single source domain. As the project grows, more cross-source business questions
will arise (machine lifetime from oracle + maintenance from yuman + interventions
from nesp_tech). Organizing marts by **business unit** makes the data model answer
business questions directly, rather than by technical source system.

### Proposed BU Structure

```
models/
├── staging/
│   └── {source}/           ← NO CHANGE. Keep source-based forever.
│
├── intermediate/
│   └── {source}/           ← Keep source-based for single-source logic.
│   └── cross/              ← NEW: intermediate models joining 2+ sources
│       └── int_cross__{process}.sql
│
└── marts/
    ├── logistique/          ← oracle_neshu + oracle_lcdp supply chain
    ├── technique/           ← nesp_tech + yuman maintenance (already exists)
    ├── commerce/            ← nesp_co + yuman commercial activity
    ├── finance/             ← mssql_sage P&L and accounting
    ├── parc_machines/       ← machine tracking: oracle_neshu + oracle_lcdp + yuman + nesp_tech
    ├── stock/               ← yuman_gcs + oracle_neshu_gcs stock snapshots
    └── assets/              ← gac vehicle claims
```

### BU Tag Strategy

```yaml
# dbt_project.yml
models:
  dbt_warehouse:
    marts:
      logistique:
        +tags: ['marts', 'bu_logistique']
      technique:
        +tags: ['marts', 'bu_technique']
      commerce:
        +tags: ['marts', 'bu_commerce']
      finance:
        +tags: ['marts', 'bu_finance']
      parc_machines:
        +tags: ['marts', 'bu_parc_machines']
      stock:
        +tags: ['marts', 'bu_stock']
      assets:
        +tags: ['marts', 'bu_assets']
```

### Model Mapping — Current → Proposed BU

| Current location | Current tag | Proposed BU folder | Proposed tag |
|---|---|---|---|
| marts/oracle_neshu/ (supply facts) | oracle_neshu | marts/logistique/ | bu_logistique |
| marts/oracle_lcdp/ (dims) | oracle_lcdp | marts/logistique/ | bu_logistique |
| marts/oracle_neshu/ (machine dims) | oracle_neshu | marts/parc_machines/ | bu_parc_machines |
| marts/yuman/ (workorder facts) | yuman | marts/technique/ or commerce/ | bu_technique / bu_commerce |
| marts/technique/ | technique | marts/technique/ | bu_technique ← keep |
| marts/nesp_co/ | nesp_co | marts/commerce/ | bu_commerce |
| marts/mssql_sage/ | mssql_sage | marts/finance/ | bu_finance |
| marts/gac/ | gac | marts/assets/ | bu_assets |
| marts/yuman_gcs/ | yuman_gcs | marts/stock/ | bu_stock |
| marts/oracle_neshu_gcs/ | oracle_neshu_gcs | marts/stock/ | bu_stock |

### Scheduling in the BU Model

Cross-source BU marts need a dedicated scheduler that fires after all their
upstream source pipelines are complete. Proposed approach:

**Option A — BU trigger at the end of the last upstream pipeline**

For each BU, identify the "last source to land" and append a BU dbt build step
at the end of that pipeline workflow.

| BU | Upstream sources | Last to land | Add BU build to |
|---|---|---|---|
| bu_logistique | oracle_neshu, oracle_lcdp | Both at 01:00 — add step after both | Dedicated Cloud Scheduler at 02:30 |
| bu_technique | nesp_tech, yuman | nesp_tech (Monday 07:30) | Already done: pipeline-nesp-tech runs `tag:technique` |
| bu_commerce | nesp_co, yuman | nesp_co at 08:00 (yuman is 01:00) | pipeline-sftp-nesp-client: add `tag:bu_commerce` step |
| bu_parc_machines | oracle_neshu, yuman, nesp_tech | nesp_tech (Monday 07:30) | pipeline-nesp-tech: add `tag:bu_parc_machines` step |
| bu_finance | mssql_sage | mssql_sage at 01:00 | pipeline-mssql-sage: add `tag:bu_finance` step |
| bu_stock | yuman_gcs, oracle_neshu_gcs | oracle_neshu_gcs at 23:00 | pipeline-oracle-stock-theorique: add `tag:bu_stock` step |
| bu_assets | gac | gac at 08:00 | pipeline-sftp-evs: add `tag:bu_assets` step |

**Option B — Dedicated cross-source Cloud Scheduler (simpler, less coupled)**

Add a single Cloud Scheduler at 09:30 Paris on weekdays that triggers a
dedicated workflow running all cross-source BU tags in order:

```
09:30 → dbt build --select tag:bu_logistique tag:bu_commerce tag:bu_parc_machines tag:bu_finance tag:bu_assets
```

This is less real-time but simpler to maintain and guarantees all upstream sources
(all running between 01:00–09:00) are complete before BU marts are rebuilt.

### Migration Steps

**Phase 1 — Fix current bugs without restructuring (1–2 days)**
1. Fix `fct_nesp_co__machines_avec_interventions` tag → move to `technique/`
2. Add cross-source tag to `int_oracle_neshu__machines_yuman_maintenance_base`
3. Add missing freshness checks

**Phase 2 — Create BU mart folders and move cross-source models (1 week)**
1. Create new BU folders under `marts/`
2. Move cross-source models to their BU folder
3. Update `dbt_project.yml` tags
4. Keep single-source marts in existing folders OR move them too (full migration)
5. Update `intermediate/cross/` for any intermediate models that span sources

**Phase 3 — Update Cloud Workflows for BU scheduling**
1. Add BU dbt build steps to relevant pipeline workflows
2. Or deploy the dedicated cross-source Cloud Scheduler
3. Update monitoring alerts if any are scoped by tag

**What to keep in mind during migration:**
- BigQuery table names are driven by `schema` + `alias` in model config, not by
  folder path. Moving a model file does NOT rename the BigQuery table as long as
  `alias` is set or the model name stays the same.
- If a model doesn't have an explicit `alias`, the BigQuery table name = model file name.
  Moving the file is safe in that case, but double-check before migrating.
- BI reports (Power BI etc.) reference the BigQuery table names, not dbt model paths.
  Table names will not break if you only move files and update tags.

---

## 6. Decision Points for the Team

1. **Do we do Phase 2 now or after other priorities?**
   Phase 1 fixes the real bugs. Phase 2 is a structural improvement for long-term
   maintainability. Recommend Phase 1 now, Phase 2 when there is a quiet period.

2. **BU trigger: append to pipeline workflow (Option A) or dedicated scheduler (Option B)?**
   Option B is simpler and decoupled. Option A gives fresher data but adds complexity
   to each workflow. Given most BU reports are not real-time, Option B is recommended.

3. **Do we keep source-based marts alongside BU marts, or do a full migration?**
   A full migration is cleaner but higher risk. A hybrid (BU for cross-source,
   source-based for pure single-source) is acceptable and lower effort.

4. **Who owns the `parc_machines` BU?**
   This BU spans oracle_neshu (device registry), yuman (material/maintenance),
   and nesp_tech (interventions). It touches multiple teams. Ownership should be
   agreed before building it out.
