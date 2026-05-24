# dbt_warehouse — Architecture Review

> Internal doc — Last updated: 2026-05-22.
>
> **Scope** : inventaire des sources/pipelines + problèmes en production
> non encore corrigés + todos court-terme.
>
> Pour le refacto marts by BU (mapping, structure cible, progression) :
> voir [`docs/migration-marts/`](./migration-marts/). Sections §5-7 obsolètes
> supprimées 2026-05-22 (brainstorming BU structure et naming convention
> remplacés par `CONVENTIONS.md` § Marts — pattern complet).

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
- `fct_technique__intervention` cross-source scenario is **already handled correctly**:
  the nesp_tech workflow runs `tag:nesp_tech` then `tag:technique` sequentially,
  and yuman runs at 01:00 so it's fresh by 07:30 Monday.
- Incremental merge strategy on high-frequency fact tables (oracle_neshu tasks,
  passages appro) is appropriate for BigQuery.
- Partitioning and clustering are applied correctly across the board.

---

## 4. Todo List — Short Term Fixes

### ~~Fix 1~~ — superseded by commerce migration
~~Move `fct_nesp_co__machines_avec_interventions` out of nesp_co tag~~

Le modèle est déjà dans `marts/commerce/` (déplacé/renommé avant le refacto BU). Tagué `commerce` via folder. Sera renommé `fct_commerce__machine_intervention` lors de la PR commerce (Phase 2 #8).

### ~~Fix 2~~ — model no longer exists
~~Add cross-source tags to `int_oracle_neshu__machines_yuman_maintenance_base`~~

Ce modèle a été supprimé ou renommé entre 2026-03 (date du doc) et aujourd'hui. Seul `int_oracle_neshu__valorisation_parc_machines` subsiste dans `intermediate/oracle_neshu/`.

### Fix 3 — Add freshness checks to unconfigured sources

⏳ **Still TODO**. À vérifier dans `models/staging/<source>/_<source>__sources.yml` si chaque source a `loaded_at_field` + `freshness` configurés. Sources concernées :

- [ ] `nesp_tech`: add `warn_after: {count: 8, period: day}`, `error_after: {count: 10, period: day}`
      (weekly source, so warn if >8 days without refresh)
- [ ] `nesp_co`: add `warn_after: {count: 26, period: hour}`, `error_after: {count: 36, period: hour}`
- [ ] `gac`: decide if freshness should be enabled (currently null/disabled)
- [ ] `yuman_gcs`, `oracle_neshu_gcs`: add appropriate freshness based on ingestion cadence

---
