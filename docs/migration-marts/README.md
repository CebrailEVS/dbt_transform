# Marts refacto by BU — preparation

> Internal doc — design and migration plan for reorganizing `models/marts/` by Business Unit.
> Status: **preparation, not started**. Last updated: 2026-05-19.
>
> **Companion docs:**
> - [`docs/architecture_review.md`](../architecture_review.md) — current state, source inventory, scheduling table
> - [`docs/scheduling_and_tagging_decisions.md`](../scheduling_and_tagging_decisions.md) — tag conventions, cross-source patterns

---

## 1. Why we refacto

Current `marts/` mixes two organizing axes:
- **By source:** `oracle_neshu/`, `yuman/`, `mssql_sage/`, ... → mono-source marts
- **By domain:** `technique/`, `commerce/` → cross-source marts

This breaks as soon as a new mart belongs to a partner BU but spans sources outside the existing transverse domains. Trigger: May 2026 — cross-source mart for BU Neshu (appro `oracle_neshu` + technique `yuman`) had no natural home.

Better to refacto before we accumulate 5–10 marts in the wrong place.

---

## 2. Core principles

### 2.1 Per source from raw to intermediate, per BU at marts

| Layer | Organization | Changes? |
|---|---|---|
| `prod_raw` (BigQuery) | per source | unchanged (out of dbt scope) |
| `models/staging/` | per source | unchanged |
| `models/intermediate/` | per source | unchanged — cross-source joins go **directly** into marts, never into intermediate |
| `models/marts/` | **per BU + transverse domains** | **moved** |

### 2.2 EL stays per source, T moves to BU

Sources are shared across BUs (e.g. `yuman` serves Neshu, LCDP, Technique, Commerce, Supply Chain). Extraction frequency and connector ownership belong to the source, not the BU.

| Layer | Today | After refacto |
|---|---|---|
| EL pipelines | `pipeline-<source>` (per source) | **Unchanged** structure, but `dbt build` step inside is **scoped down** to `tag:<source>,path:staging|intermediate` only |
| T workflows | `transform-technique-daily`, `transform-commerce-daily` (per domain) | **One per mart folder**: `transform-<bu>-daily` and `transform-<domain>-daily` |

### 2.3 Mart placement rule

> *Who is this mart about?*
>
> - About a partner (internal client) → `marts/<partner>/` (e.g. `neshu/`, `lcdp/`)
> - About EVS's own activity, transverse → `marts/<domain>/` (e.g. `commerce/`, `technique/`)
> - About an EVS support function → `marts/<function>/` (e.g. `finance/`, `supply_chain/`, `services_generaux/`)

A mart that crosses topics for one partner (e.g. Neshu maintenance preventive = Neshu commercial + Neshu technical data) stays in the **partner folder** (`neshu/`). The transverse `technique/` and `commerce/` are reserved for **non-partner-specific** EVS activity.

---

## 3. Target structure

### 3.1 Mart folders (= Power BI workspaces, 1:1)

| Folder | Type | Power BI workspace | Sources commonly used |
|---|---|---|---|
| `neshu/` | Partner BU | Neshu | oracle_neshu, yuman, zoho_desk (future) |
| `lcdp/` | Partner BU | LCDP | oracle_lcdp, yuman, zoho_desk (future) |
| `commerce/` | EVS transverse | Commerce | nesp_co, nesp_tech, yuman |
| `technique/` | EVS transverse | Technique | nesp_co, nesp_tech, yuman |
| `finance/` | EVS function | Finance | mssql_sage |
| `services_generaux/` | EVS function | Services Généraux | gac |
| `supply_chain/` | EVS function | Supply Chain | yuman_gcs, oracle_neshu_gcs, oracle_neshu |

### 3.2 Adding a future partner BU

Designed to scale. Adding e.g. `partner_xyz/` requires:
1. New folder `models/marts/partner_xyz/` + `_partner_xyz__marts_models.yml`
2. New subkey in `dbt_project.yml` under `marts:`
3. New `transform-partner-xyz-daily.yaml` (copy from template)
4. Two Terraform resources in `workflows.tf` (workflow + scheduler)
5. New exposure file `models/exposures/partner_xyz.yml`

~30 min of plumbing per new partner. No refacto of existing folders.

---

## 4. Orchestration: 7 T workflows

### 4.1 EL pipeline scheduling (current state, unchanged)

| Source | EL pipeline | Schedule |
|---|---|---|
| oracle_neshu | `pipeline-oracle-neshu` | `0 1 * * *` — daily 01:00 |
| oracle_lcdp | `pipeline-oracle-lcdp` | `0 1 * * *` — daily 01:00 |
| yuman | `pipeline-yuman` | `0 1 * * 1-5` — weekdays 01:00 |
| mssql_sage | `pipeline-mssql-sage` | `0 1 * * 1-5` — weekdays 01:00 |
| oracle_neshu_gcs | `pipeline-oracle-stock-theorique` | `0 23 * * *` — daily 23:00 |
| yuman_gcs | `pipeline-sftp-gcs-yuman` | `0 6 * * 1-6` — Mon–Sat 06:00 |
| nesp_co | `pipeline-nesp-co` | `0 8 * * *` — daily 08:00 |
| nesp_tech | `pipeline-nesp-tech` | `30 7 * * 1` — Monday 07:30 (weekly) |
| gac | `pipeline-sftp-evs` | `0 8 * * *` — daily 08:00 |

### 4.2 Target T workflow schedules (+2h margin minimum)

| T workflow | Waits for EL | Schedule |
|---|---|---|
| `transform-neshu-daily` | oracle_neshu (01:00), yuman (01:00) | `0 3 * * *` — daily 03:00 |
| `transform-lcdp-daily` | oracle_lcdp (01:00), yuman (01:00) | `0 3 * * *` — daily 03:00 |
| `transform-technique-daily` | nesp_co (08:00), nesp_tech (Mon 07:30), yuman (01:00) | `0 10 * * *` — daily 10:00 ⚠️ moved from 03:00 |
| `transform-commerce-daily` | nesp_co (08:00), nesp_tech (Mon 07:30), yuman (01:00) | `0 10 * * *` — daily 10:00 |
| `transform-finance-daily` | mssql_sage (01:00 weekdays) | `0 3 * * 1-5` — weekdays 03:00 |
| `transform-services-generaux-daily` | gac (08:00) | `0 10 * * *` — daily 10:00 |
| `transform-supply-chain-daily` | oracle_neshu_gcs (23:00 prev day), yuman_gcs (06:00), oracle_neshu (01:00) | `0 8 * * *` — daily 08:00 |

**⚠️ Schedule fix:** `transform-technique-daily` currently runs at 03:00 but its sources `nesp_co` (08:00) and `nesp_tech` (Mon 07:30) finish later. Today this means technique marts use **previous day's** nesp data. Moving to 10:00 fixes this. Same applies to the new `transform-commerce-daily`.

**Note:** `transform-commerce-daily` does **not exist today** — commerce marts are not currently orchestrated by a dedicated workflow. To be created as part of the refacto.

---

## 5. Critical things to watch

### 5.1 `dbt_project.yml` — must be rewritten
Today: per-source subkeys auto-tag every mart. After refacto: replace with BU/domain subkeys.

```yaml
# Before
marts:
  oracle_neshu:
    +tags: ['marts', 'oracle_neshu']
  technique:
    +tags: ['marts', 'technique']

# After
marts:
  neshu:
    +tags: ['marts', 'neshu']
  lcdp:
    +tags: ['marts', 'lcdp']
  commerce:
    +tags: ['marts', 'commerce']      # unchanged
  technique:
    +tags: ['marts', 'technique']     # unchanged (scope reduced — neshu/lcdp marts move out)
  finance:
    +tags: ['marts', 'finance']
  services_generaux:
    +tags: ['marts', 'services_generaux']
  supply_chain:
    +tags: ['marts', 'supply_chain']
```

With folder-based tagging, **do not** set `tags=[...]` manually in each mart's `{{ config() }}` — the folder config handles it. Remove redundant model-level tags during migration.

`staging:` and `intermediate:` subkeys stay per source, unchanged.

### 5.2 Model renaming — same-day rename, DA coordination

For each migrated mart we rename the .sql file **and** the BigQuery physical table at the same time. The Data Analyst updates the corresponding Power BI dataset(s) the same day so reports point to the new table.

| What | How |
|---|---|
| Rename .sql file | `git mv` to new BU folder + new name (e.g. `fct_oracle_neshu__device.sql` → `neshu/fct_neshu__device.sql`) |
| Rename BigQuery physical table | nothing to do — dbt builds the new table on next run; old table can be dropped after BI is repointed |
| Update Power BI dataset | DA repoints the `.pbix` to the new `project.dataset.table` and republishes |

**Tradeoff:** ~15-30 min of broken BI per migrated mart between dbt build and `.pbix` republish. Acceptable given solo DE + 1 DA. Each Phase 2 PR is announced to DA in advance so they can plan the .pbix update.

**Why not `alias=`:** keeping the BQ name pinned to the old source-based name via `alias='fct_oracle_neshu__device'` would avoid the BI break but creates tech debt — a Phase 3 PR to remove all aliases later, plus a "physical rename" project that in practice would never happen. Our team size makes same-day coordination cheaper.

### 5.3 Refs in downstream marts and exposures
- Every `ref()` to a renamed dbt resource must be updated in the same PR.
- `models/exposures/*.yml` must reference the new dbt names.
- Run `dbt ls --select exposure:*` after each batch.

### 5.4 Terraform state
Renaming `google_workflows_workflow.transform_technique_daily` recreates the resource (delete + create) unless `terraform state mv` is run first.

Since the refacto is **additive** in phase 1 (new workflows alongside old), no Terraform rename is needed initially. Cleanup phase removes old resources after all marts have migrated.

### 5.5 Existing `dbt build tag:<source>` in EL pipelines
Each `pipeline-<source>.yaml` today builds `tag:<source>` post-extraction (staging + intermediate + marts of that source). After refacto, scope down to staging + intermediate only:

```yaml
env_override:
  - name: "DBT_TAG_SELECTOR"
    value: "tag:<source>,path:models/staging models/intermediate"
```

Apply to every EL pipeline as part of phase 3 cleanup.

### 5.6 `profiles.yml` — not impacted
Contains GCP project, dataset prefix, keyfile, dev/prod targets. No coupling to mart layout. No action.

---

## 6. Migration plan

### Phase 0 — Inventory (no code changes)
- [x] Inventory the 32 existing marts → target folder mapping. See [`inventory.md`](./inventory.md). 24 unambiguous, 8 to confirm.
- [ ] Confirm the 7 BI workspaces match the 7 mart folders.
- [ ] Identify exposures impact: per Power BI report, which marts feed it.

### Phase 1 — Additive scaffolding (1 PR)
- [ ] Create empty folders: `neshu/`, `lcdp/`, `finance/`, `services_generaux/`, `supply_chain/`.
- [ ] Add empty `_<folder>__marts_models.yml` in each.
- [ ] Add new subkeys in `dbt_project.yml`.
- [ ] Create 5 new `transform-<x>-daily.yaml` workflow files (copy `transform-technique-daily.yaml` template).
- [ ] Add 5 Terraform `google_workflows_workflow` + `google_cloud_scheduler_job` blocks. **Schedulers paused** until phase 2 finishes for the corresponding BU.
- [ ] Merge — no behavioral change.

### Phase 2 — Migrate marts, BU by BU (1 PR per BU/domain)

Order suggestion (smallest first to rehearse the process):

1. `finance/` (likely smallest — only mssql_sage marts)
2. `services_generaux/` (gac marts)
3. `supply_chain/` (gcs + neshu)
4. `lcdp/` (oracle_lcdp marts + any technique mart that's actually LCDP-specific)
5. `neshu/` (oracle_neshu marts + technique marts that are Neshu-specific, e.g. `fct_technique__neshu_maintenance_preventives`)
6. `technique/`, `commerce/` — scope reduction (move out partner-specific marts, keep transverse only)

Per-BU checklist:
- [ ] Announce upcoming PR to DA so they can plan the `.pbix` update window
- [ ] Branch `feature/marts-refacto-<folder>`
- [ ] `git mv` .sql files to new folder, rename to `<prefix>_<folder>__<entity>` (e.g. `fct_neshu__device`)
- [ ] Update all `ref()` calls across the repo
- [ ] Remove explicit `tags=[...]` in model configs (folder config handles it)
- [ ] Merge YAML test files into `_<folder>__marts_models.yml`
- [ ] Update `models/exposures/*.yml`
- [ ] Activate the BU scheduler in Terraform (`paused = false`)
- [ ] PR + merge
- [ ] DA updates the `.pbix` to point to the new BigQuery table(s) and republishes
- [ ] Drop the old BigQuery table(s) once BI is confirmed working

### Phase 3 — Cleanup (1 PR)
- [ ] Delete empty `marts/oracle_neshu/`, `marts/oracle_lcdp/`, `marts/yuman/`, `marts/mssql_sage/`, `marts/gac/`, `marts/yuman_gcs/`, `marts/oracle_neshu_gcs/`, `marts/nesp_tech/` folders
- [ ] Remove source-named subkeys from `dbt_project.yml` `marts:` section
- [ ] Scope down `dbt build` step in each `pipeline-<source>.yaml` to staging+intermediate only
- [ ] Update `transform-technique-daily` schedule to 10:00 (fix latency bug for nesp data)
- [ ] Update `README.md`, `CONTRIBUTING.md`, `CONVENTIONS.md`, `CLAUDE.md` to document new layout

### Phase 4 — Validation
- [ ] `dbt build` in dev → all green
- [ ] All exposures resolve: `dbt ls --select exposure:*`
- [ ] Power BI smoke test: 1 dataset per BU refreshes
- [ ] `dbt source freshness` unchanged
- [ ] All 7 BU/domain schedulers triggered successfully day 1

---

## 7. Estimated effort (rough, solo DE)

| Phase | Effort |
|---|---|
| Phase 0 (inventory) | 0.5 day |
| Phase 1 (scaffolding) | 0.5 day |
| Phase 2 (migration, per BU/domain × 6) | 0.5–1 day each → 3–6 days |
| Phase 3 (cleanup) | 0.5 day |
| Phase 4 (validation) | 0.5 day |
| **Total** | **5–8 days** spread over 2–3 weeks |

PR per BU. Validate Power BI between each. Small team → can move fast, incidents recoverable.

---

## 8. Out of scope (intentionally)

- Migrating EL pipelines to BU organization (sources stay per source).
- Reorganizing `staging/` and `intermediate/` (stay per source).
- Snapshots (managed by Cloud Workflows, not refactored).
- Renaming physical BigQuery tables (separate later project).
- Switching from cron to event-driven workflow triggers (separate initiative — see `docs/architecture_review.md`).

---

## 9. Open questions

- Existing `docs/scheduling_and_tagging_decisions.md` mentions an `cross_post_*` tag pattern explicitly dropped. Confirm no remnant in current code before phase 2.
- Confirm `gac` workspace name is "Services Généraux" in Power BI (folder `services_generaux/` must match).
- For partner-specific exposures already in `models/exposures/neshu.yml`, `lcdp.yml`: are they all up to date?
