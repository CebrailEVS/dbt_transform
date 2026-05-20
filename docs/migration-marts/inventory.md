# Marts inventory — current → target folder

> Companion of [`README.md`](./README.md) — Phase 0 deliverable.
> Status: **draft**. Last updated: 2026-05-19.
>
> 32 marts (not 23 — memory was stale). 1 to delete, 31 to migrate.

---

## 0. Naming convention for marts

Documented in [`CONVENTIONS.md` § Nommage des marts](../../CONVENTIONS.md#nommage-des-marts--convention-by-bu-refacto-en-cours). Target names in §1 below apply that convention.

**BQ table rename:** same-day with the SQL rename. DA updates the `.pbix` the same day to point to the new table. See [`README.md` §5.2](./README.md#52-model-renaming--same-day-rename-da-coordination).

---

## 1. Mapping by target folder (31 marts after deletions)

Target names follow §0 convention. ⚠️ flags model names worth a second look before migration.

### → `neshu/` (12)

| Current path | Target name | Notes |
|---|---|---|
| `oracle_neshu/dim_oracle_neshu__company.sql` | `dim_neshu__company` | exposed by `business_review`, `passage_appro_monitoring`, `reporting_appro` |
| `oracle_neshu/dim_oracle_neshu__contract.sql` | `dim_neshu__contract` | exposed by `business_review` |
| `oracle_neshu/dim_oracle_neshu__device.sql` | `dim_neshu__device` | exposed by `business_review` |
| `oracle_neshu/dim_oracle_neshu__product.sql` | `dim_neshu__product` | |
| `oracle_neshu/dim_oracle_neshu__resources.sql` | `dim_neshu__resource` | singular |
| `oracle_neshu/dim_oracle_neshu__vehicule_roadman.sql` | `dim_neshu__vehicule_roadman` | |
| `oracle_neshu/fct_oracle_neshu__appro.sql` | `fct_neshu__appro` | exposed by `reporting_appro` |
| `oracle_neshu/fct_oracle_neshu__chargement_par_quinzaine.sql` | `fct_neshu__chargement_quinzaine` | grain suffix |
| `oracle_neshu/fct_oracle_neshu__chargement_vs_conso.sql` | ⏸️ **pending DA** | BQ description says "Table intermédiaire" — may need to move to `intermediate/` instead of marts. Decision deferred until DA confirms direct PBI consumer vs downstream mart usage. |
| `oracle_neshu/fct_oracle_neshu__conso_business_review.sql` | `fct_neshu__consommation` | BI name moves to exposure |
| `oracle_neshu/fct_oracle_neshu__pa_business_review.sql` | `fct_neshu__passage_appro` | PA = Passage Appro (confirmed via BQ description audit 2026-05-20) |
| `technique/fct_technique__neshu_maintenance_preventives.sql` | `fct_neshu__maintenance_preventive` | partner-specific → moves out of `technique/`, exposed by `maintenance_preventives` |
| `yuman/fct_yuman__workorder_delais_neshu.sql` | `fct_neshu__workorder_delai` | ⚠️ Neshu-specific yuman fact moves to neshu folder |

### → `lcdp/` (3)

| Current path | Target name | Notes |
|---|---|---|
| `oracle_lcdp/dim_oracle_lcdp__company.sql` | `dim_lcdp__company` | exposed by `passage_appro_monitoring_lcdp` |
| `oracle_lcdp/dim_oracle_lcdp__device.sql` | `dim_lcdp__device` | |
| `oracle_lcdp/dim_oracle_lcdp__product.sql` | `dim_lcdp__product` | |

> No fact today — facts to be built by DA later.

### → `technique/` (8)

| Current path | Target name | Notes |
|---|---|---|
| `yuman/dim_yuman__clients.sql` | `dim_technique__client` | singular |
| `yuman/dim_yuman__sites.sql` | `dim_technique__site` | singular |
| `yuman/dim_yuman__materials.sql` | `dim_technique__material` | singular. Add `client_id` + `site_id` FK columns if not present, for relationships with `dim_technique__client` / `dim_technique__site` in PBI |
| `yuman/dim_yuman__technicians.sql` | `dim_technique__technician` | singular |
| `technique/fct_technique__interventions.sql` | `fct_technique__intervention` | singular |
| `yuman/fct_yuman__workorder_pricing.sql` | `fct_technique__workorder_pricing` | transverse (ad-hoc reporting for intervention pricing) |
| `nesp_tech/fct_nesp_tech__alerting_conso_pieces_aguila.sql` | `fct_technique__alerting_consommation_piece_aguila` | long name accepté |
| `nesp_tech/fct_nesp_tech__pieces_detachees_pricing.sql` | `fct_technique__piece_detachee_pricing` | |

### → `commerce/` (1)

| Current path | Target name | Notes |
|---|---|---|
| `commerce/fct_commerce__machines_avec_interventions.sql` | `fct_commerce__machine_intervention` | singular, drop `_avec_` connector |

### → `finance/` (1) ✅ DONE 2026-05-20

| Current path | Target name | Notes |
|---|---|---|
| ~~`mssql_sage/fct_mssql_sage__pnl_bu_kpis.sql`~~ → `finance/fct_finance__pnl_bu.sql` | `fct_finance__pnl_bu` | ✅ fully migrated. PR #76 merged. `.pbix` repointed by DA. Old BQ tables dropped (dev + prod). Row count + aggregate parity verified before drop. |

### → `services_generaux/` (1)

| Current path | Target name | Notes |
|---|---|---|
| `gac/fct_gac__sinistres_sg.sql` | `fct_services_generaux__sinistre` | `_sg` redundant once in folder, singular |

### → `supply_chain/` (3)

Three stock-related facts here. Source suffix kept to disambiguate them (rule §0 last row).

| Current path | Target name | Notes |
|---|---|---|
| `oracle_neshu_gcs/fct_oracle_neshu_gcs__stock_products.sql` | `fct_supply_chain__stock_neshu` | source suffix kept (collision risk with yuman stock) |
| `yuman_gcs/fct_yuman_gcs__stock_articles.sql` | `fct_supply_chain__stock_yuman` | idem |
| `oracle_neshu/fct_oracle_neshu__supply_flux.sql` | `fct_supply_chain__flux_neshu` | BI consumer is Supply Chain workspace |

### → DELETE (2)

| Current path | Reason |
|---|---|
| `yuman/fct_yuman__suivi_partenaires.sql` | not consumed today, confirmed by DE |
| `yuman/dim_yuman__materials_clients.sql` | **OBT anti-pattern** — flattens client+site attrs into the material dim. Decided 2026-05-20 to drop. DA recreates an enriched view via Power Query / SQL custom in PBI when needed. If 3+ reports end up duplicating the same flatten, promote back to a dedicated dbt mart. See §5 architecture decision below. |

### → External sources (not dbt models — 2)

Tables in `prod_marts` written directly by Cloud Run jobs (Oracle SQL → BQ → API call to refresh PBI, ×8 / day). Declared in dbt as `source()` in `_<source>__marts_sources.yml`, not as models.

| Current table | Target table name | Where it's declared today | Where it moves |
|---|---|---|---|
| `prod_marts.fct_oracle_neshu__monitoring_passages_appro` | `fct_neshu__monitoring_passage_appro` | `models/marts/oracle_neshu/_oracle_neshu__marts_sources.yml` | new `models/marts/neshu/_neshu__marts_sources.yml` |
| `prod_marts.fct_oracle_lcdp__monitoring_passages_appro` | `fct_lcdp__monitoring_passage_appro` | `models/marts/oracle_lcdp/_oracle_lcdp__marts_sources.yml` | new `models/marts/lcdp/_lcdp__marts_sources.yml` |

> ⚠️ **Rename complexity** — these tables are NOT under dbt control. Renaming requires coordinated update across:
> 1. Cloud Run job code (Oracle SQL pipeline writing to BQ)
> 2. Cloud Run job config (target table name)
> 3. PBI API refresh call (if table name is passed as arg)
> 4. dbt `source()` declaration
> 5. `.pbix` dataset connector
>
> To handle inside the `neshu/` and `lcdp/` Phase 2 PRs, with extra coordination.

---

## 2. Decisions log

### 2026-05-19
- Yuman dims and `workorder_pricing` → `technique/` (Yuman is transverse for all non-Nespresso partners).
- `fct_yuman__suivi_partenaires` → **delete**.
- `fct_yuman__workorder_delais_neshu` → `neshu/`.
- `fct_oracle_neshu__supply_flux` → `supply_chain/` (BI in Supply Chain workspace).
- `gac` BU confirmed: **Services Généraux**.
- LCDP has no fact today — expected, DA will build them later.
- Source suffix in mart name only when needed to disambiguate inside a folder (currently only `supply_chain/` stocks).
- Mart naming convention documented in `CONVENTIONS.md` § Nommage des marts.

### 2026-05-20 (BigQuery audit)
- `fct_oracle_neshu__pa_business_review` → `fct_neshu__passage_appro` (PA = Passage Appro, confirmed via BQ table description).
- `fct_oracle_neshu__chargement_vs_conso` → **pending DA**: may belong to `intermediate/` rather than marts (BQ description says "Table intermédiaire").
- `dim_yuman__materials_clients` → **delete** (OBT anti-pattern, see §5).
- 2 external Cloud Run tables identified (`monitoring_passages_appro` neshu + lcdp) — must be coordinated separately.
- Initial inventory said 32 marts. Correct count: **33 dbt models + 2 external sources = 35 objects** in `prod_marts`.

---

## 3. Remaining items to address during Phase 2

1. **Exposures backfill** — DE only declared the 5 exposures for the BI dashboards he built himself. The DA owns BI now and will be the source of truth for which mart feeds which report. **Action:** during each BU PR in Phase 2, ping DA to identify the consumers and declare them as exposures in `models/exposures/<bu>.yml`.
2. **`fct_oracle_neshu__chargement_vs_conso`** — pending DA call: mart or intermediate? If used directly by a PBI report → keep in marts as `fct_neshu__chargement_consommation_par_passage`. If only consumed by another mart → move to `models/intermediate/oracle_neshu/` as `int_oracle_neshu__chargement_consommation_passage`.
3. **`fct_neshu__maintenance_preventive`** — confirm singular form with DA at the time of the `neshu/` PR.
4. **Cloud Run table rename coordination** (`neshu` + `lcdp` PRs) — see §1 "External sources" warning box.

---

## 4. Final count (corrected 2026-05-20)

| Target | dbt models | External sources |
|---|---|---|
| `neshu/` | 13 | 1 (`fct_neshu__monitoring_passage_appro`) |
| `lcdp/` | 3 | 1 (`fct_lcdp__monitoring_passage_appro`) |
| `technique/` | 8 | 0 |
| `commerce/` | 1 | 0 |
| `finance/` | 1 | 0 |
| `services_generaux/` | 1 | 0 |
| `supply_chain/` | 3 | 0 |
| **Migrated total** | **30** | **2** |
| Deleted | 2 (`fct_yuman__suivi_partenaires`, `dim_yuman__materials_clients`) | — |
| **Grand total** | **32 dbt + 2 external = 34 BQ tables** | |

---

## 5. Architecture decision (2026-05-20) — flatten in dbt vs flatten in PBI

Triggered by the review of `dim_yuman__materials_clients` (OBT-style dim aggregating material + client + site attrs into one table, 9500 rows). Three options were debated:

| Option | Where the flatten logic lives | Verdict |
|---|---|---|
| 1. Flatten in dbt (current `dim_yuman__materials_clients`) | dbt | **Rejected** — OBT anti-pattern: conflates 3 distinct entities (material, client, site), duplicates storage, breaks conformed dimensions principle |
| 2. Star schema in dbt + PBI relationships | PBI semantic model | Valid but constrains the DA's data model |
| 3. **Star schema in dbt + custom SQL in Power Query when needed** | PBI Power Query (per report) | **Chosen as default** — clean dbt, DA flexibility |

**Rule adopted:**

- **Default** = star schema in dbt with separate conformed dimensions (`dim_technique__client`, `dim_technique__site`, `dim_technique__material`). DA can write a SQL custom query in Power Query / Power BI to flatten when a specific report needs it.
- **Promotion to mart** = if the same flatten is duplicated across 3+ reports, or contains non-trivial logic (filtering rules, derived statuses), promote it back to a dedicated dbt mart (e.g. `fct_neshu__machine_park_enriched`). This brings the logic back into version control + tests + lineage.

**Tradeoff accepted:** logic in Power Query is not versioned in git, not tested by `dbt test`, and invisible to `dbt ls` lineage. The promotion rule mitigates this for recurring logic.

**Kimball clarification (for the record):** the previous CONVENTIONS.md guidance "aplatir les attributs des dims parentes dans la dim enfant" only applies to **display attributes of a natural parent** (ex. `company_name` in `dim_device` for tooltips, 1-3 columns max). It does NOT mean flattening entire parent dimensions into child dims — that's OBT.
