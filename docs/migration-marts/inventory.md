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

### → `neshu/` (13) ✅ migrated 2026-05-21

| Current path | Target name | Notes |
|---|---|---|
| ~~`oracle_neshu/dim_oracle_neshu__company.sql`~~ → `neshu/dim_neshu__company.sql` | `dim_neshu__company` | exposed by `business_review`, `passage_appro_monitoring`, `reporting_appro` |
| ~~`oracle_neshu/dim_oracle_neshu__contract.sql`~~ → `neshu/dim_neshu__contract.sql` | `dim_neshu__contract` | exposed by `business_review` |
| ~~`oracle_neshu/dim_oracle_neshu__device.sql`~~ → `neshu/dim_neshu__device.sql` | `dim_neshu__device` | exposed by `business_review` |
| ~~`oracle_neshu/dim_oracle_neshu__product.sql`~~ → `neshu/dim_neshu__product.sql` | `dim_neshu__product` | |
| ~~`oracle_neshu/dim_oracle_neshu__resources.sql`~~ → `neshu/dim_neshu__resource.sql` | `dim_neshu__resource` | singular |
| ~~`oracle_neshu/dim_oracle_neshu__vehicule_roadman.sql`~~ → `neshu/dim_neshu__vehicule_roadman.sql` | `dim_neshu__vehicule_roadman` | |
| ~~`oracle_neshu/fct_oracle_neshu__appro.sql`~~ → `neshu/fct_neshu__appro.sql` | `fct_neshu__appro` | exposed by `reporting_appro`, `business_review` |
| ~~`oracle_neshu/fct_oracle_neshu__chargement_par_quinzaine.sql`~~ → `neshu/fct_neshu__chargement_quinzaine.sql` | `fct_neshu__chargement_quinzaine` | grain suffix |
| ~~`oracle_neshu/fct_oracle_neshu__chargement_vs_conso.sql`~~ → `neshu/fct_neshu__chargement_consommation.sql` | `fct_neshu__chargement_consommation` | Resolved: mart (not intermediate). Drop `_vs_` connector. YAML description updated to remove "Table intermédiaire" wording. |
| ~~`oracle_neshu/fct_oracle_neshu__conso_business_review.sql`~~ → `neshu/fct_neshu__consommation.sql` | `fct_neshu__consommation` | BI name moves to exposure |
| ~~`oracle_neshu/fct_oracle_neshu__pa_business_review.sql`~~ | ❌ **deleted post-migration** | Obsolete — not consumed by any PBI report (confirmed via PBI MCP audit). Renamed during the neshu PR then deleted in a follow-up commit. BQ table to be dropped manually. |
| ~~`technique/fct_technique__neshu_maintenance_preventives.sql`~~ → `neshu/fct_neshu__maintenance_preventive.sql` | `fct_neshu__maintenance_preventive` | partner-specific → moves out of `technique/`, exposed by `maintenance_preventives` |
| ~~`yuman/fct_yuman__workorder_delais_neshu.sql`~~ → `neshu/fct_neshu__workorder_delai.sql` | `fct_neshu__workorder_delai` | leftover `alias=` cleaned up during migration |

Snapshots updated in this PR: `snap_oracle_neshu__company` (ref → `dim_neshu__company`), `snap_oracle_neshu__device` (ref → `dim_neshu__device`). File names and BQ table names of snapshots unchanged (history preservation).

**Out of scope (separate PRs):**
- External Cloud Run table `fct_oracle_neshu__monitoring_passages_appro` rename → dedicated branch later
- Dim refactor based on PBI architecture lessons (denormalize company attrs into device, drop bidirectionals, etc.) → separate follow-up

### → `lcdp/` (3) ✅ migrated 2026-05-21

| Current path | Target name | Notes |
|---|---|---|
| ~~`oracle_lcdp/dim_oracle_lcdp__company.sql`~~ → `lcdp/dim_lcdp__company.sql` | `dim_lcdp__company` | exposed by `passage_appro_monitoring_lcdp` |
| ~~`oracle_lcdp/dim_oracle_lcdp__device.sql`~~ → `lcdp/dim_lcdp__device.sql` | `dim_lcdp__device` | |
| ~~`oracle_lcdp/dim_oracle_lcdp__product.sql`~~ → `lcdp/dim_lcdp__product.sql` | `dim_lcdp__product` | |

External source `fct_lcdp__monitoring_passage_appro` already migrated in PR #82.

> No fact today — facts to be built by DA later.

### → `technique/` (10) ✅ migrated 2026-05-21

5 dims (conformed yuman) + 5 facts (3 yuman + 2 nesp_tech après rename).

| Current path | Target name | Notes |
|---|---|---|
| ~~`yuman/dim_yuman__clients.sql`~~ → `technique/dim_technique__client.sql` | `dim_technique__client` | singular |
| ~~`yuman/dim_yuman__sites.sql`~~ → `technique/dim_technique__site.sql` | `dim_technique__site` | singular |
| ~~`yuman/dim_yuman__materials.sql`~~ → `technique/dim_technique__material.sql` | `dim_technique__material` | singular |
| ~~`yuman/dim_yuman__materials_clients.sql`~~ → `technique/dim_technique__parc_machine.sql` | `dim_technique__parc_machine` | renommé en métier (parc machine) — OBT préservée car consommée directement par un BI |
| ~~`yuman/dim_yuman__technicians.sql`~~ → `technique/dim_technique__technician.sql` | `dim_technique__technician` | singular |
| ~~`technique/fct_technique__interventions.sql`~~ → `technique/fct_technique__intervention.sql` | `fct_technique__intervention` | singular (rename in-place) |
| ~~`yuman/fct_yuman__workorder_pricing.sql`~~ → `technique/fct_technique__workorder_pricing.sql` | `fct_technique__workorder_pricing` | transverse (pricing Yuman, tous partenaires) |
| ~~`yuman/fct_yuman__suivi_partenaires.sql`~~ → `technique/fct_technique__suivi_partenaire.sql` | `fct_technique__suivi_partenaire` | conservé finalement (pas consommé aujourd'hui mais gardé pour follow-up) |
| ~~`nesp_tech/fct_nesp_tech__alerting_conso_pieces_aguila.sql`~~ → `technique/fct_technique__alerting_consommation_aguila.sql` | `fct_technique__alerting_consommation_aguila` | Nespresso machine type Aguila |
| ~~`nesp_tech/fct_nesp_tech__pieces_detachees_pricing.sql`~~ → `technique/fct_technique__piece_detachee_pricing_nespresso.sql` | `fct_technique__piece_detachee_pricing_nespresso` | suffixe `_nespresso` pour distinguer de workorder_pricing (Yuman) |

Folders `yuman/` et `nesp_tech/` supprimés (vides après le move). Subkeys `yuman:` et `nesp_tech:` retirés de `dbt_project.yml`.

### → `commerce/` (1)

| Current path | Target name | Notes |
|---|---|---|
| `commerce/fct_commerce__machines_avec_interventions.sql` | `fct_commerce__machine_intervention` | singular, drop `_avec_` connector |

### → `finance/` (1) ✅ DONE 2026-05-20

| Current path | Target name | Notes |
|---|---|---|
| ~~`mssql_sage/fct_mssql_sage__pnl_bu_kpis.sql`~~ → `finance/fct_finance__pnl_bu.sql` | `fct_finance__pnl_bu` | ✅ fully migrated. PR #76 merged. `.pbix` repointed by DA. Old BQ tables dropped (dev + prod). Row count + aggregate parity verified before drop. |

### → `services_generaux/` (1) ✅ DONE 2026-05-20

| Current path | Target name | Notes |
|---|---|---|
| ~~`gac/fct_gac__sinistres_sg.sql`~~ → `services_generaux/fct_services_generaux__sinistre.sql` | `fct_services_generaux__sinistre` | ✅ fully migrated. PR #78 merged. Leftover `alias='fct_gac__sinistres_sg'` removed during migration. `.pbix` repointed by DA. Old BQ tables dropped (dev + prod). Row count + aggregate parity verified (237 lignes, sum cout_global 617 136 €). |

### → `supply_chain/` (3) ✅ DONE 2026-05-20

Three stock-related facts here. Source suffix kept to disambiguate them (rule §0 last row).

| Current path | Target name | Notes |
|---|---|---|
| ~~`oracle_neshu_gcs/fct_oracle_neshu_gcs__stock_products.sql`~~ → `supply_chain/fct_supply_chain__stock_neshu.sql` | `fct_supply_chain__stock_neshu` | ✅ migrated. PR #79 merged. Parity check OK (416 785 lignes). |
| ~~`yuman_gcs/fct_yuman_gcs__stock_articles.sql`~~ → `supply_chain/fct_supply_chain__stock_yuman.sql` | `fct_supply_chain__stock_yuman` | ✅ migrated. PR #79 merged. Parity check OK (980 471 lignes). YAML column name corrected `nom_du_stock` → `stock` (pre-existing drift). |
| ~~`oracle_neshu/fct_oracle_neshu__supply_flux.sql`~~ → `supply_chain/fct_supply_chain__flux_neshu.sql` | `fct_supply_chain__flux_neshu` | ✅ migrated. PR #79 merged. Parity check OK (18 mois). |

All 3 `.pbix` repointed by DA, old BQ tables dropped (dev + prod).

### → DELETE — none (decisions reversed 2026-05-21)

Initialement 2 fichiers étaient marqués à supprimer, finalement conservés lors de la PR `technique/` :
- `fct_yuman__suivi_partenaires` → migré vers `fct_technique__suivi_partenaire` (gardé même si non consommé aujourd'hui, pourrait servir plus tard)
- `dim_yuman__materials_clients` → migré vers `dim_technique__parc_machine` (OBT préservée car consommée par un BI ; le renaming en métier "parc machine" clarifie son usage)

L'OBT reste un anti-pattern à surveiller (cf. §5) — règle de promotion à un vrai mart inchangée.

### → External sources (not dbt models — 2) ✅ migrated 2026-05-21

Tables in `prod_marts` written directly by Cloud Run jobs (Oracle SQL → BQ → PBI refresh, ×8 / day). Declared in dbt as `source()` in `_<bu>__marts_sources.yml`.

| Current table | Target table name | dbt source declaration |
|---|---|---|
| ~~`prod_marts.fct_oracle_neshu__monitoring_passages_appro`~~ → `prod_marts.fct_neshu__monitoring_passage_appro` | `fct_neshu__monitoring_passage_appro` | `models/marts/neshu/_neshu__marts_sources.yml` (source: `marts_neshu_external`) |
| ~~`prod_marts.fct_oracle_lcdp__monitoring_passages_appro`~~ → `prod_marts.fct_lcdp__monitoring_passage_appro` | `fct_lcdp__monitoring_passage_appro` | `models/marts/lcdp/_lcdp__marts_sources.yml` (source: `marts_lcdp_external`) |

**Migration done in a dedicated PR** (separate from the main neshu/lcdp marts PRs):
1. Cloud Run repo (`/mnt/data/extract_load/ingest_oracle_passages_appro`): `TABLE` constant updated in `neshu_to_bq.py` + `lcdp_to_bq.py` + README. Direct master commit (solo repo).
2. dbt repo: source YAMLs moved to `marts/neshu/` and `marts/lcdp/`, identifiers renamed (`marts_<old>_external` → `marts_<bu>_external`), exposures updated.
3. DA repointed both `.pbix` (PROD APPRO MONITORING Neshu + DEV APPRO MONITORING LCDP).
4. Old BQ tables dropped.

---

## 2. Decisions log

### 2026-05-19
- Yuman dims and `workorder_pricing` → `technique/` (Yuman is transverse for all non-Nespresso partners).
- `fct_technique__suivi_partenaire` → **delete**.
- `fct_yuman__workorder_delais_neshu` → `neshu/`.
- `fct_oracle_neshu__supply_flux` → `supply_chain/` (BI in Supply Chain workspace).
- `gac` BU confirmed: **Services Généraux**.
- LCDP has no fact today — expected, DA will build them later.
- Source suffix in mart name only when needed to disambiguate inside a folder (currently only `supply_chain/` stocks).
- Mart naming convention documented in `CONVENTIONS.md` § Nommage des marts.

### 2026-05-20 (BigQuery audit)
- `fct_oracle_neshu__pa_business_review` → `fct_neshu__passage_appro` (PA = Passage Appro, confirmed via BQ table description).
- `fct_oracle_neshu__chargement_vs_conso` → **pending DA**: may belong to `intermediate/` rather than marts (BQ description says "Table intermédiaire").
- `dim_technique__parc_machine` → **delete** (OBT anti-pattern, see §5).
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
| Deleted | 2 (`fct_technique__suivi_partenaire`, `dim_technique__parc_machine`) | — |
| **Grand total** | **32 dbt + 2 external = 34 BQ tables** | |

---

## 5. Architecture decision (2026-05-20) — flatten in dbt vs flatten in PBI

Triggered by the review of `dim_technique__parc_machine` (OBT-style dim aggregating material + client + site attrs into one table, 9500 rows). Three options were debated:

| Option | Where the flatten logic lives | Verdict |
|---|---|---|
| 1. Flatten in dbt (current `dim_technique__parc_machine`) | dbt | **Rejected** — OBT anti-pattern: conflates 3 distinct entities (material, client, site), duplicates storage, breaks conformed dimensions principle |
| 2. Star schema in dbt + PBI relationships | PBI semantic model | Valid but constrains the DA's data model |
| 3. **Star schema in dbt + custom SQL in Power Query when needed** | PBI Power Query (per report) | **Chosen as default** — clean dbt, DA flexibility |

**Rule adopted:**

- **Default** = star schema in dbt with separate conformed dimensions (`dim_technique__client`, `dim_technique__site`, `dim_technique__material`). DA can write a SQL custom query in Power Query / Power BI to flatten when a specific report needs it.
- **Promotion to mart** = if the same flatten is duplicated across 3+ reports, or contains non-trivial logic (filtering rules, derived statuses), promote it back to a dedicated dbt mart (e.g. `fct_neshu__machine_park_enriched`). This brings the logic back into version control + tests + lineage.

**Tradeoff accepted:** logic in Power Query is not versioned in git, not tested by `dbt test`, and invisible to `dbt ls` lineage. The promotion rule mitigates this for recurring logic.

**Kimball clarification (for the record):** the previous CONVENTIONS.md guidance "aplatir les attributs des dims parentes dans la dim enfant" only applies to **display attributes of a natural parent** (ex. `company_name` in `dim_device` for tooltips, 1-3 columns max). It does NOT mean flattening entire parent dimensions into child dims — that's OBT.
