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
| `oracle_neshu/fct_oracle_neshu__chargement_vs_conso.sql` | `fct_neshu__chargement_vs_consommation` | ⚠️ comparison view — could become an intermediate or a derived BI view; keep for now |
| `oracle_neshu/fct_oracle_neshu__conso_business_review.sql` | `fct_neshu__consommation` | BI name moves to exposure |
| `oracle_neshu/fct_oracle_neshu__pa_business_review.sql` | `fct_neshu__pa` | ⚠️ confirm what "pa" means (prix d'achat ? plan d'appro ?) — rename more explicit if possible |
| `technique/fct_technique__neshu_maintenance_preventives.sql` | `fct_neshu__maintenance_preventive` | partner-specific → moves out of `technique/`, exposed by `maintenance_preventives` |
| `yuman/fct_yuman__workorder_delais_neshu.sql` | `fct_neshu__workorder_delai` | ⚠️ Neshu-specific yuman fact moves to neshu folder |

### → `lcdp/` (3)

| Current path | Target name | Notes |
|---|---|---|
| `oracle_lcdp/dim_oracle_lcdp__company.sql` | `dim_lcdp__company` | exposed by `passage_appro_monitoring_lcdp` |
| `oracle_lcdp/dim_oracle_lcdp__device.sql` | `dim_lcdp__device` | |
| `oracle_lcdp/dim_oracle_lcdp__product.sql` | `dim_lcdp__product` | |

> No fact today — facts to be built by DA later.

### → `technique/` (9)

| Current path | Target name | Notes |
|---|---|---|
| `yuman/dim_yuman__clients.sql` | `dim_technique__client` | singular |
| `yuman/dim_yuman__sites.sql` | `dim_technique__site` | singular |
| `yuman/dim_yuman__materials.sql` | `dim_technique__material` | singular |
| `yuman/dim_yuman__materials_clients.sql` | `dim_technique__material_client` | bridge (kept as dim until grain reviewed) |
| `yuman/dim_yuman__technicians.sql` | `dim_technique__technician` | singular |
| `technique/fct_technique__interventions.sql` | `fct_technique__intervention` | singular |
| `yuman/fct_yuman__workorder_pricing.sql` | `fct_technique__workorder_pricing` | transverse (ad-hoc reporting for intervention pricing) |
| `nesp_tech/fct_nesp_tech__alerting_conso_pieces_aguila.sql` | `fct_technique__alerting_consommation_piece_aguila` | ⚠️ long name; OK |
| `nesp_tech/fct_nesp_tech__pieces_detachees_pricing.sql` | `fct_technique__piece_detachee_pricing` | |

### → `commerce/` (1)

| Current path | Target name | Notes |
|---|---|---|
| `commerce/fct_commerce__machines_avec_interventions.sql` | `fct_commerce__machine_intervention` | singular, drop `_avec_` connector |

### → `finance/` (1)

| Current path | Target name | Notes |
|---|---|---|
| `mssql_sage/fct_mssql_sage__pnl_bu_kpis.sql` | `fct_finance__pnl_bu` | `_kpis` redundant |

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

### → DELETE (1)

| Current path | Reason |
|---|---|
| `yuman/fct_yuman__suivi_partenaires.sql` | not consumed today, confirmed by DE |

---

## 2. Decisions log (2026-05-19)

- Yuman dims and `workorder_pricing` → `technique/` (Yuman is transverse for all non-Nespresso partners).
- `fct_yuman__suivi_partenaires` → **delete**.
- `fct_yuman__workorder_delais_neshu` → `neshu/`.
- `fct_oracle_neshu__supply_flux` → `supply_chain/` (BI in Supply Chain workspace).
- `gac` BU confirmed: **Services Généraux**.
- LCDP has no fact today — expected, DA will build them later.
- Source suffix in mart name only when needed to disambiguate inside a folder (currently only `supply_chain/` stocks).
- Mart naming convention documented in §0 — to be formalized in `CONVENTIONS.md` during the refacto.

---

## 3. Remaining items to address during Phase 2

1. **Exposures backfill** — DE only declared the 5 exposures for the BI dashboards he built himself. The DA owns BI now and will be the source of truth for which mart feeds which report. **Action:** during each BU PR in Phase 2, ping DA to identify the consumers and declare them as exposures in `models/exposures/<bu>.yml`.
2. **⚠️ flagged renames** — confirm `fct_neshu__pa`, `fct_neshu__chargement_vs_consommation`, `fct_neshu__maintenance_preventive` (singular vs plural for "preventives") with DA at the time of the BU PR.
3. **`dim_technique__material_client`** — review grain (looks like a bridge table); confirm dim vs fact classification.

---

## 4. Final count

| Target | Count |
|---|---|
| `neshu/` | 13 |
| `lcdp/` | 3 |
| `technique/` | 9 |
| `commerce/` | 1 |
| `finance/` | 1 |
| `services_generaux/` | 1 |
| `supply_chain/` | 3 |
| **Migrated total** | **31** |
| Deleted | 1 |
| **Grand total** | **32** |
