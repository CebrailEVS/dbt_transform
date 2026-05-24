# Audit marts — 2026-05-24

Scope : **32 marts répartis sur 8 BUs** (refacto by BU terminée). Référence : `CONVENTIONS.md` § Marts — pattern complet (4 piliers).

> Cet audit remplace `marts-audit-2026-05-22.md`, qui ne couvrait que 5/8 BUs (avant migration de `technique` et `commerce`). Toutes les MAJ intermédiaires sont intégrées ci-dessous.

## Inventaire

| BU | Dims | Facts | Total |
|---|---|---|---|
| `commerce/` | 0 | 1 | 1 |
| `finance/` | 0 | 1 | 1 |
| `lcdp/` | 3 | 0 | 3 |
| `neshu/` | 6 | 7 | 13 |
| `services_generaux/` | 0 | 1 | 1 |
| `supply_chain/` | 0 | 3 | 3 |
| `technique/` | 5 | 5 | 10 |
| **Total** | **14** | **18** | **32** |

## Synthèse exécutive

| Pilier | Couverture | Détail |
|---|---|---|
| 1. **Description trame 4 blocs** | **31/32 ✅** | Manque : `dim_technique__site` (pas de `[NOTES]`) |
| 2. **Tests obligatoires** | **Dims 14/14 ✅** · **Facts 8/18 ⚠️** | Tests `relationships` présents sur 8 facts neshu+technique. **10 facts sans FK relationships** (standalone facts + supply_chain + technique pricing + commerce + intervention) |
| 3. **Config hygiène** | **32/32 ✅** | Aucun `description=` dans les `{{ config() }}`. Aucun `tags=[...]` model-level constaté. |
| 4. **Star schema (no fact-to-fact)** | **16/18 ✅** | 2 violations : `fct_neshu__workorder_delai` → `fct_technique__workorder_pricing` ; `fct_technique__intervention` → `fct_neshu__workorder_delai` |
| 5. **Flatten parent direct (1-3 cols max)** | ⚠️ | `dim_technique__parc_machine` aplatit `dim_technique__client` (4 cols) + `dim_technique__site` (4 cols) — pas dans le périmètre 1-3 cols max |

## Anomalies prod détectées via MCP BigQuery (2026-05-24)

| Item | Constat | Décision |
|---|---|---|
| `dim_neshu__resource.location_id` | **100 % NULL** (218/218) — colonne morte | À supprimer (déjà flaggée 2026-05-22, non corrigée) |
| `dim_neshu__company.company_code` | 1 doublon (EVS auto-référencée) | Non bloquant — aucune jointure dessus. Maintenir tel quel. |
| `fct_neshu__appro.device_id` | 1 NULL sur 540 872 (task_id 11460408) | Acceptable, garder en warn |
| `dim_technique__material` vs `dim_technique__parc_machine` | Même grain (9 544 lignes, PK `material_id` identique) | À analyser : doublon de dim ou volonté délibérée ? cf. §Star schema |

---

## Détail par BU

### `commerce/` (1 mart)

#### `fct_commerce__machine_intervention` (7 792 lignes)
- Description trame : ✅
- Tests : 1 unique + 5 not_null, **0 relationships** ❌ — fact qui croise nesp_co + nesp_tech via machine. Devrait référencer `dim_technique__material` et/ou un futur `dim_commerce__client`.
- Config hygiène : ✅
- Star schema : ✅
- **Suggestion** : ajouter `relationships` vers la dim machine partagée avec `technique/`.

---

### `finance/` (1 mart)

#### `fct_finance__pnl_bu`
- Description trame : ✅
- Tests : 0 unique, 4 not_null, **0 relationships** — standalone (pas de FK)
- Config hygiène : ✅
- Star schema : ✅ (standalone)
- **Suggestion** : ajouter `dbt_utils.unique_combination_of_columns(scenario, annee, mois, bu, kpi)` et `accepted_values` sur `scenario`.

---

### `lcdp/` (3 dims)

#### `dim_lcdp__company`, `dim_lcdp__product`
- Description trame : ✅
- Tests : 1 unique + 1 not_null sur PK ✅
- Pas de fact aval → couverture minimale acceptable.

#### `dim_lcdp__device`
- Description trame : ✅
- Tests : 1 unique + 2 not_null + 1 relationships ✅

---

### `neshu/` (13 marts) — BU la plus mature

#### Dimensions (6)
Toutes ont unique + not_null sur PK ✅. Trame description ✅.
- `dim_neshu__company`, `dim_neshu__product`, `dim_neshu__device`, `dim_neshu__contract`, `dim_neshu__resource`, `dim_neshu__vehicule_roadman` (3 uniques car table de mapping).

#### Facts (7)

| Fact | unique | not_null | relationships | Notes |
|---|---|---|---|---|
| `fct_neshu__consommation` | 0 | 11 | 3 | ✅ |
| `fct_neshu__appro` | 1 | 3 | 3 | ✅ |
| `fct_neshu__chargement_consommation` | 0 | 5 | 2 | ✅ |
| `fct_neshu__chargement_quinzaine` | 0 | 4 | **0** | ⚠️ agrégat sans FK — devrait pointer dim_company/product au minimum |
| `fct_neshu__maintenance_preventive` | 1 | 10 | 2 | ✅ |
| `fct_neshu__workorder_delai` | 2 | 0 | 3 | ✅ tests mais **fact-to-fact ❌** (ref `fct_technique__workorder_pricing` ×2) |
| `fct_neshu__machine_appro_intervention` | 1 | 7 | 5 | ✅ |

---

### `services_generaux/` (1 mart)

#### `fct_services_generaux__sinistre`
- Description trame : ✅
- Tests : 2 unique + 1 not_null, **0 relationships** — standalone
- Config hygiène : ✅
- **Suggestion** : ajouter `accepted_values` sur `statut_actuel` (Clos, A la route) et `cloture`.

---

### `supply_chain/` (3 facts)

#### `fct_supply_chain__stock_neshu`, `fct_supply_chain__stock_yuman`, `fct_supply_chain__flux_neshu`
- Description trame : ✅
- Tests : not_null OK, **0 relationships** sur les 3 ❌
- Config hygiène : ✅
- Star schema : ✅
- **Suggestion** : `stock_neshu` devrait avoir des FK vers `dim_neshu__product` / `dim_neshu__resource` (via id_entity + product_code). `stock_yuman` n'a pas de dim Yuman dédiée — laisser tel quel. `flux_neshu` à enrichir FK product/company.

---

### `technique/` (10 marts) — nouvellement migrée

#### Dimensions (5)
| Dim | unique | not_null | relationships | Trame |
|---|---|---|---|---|
| `dim_technique__client` | 1 | 1 | 0 | ✅ |
| `dim_technique__site` | 1 | 2 | 1 | ✅ — **manque `[NOTES]`** |
| `dim_technique__material` | 1 | 2 | 1 | ✅ |
| `dim_technique__parc_machine` | 1 | 3 | 0 | ✅ |
| `dim_technique__technician` | 1 | 1 | 0 | ✅ |

> ⚠️ `dim_technique__parc_machine` a la **même PK et le même grain** que `dim_technique__material` (9 544 lignes, PK `material_id`). Il aplatit en plus 8 colonnes de `dim_technique__client` + `dim_technique__site` (`client_id`, `client_code`, `client_address`, `client_name`, `partner_name`, `site_id`, `site_address`, `site_name`, `site_postal_code`). **Viole le pattern flatten 1-3 cols max** — soit assumer comme dim "wide" pour BI (et documenter l'exception), soit retirer le flatten et passer par des relationships PBI.

#### Facts (5)

| Fact | unique | not_null | relationships | Notes |
|---|---|---|---|---|
| `fct_technique__workorder_pricing` | 2 | 0 | 3 | ✅ |
| `fct_technique__suivi_partenaire` | 1 | 0 | 3 | ✅ |
| `fct_technique__intervention` (87 893) | 1 | 3 | **0** | ⚠️ devrait avoir FKs vers dim_technique__*. **Fact-to-fact ❌** (ref `fct_neshu__workorder_delai`) |
| `fct_technique__piece_detachee_pricing_nespresso` (160 277) | 0 | 3 | **0** | ⚠️ à enrichir tests |
| `fct_technique__alerting_consommation_aguila` (2 056) | 1 | 1 | **0** | ⚠️ |

---

## Violations Star schema — fact-to-fact

Deux marts cassent la règle "pas de jointure fait-à-fait" :

1. `fct_neshu__workorder_delai` → `ref('fct_technique__workorder_pricing')` ×2
2. `fct_technique__intervention` → `ref('fct_neshu__workorder_delai')`

Cela crée un cycle implicite neshu ↔ technique. À investiguer :
- Légitime (denormalisation contrôlée pour BI) ? → assumer et documenter dans `[NOTES]` YAML.
- Drift d'architecture ? → remonter la logique commune dans un intermediate partagé.

---

## Plan d'action recommandé

### Quick wins (~1 PR chacun)
1. Supprimer `dim_neshu__resource.location_id` (colonne morte 100 % NULL).
2. Ajouter `[NOTES]` sur `dim_technique__site`.
3. Ajouter `dbt_utils.unique_combination_of_columns` + `accepted_values` sur `fct_finance__pnl_bu` et `fct_services_generaux__sinistre`.
4. Ajouter `relationships` warn sur les 10 facts sans FK test (commerce, supply_chain ×3, neshu/chargement_quinzaine, technique/intervention + pricing + suivi + alerting).

### Moyennes (à arbitrer)
5. Décider du statut de `dim_technique__parc_machine` vs `dim_technique__material` : doublon assumé ou refonte ?
6. Décider du statut des 2 ref fact-to-fact (neshu/technique) : assumer + documenter, ou refacto en intermediate partagé.

### Bas (cosmétique)
7. Tests `expect_table_row_count_to_be_between` sur les facts critiques (catch row count drift).
