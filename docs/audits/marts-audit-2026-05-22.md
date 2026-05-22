# Audit marts — 2026-05-22

Scope : 5 BUs migrées (`finance`, `services_generaux`, `supply_chain`, `neshu`, `lcdp`).
Référence : `CONVENTIONS.md` § Marts — pattern complet (4 piliers : description 4 blocs, tests minimum, config hygiène, star schema).

> **MAJ 2026-05-22 (post-PR description trame)** — branche `feature/marts-description-trame-4-blocs` : **18/18 descriptions reformatées** au standard `[QUOI MÉTIER] / [COMMENT CONSTRUITE] / [GRAIN] / [NOTES]`. Mentions BI laissées en place (à retirer dans une PR séparée).

## Synthèse exécutive

| Pilier | État global | Détail |
|---|---|---|
| 1. Description trame 4 blocs | ✅ 18/18 conformes (était 0/18) | finance, services_generaux, supply_chain (3), neshu (12), lcdp (3). |
| 2. Tests obligatoires | ⚠️ 14/18 OK sur PK | PK dims/facts globalement testées. **FK relationships manquantes** sur ~60 % des facts. |
| 3. Tests recommandés | ❌ Quasi-absents | `accepted_values`, `row_count_between`, `unique_combination_of_columns`, `expression_is_true` : 3 marts conformes sur 18. |
| 4. Config hygiène | ❌ 14/18 violent la règle | `description=` présent dans `{{ config() }}` de 14 marts (interdit en marts depuis refacto — la description vit en YAML). Aucun `tags=[...]` constaté ✅. |
| 5. Star schema | ✅ Globalement OK | Pas de fact-à-fact ni snowflake détecté. Cas à vérifier : `dim_neshu__vehicule_roadman` vs `dim_neshu__resource` (chevauchement périmètre). |
| 6. Référence à un nom de rapport BI dans description | ⚠️ 3 cas | `fct_neshu__consommation`, `fct_neshu__chargement_consommation`, `fct_neshu__chargement_quinzaine` mentionnent "Business Review", "BI taux d'écoulement", etc. — à déplacer dans `models/exposures/neshu.yml`. |

---

## finance/

### fct_finance__pnl_bu
- **Description trame** : ✅ (MAJ 2026-05-22)
- **Description trame (avant MAJ)** : ⚠️ 2 phrases, pas de [GRAIN] explicite. Grain implicite = (scenario, annee, mois, bu, kpi).
- **Tests obligatoires** : ⚠️ `not_null` OK sur scenario/annee/mois/bu/kpi. Pas de FK (table standalone) → OK.
- **Tests recommandés** : ❌ Manque `accepted_values` sur `scenario` (`AVEC_PROVISIONS_CP`, `SANS_PROVISIONS_CP`), `unique_combination_of_columns(scenario, annee, mois, bu, kpi)`, `expect_table_row_count_to_be_between`. `accepted_values` sur `kpi` ✅.
- **Config hygiène** : ❌ `description='PnL complet avec réel, budget, YTD, N-1 et écarts'` dans le config block — à supprimer.
- **Structure** : ✅ standalone fact, pas de FK donc pas de star schema applicable.
- **Suggestion** :
  - Ajouter trame YAML : `[QUOI MÉTIER] P&L mensuel par BU. [COMMENT CONSTRUITE] Issu de int_sage__pnl + scenarios CP. [GRAIN] 1 ligne par (scenario, annee, mois, bu, kpi). [NOTES] Inclut YTD, N-1, écarts, % CA.`
  - Tests : `unique_combination_of_columns(scenario, annee, mois, bu, kpi)`, `accepted_values` sur `scenario`.
  - Supprimer `description=` du config.

---

## services_generaux/

### fct_services_generaux__sinistre
- **Description trame** : ✅ (MAJ 2026-05-22)
- **Description trame (avant MAJ)** : ❌ 1 ligne unique, pas de blocs, pas de [GRAIN].
- **Tests obligatoires** : ⚠️ `unique` sur `n_de_sinistre` et `reference_gac` mais **pas de `not_null`** (un sinistre sans numéro casserait silencieusement). Aucune FK déclarée (table standalone).
- **Tests recommandés** : ❌ Manque `accepted_values` sur `resp`, `cloture`, `statut_actuel`, `genre_fiscal`, `tiers`. Manque `row_count_between`. Pas de bornes sur `cout_global`, `franchise` (>= 0).
- **Config hygiène** : ✅ propre.
- **Structure** : ✅ pas d'OBT, mais colonnes `dbt_updated_at` / `dbt_invocation_id` exposées : pratique discutable (à valider, plutôt côté metadata interne).
- **Suggestion** :
  - Ajouter trame avec `[GRAIN] 1 ligne par sinistre (n_de_sinistre)`.
  - Ajouter `not_null` sur PK ; `accepted_values` sur `cloture`/`statut_actuel` (explorer prod_marts pour bornes réelles).

---

## supply_chain/

### fct_supply_chain__stock_neshu
- **Description trame** : ✅ (MAJ 2026-05-22)
- **Description trame (avant MAJ)** : ⚠️ 1 phrase, pas de blocs, pas de [GRAIN]. Grain implicite : (id_entity, product_code, date_system).
- **Tests obligatoires** : ⚠️ `not_null` OK sur id_entity, date_system, product_code, file_datetime. Pas de FK déclarée (source externe Cloud Run pré-dim).
- **Tests recommandés** : ❌ Manque `accepted_values` sur `entity_type` (depot/vehicule), `is_out_of_stock`. Pas de `unique_combination_of_columns`, ni `row_count_between`, ni `>=0` sur `stock_at_date`/`stock_inventaire`/`dpa`/`purchase_price`.
- **Config hygiène** : ❌ `description=` dans le config — à supprimer.
- **Suggestion** : ajouter trame + `unique_combination_of_columns(id_entity, product_code, date_system)` + bornes >=0 sur stocks.

### fct_supply_chain__stock_yuman
- **Description trame** : ✅ (MAJ 2026-05-22)
- **Description trame (avant MAJ)** : ❌ 1 ligne, pas de [GRAIN].
- **Tests obligatoires** : ⚠️ `not_null` sur `reference` et `stock_date`. Pas de FK.
- **Tests recommandés** : ❌ tout manque (accepted_values sur `stock` si liste finie de dépôts/techniciens, row_count, >=0 sur `quantite`).
- **Config hygiène** : ❌ `description=` à supprimer.
- **Suggestion** : ajouter trame `[GRAIN] 1 ligne par (reference, stock, stock_date)` + `unique_combination_of_columns` + `quantite >= 0`.

### fct_supply_chain__flux_neshu
- **Description trame** : ✅ (MAJ 2026-05-22)
- **Description trame (avant MAJ)** : ⚠️ description riche multi-bloc mais sans tags `[QUOI MÉTIER] / [GRAIN]`. Grain mois implicite mentionné en prose.
- **Tests obligatoires** : ✅ `not_null` + `unique` sur `mois_date`, `not_null` sur `annee`/`mois`.
- **Tests recommandés** : ✅ 3 `expression_is_true` au niveau model (`stock_total = stock_depot + stock_vehicule`, non-négatifs). ⚠️ Manque `row_count_between`, `expect_column_values_to_be_between` sur `mois_date` (plage attendue).
- **Config hygiène** : ✅ propre.
- **Suggestion** : reformater description avec les balises canoniques. Ajouter borne de `mois_date` (`'2020-01-01'` → `current_date()`).

---

## neshu/

### dim_neshu__company
- **Description trame** : ✅ (MAJ 2026-05-22)
- **Description trame (avant MAJ)** : ❌ 1 paragraphe, pas de blocs, pas de [GRAIN].
- **Tests obligatoires** : ✅ `not_null` + `unique` sur `company_id`.
- **Tests recommandés** : ❌ Manque `accepted_values` sur `sector` (HORECA/OFFICE), `client_status` (OR/ARGENT/BRONZE), `is_active` (true/false). Pas de `row_count_between`.
- **Config hygiène** : ❌ `description=` à supprimer.
- **Suggestion** : ajouter trame `[GRAIN] 1 ligne par company_id` + accepted_values sur les ~6 colonnes labels bornées.

### dim_neshu__product
- **Description trame** : ✅ (MAJ 2026-05-22)
- **Description trame (avant MAJ)** : ❌ 1 paragraphe, pas de [GRAIN].
- **Tests obligatoires** : ✅ `not_null` + `unique` sur `product_id`.
- **Tests recommandés** : ❌ Manque `accepted_values` sur `product_type` (9 valeurs définies dans `fct_neshu__consommation` — réutiliser la même liste), `is_active`. Pas de row_count.
- **Config hygiène** : ❌ `description=` à supprimer.
- **Suggestion** : ajouter trame + `accepted_values` cohérent avec `fct_neshu__consommation`.

### dim_neshu__device
- **Description trame** : ✅ (MAJ 2026-05-22)
- **Description trame (avant MAJ)** : ❌ 1 paragraphe, pas de [GRAIN].
- **Tests obligatoires** : ✅ PK testée ; FK `company_id` avec `not_null` + `relationships` vers `dim_neshu__company` ✅ (exemplaire).
- **Tests recommandés** : ❌ Manque `accepted_values` sur `device_brand` (NESPRESSO/NESTLE/ANIMO), `device_economic_model`, `is_active`. Pas de row_count.
- **Config hygiène** : ❌ `description=` à supprimer.
- **Suggestion** : ajouter trame + accepted_values + reproduire le pattern `not_null + relationships` ailleurs.

### dim_neshu__contract
- **Description trame** : ✅ (MAJ 2026-05-22)
- **Description trame (avant MAJ)** : ⚠️ 2 phrases, grain mentionné en prose ("un seul contrat actif par client") mais pas via balise [GRAIN].
- **Tests obligatoires** : ✅ `unique` + `not_null` sur `contract_id`. ⚠️ `company_id` testé via `unique_combination_of_columns` warn — bon, mais idéalement `relationships` vers `dim_neshu__company` aussi.
- **Tests recommandés** : ⚠️ `unique_combination_of_columns(company_id)` ✅. Manque `accepted_values` sur `is_active`. Pas de bornes sur `nombre_collab` (>= 0), `engagement_clean`.
- **Config hygiène** : ❌ doublon : config dans le YAML (`config: materialized + cluster_by`) **et** dans le SQL (`{{ config(materialized=..., description=...) }}`). Supprimer le `description=` SQL + consolider config (préférer un seul endroit, idéalement SQL).
- **Suggestion** : trancher l'endroit canonique du config block (SQL uniquement), ajouter trame.

### dim_neshu__vehicule_roadman
- **Description trame** : ✅ (MAJ 2026-05-22)
- **Description trame (avant MAJ)** : ❌ 1 phrase, pas de [GRAIN].
- **Tests obligatoires** : ✅ `not_null` + `unique` sur `resources_vehicule_id`. `unique` sur `vehicule_code` ✅. `unique` sur `roadman_code` (sans not_null — un véhicule sans roadman?).
- **Tests recommandés** : ❌ tout manque.
- **Config hygiène** : ❌ `description=` à supprimer.
- **Structure** : ⚠️ **Chevauchement avec `dim_neshu__resource`** qui couvre déjà PERSON + VEHICULE. À clarifier : pourquoi 2 dims ressources ? Risque de snowflake/doublon de référentiel.
- **Suggestion** : ajouter trame + statuer sur fusion/dépréciation vs `dim_neshu__resource`.

### dim_neshu__resource
- **Description trame** : ✅ (MAJ 2026-05-22)
- **Description trame (avant MAJ)** : ⚠️ description riche (sources, hiérarchie, notes BI) mais pas de balises canoniques, pas de [GRAIN].
- **Tests obligatoires** : ✅ `not_null` + `unique` sur `resources_id`. `not_null` sur `resources_code`, `resources_type`, `is_active` ✅.
- **Tests recommandés** : ❌ Manque `accepted_values` sur `resources_type` (PERSON, VEHICULE), `code_status_record` (1/autre), `is_active`. Pas de row_count.
- **Config hygiène** : ❌ `description=` à supprimer.
- **Suggestion** : ajouter trame + accepted_values sur resources_type.

### fct_neshu__consommation
- **Description trame** : ✅ (MAJ 2026-05-22)
- **Description trame (avant MAJ)** : ❌ multi-paragraphe sans balises ; **mentionne "Business Review Neshu"** → à déplacer dans exposures.
- **Tests obligatoires** : ✅ `not_null` + `relationships` sur company_id, product_id ; `relationships` sur device_id (nullable OK pour LIVRAISON). `not_null` sur `consumption_date`.
- **Tests recommandés** : ✅ Exemplaire — `unique_combination_of_columns`, `expression_is_true` (device_id NULL ⇒ LIVRAISON), `accepted_values` sur product_type/data_source, `accepted_range` sur consumption_date. ⚠️ Pas de `row_count_between`, pas de borne `quantity >= 0`.
- **Config hygiène** : ❌ `description=` dans config + mention BI ("BR Neshu") — à supprimer.
- **Suggestion** : modèle de référence pour les autres facts. Ajouter trame + retirer description= + `quantity >= 0`.

### fct_neshu__chargement_consommation
- **Description trame** : ✅ (MAJ 2026-05-22) — trame canonique appliquée.
- **Description trame (avant MAJ)** : ✅ mentionnait `Grain : (device, passage_appro, product)` en prose — meilleur que les autres mais sans balise.
- **Tests obligatoires** : ⚠️ `not_null` sur device_id, date_debut_passage_appro, product_type, product_code. **Manque `relationships`** sur device_id → `dim_neshu__device`, product_code n'est pas un FK direct.
- **Tests recommandés** : ❌ Pas de `unique_combination_of_columns(device_id, date_debut_passage_appro, product_code)`. Pas de `>= 0` sur `q_consommee`/`q_chargee`. Pas de row_count.
- **Config hygiène** : ❌ `description=` + mention BI ("BI de taux d ecoulement et Suivi des chargements machines gratuités") — à déplacer dans exposures.
- **Suggestion** : ajouter relationships sur device_id, unique_combination, bornes >=0.

### fct_neshu__chargement_quinzaine
- **Description trame** : ✅ (MAJ 2026-05-22)
- **Description trame (avant MAJ)** : ⚠️ 2 lignes, pas de [GRAIN].
- **Tests obligatoires** : ⚠️ `not_null` sur product_type/company_code/annee_chgt/quinzaine_chgt. Pas de FK (company_code au lieu de company_id — entorse aux conventions des facts).
- **Tests recommandés** : ❌ `unique_combination_of_columns(product_type, company_code, annee_chgt, quinzaine_chgt)` manquant. `quinzaine_chgt` entre 1 et 27 ? `quantite_chargee >= 0` ?
- **Config hygiène** : ❌ `description=` + mention BI — à déplacer.
- **Suggestion** : remplacer `company_code` par `company_id` (FK propre) si possible, sinon ajouter `relationships` via clé naturelle. Ajouter unique_combination + bornes.

### fct_neshu__appro
- **Description trame** : ✅ (MAJ 2026-05-22)
- **Description trame (avant MAJ)** : ⚠️ multi-paragraphe descriptive, pas de [GRAIN] explicite (grain = task_id).
- **Tests obligatoires** : ✅ `not_null` + `unique` sur `task_id`, `not_null` sur `resources_roadman_id`. ❌ **Manque `relationships`** : device_id → `dim_neshu__device`, company_id → `dim_neshu__company`, resources_roadman_id → `dim_neshu__resource`.
- **Tests recommandés** : ❌ Pas d'`accepted_values` sur `task_status_code` (PLANIFIE/FAIT/ENCOURS reclassé). Pas de bornes sur durées (`passage_duration_min >= 0`, `work_duration_min between 0 and 720`). Pas de row_count.
- **Config hygiène** : ❌ `description=` à supprimer.
- **Suggestion** : prioriser ajout des 3 `relationships` (vraie valeur business). Ajouter bornes durées et accepted_values task_status_code.

### fct_neshu__maintenance_preventive
- **Description trame** : ✅ (MAJ 2026-05-22)
- **Description trame (avant MAJ)** : ⚠️ très riche (règles métier, sources), pas de balises canoniques, pas de [GRAIN] explicite (1 par device).
- **Tests obligatoires** : ✅ Exemplaire — `not_null` + `unique` + `relationships` sur device_id ; `not_null` sur device_code/device_name/company_code/company_name/device_last_installation_date/retard_bol/retard_delai/source_last_preventive/status_inter ; `relationships` sur material_id (warn, nullable) ✅.
- **Tests recommandés** : ✅ `accepted_values` sur `source_last_preventive` et `status_inter` ✅. ❌ Manque `row_count_between` et bornes `retard_delai`.
- **Config hygiène** : ❌ `description=` à supprimer.
- **Note structure** : `unique` sur device_id → c'est en fait une dim enrichie (1 ligne par machine) plus qu'un fact transactionnel. Naming `fct_` discutable mais acceptable si interprété comme "état observé à date".
- **Suggestion** : modèle de référence pour les tests. Retirer description= + ajouter row_count + bornes retard.

### fct_neshu__workorder_delai
- **Description trame** : ✅ (MAJ 2026-05-22)
- **Description trame (avant MAJ)** : ❌ 1 paragraphe, pas de [GRAIN].
- **Tests obligatoires** : ⚠️ `unique` sur demand_id et workorder_id (sans not_null), `relationships` sur material_id/site_id/client_id ✅. ❌ **Manque `not_null`** sur les FK.
- **Tests recommandés** : ❌ Manque accepted_values sur `workorder_type_clean`, `pricing_type` (Tarif normal/Remise niv1/Remise niv2), `billing_validation_status` (VALIDATED/MISSING_TARIF/NOT_BILLABLE), `to_invoice`. Pas de `>= 0` sur `amount`, `delai_jours_ouvres`, `recurrence_count`. Pas de row_count.
- **Config hygiène** : ✅ propre (seul mart neshu sans `description=` en config !).
- **Suggestion** : ajouter trame + not_null sur FK + accepted_values + bornes.

---

## lcdp/

### dim_lcdp__company
- **Description trame** : ✅ (MAJ 2026-05-22)
- **Description trame (avant MAJ)** : ❌ 1 paragraphe, pas de [GRAIN].
- **Tests obligatoires** : ✅ `not_null` + `unique` sur `company_id`.
- **Tests recommandés** : ❌ Pas d'`accepted_values` sur is_active, geo_zone, activity_domain, business_model, key_account. Pas de row_count.
- **Config hygiène** : ❌ `description=` à supprimer.
- **Suggestion** : trame + accepted_values bornés sur labels.

### dim_lcdp__product
- **Description trame** : ✅ (MAJ 2026-05-22)
- **Description trame (avant MAJ)** : ❌ 1 paragraphe, pas de [GRAIN].
- **Tests obligatoires** : ✅ `not_null` + `unique` sur `product_id`.
- **Tests recommandés** : ❌ tout manque.
- **Config hygiène** : ❌ `description=` à supprimer.
- **Suggestion** : trame + accepted_values sur product_family/product_group/is_active.

### dim_lcdp__device
- **Description trame** : ✅ (MAJ 2026-05-22)
- **Description trame (avant MAJ)** : ❌ 1 paragraphe, pas de [GRAIN].
- **Tests obligatoires** : ⚠️ `not_null` + `unique` sur `device_id` ✅. ❌ **Manque `relationships`** sur company_id → `dim_lcdp__company` (présent dans dim_neshu__device, à reproduire ici).
- **Tests recommandés** : ❌ Pas d'`accepted_values` sur device_state, device_material_status, audit_type, currency_mode, is_active, etc. Pas de row_count.
- **Config hygiène** : ❌ `description=` à supprimer.
- **Suggestion** : reproduire le pattern de `dim_neshu__device` (relationships sur company_id) + accepted_values sur labels.

---

## Recommandations priorisées pour PRs séparées

1. **PR "config hygiène"** (cosmétique, faible risque, gros impact lisibilité)
   - Supprimer `description=` du `{{ config() }}` dans 14 marts. La description en YAML reste.
   - Consolider `dim_neshu__contract` : config en SQL uniquement (retirer le bloc `config:` du YAML).

2. **PR "description trame 4 blocs"** (cosmétique)
   - Reformater les 18 descriptions YAML avec `[QUOI MÉTIER] / [COMMENT CONSTRUITE] / [GRAIN] / [NOTES]`.
   - Retirer les mentions de rapports BI dans `fct_neshu__consommation`, `fct_neshu__chargement_consommation`, `fct_neshu__chargement_quinzaine`.

3. **PR "tests FK relationships"** (réelle valeur business)
   - Ajouter `not_null + relationships` sur les FK manquantes : `fct_neshu__appro` (device_id, company_id, resources_roadman_id), `fct_neshu__chargement_consommation` (device_id), `fct_neshu__workorder_delai` (not_null sur FK), `dim_lcdp__device` (company_id).

4. **PR "tests accepted_values + bornes"** (qualité métier — explorer prod via BQ MCP avant de figer les listes)
   - Listes `accepted_values` sur statuts, types, modèles économiques, etc.
   - Bornes `>= 0` sur quantités, durées, montants.
   - `unique_combination_of_columns` sur clés composites des facts.

5. **PR "row_count + plages dates"** (warn-only, garde-fou ordre de grandeur)
   - `expect_table_row_count_to_be_between` et `expect_column_values_to_be_between` (dates) — bornes à calibrer via BQ MCP.

6. **PR séparée à statuer** : `dim_neshu__vehicule_roadman` vs `dim_neshu__resource` — clarifier le périmètre et déprécier l'un des deux si redondant.
