# Source freshness — état des lieux et conventions

> Dernière mise à jour : 2026-05-24

---

## À quoi sert ce document

Ce document **audite** la configuration `dbt source freshness` de
l'ensemble des 10 sources du projet, et fixe les conventions cibles par
tier. C'est l'outil de référence pour :
- savoir quelles sources sont monitorées et lesquelles ne le sont pas
- repérer les drifts entre tier théorique (cf. CLAUDE.md / memory) et
  config YAML réelle
- décider quoi corriger

---

## Tiers de freshness — convention cible

| Tier | Cadence typique | `warn_after` | `error_after` | Sources concernées |
|---|---|---|---|---|
| **Critique** | quotidien, business-day SLA | 26h | 36h | `oracle_neshu`, `oracle_lcdp` |
| **Standard** | quotidien tolérant (full refresh ou rythme moins serré) | 26h | 48h | `yuman`, `mssql_sage`, `nesp_co` |
| **Hebdomadaire** | extraction 1×/semaine | 8 jours | 14 jours | `nesp_tech` |
| **Relaxe** | extraction batch / journalier non critique | 7 jours | 14 jours | `gac`, `oracle_neshu_gcs`, `yuman_gcs` |
| **Non applicable** | source sans champ timestamp exploitable | — | — | `zoho_desk` |

> Le tier **Hebdomadaire** est nouveau — `nesp_tech` (cron `30 7 * * 1`)
> mérite son propre niveau plutôt que d'être catalogué *Standard* (ce qui
> alerterait à tort tous les jours).

---

## État des lieux — par source

### ✅ Configurée — `oracle_neshu`

- `loaded_at_field: _sdc_extracted_at`
- **Défaut source** : warn 26h / error 36h ✅ cohérent tier *Critique*
- **Overrides par table** :
  - 14 tables actives répètent `freshness: 26h/36h` — **redondant** avec le défaut (à nettoyer)
  - 8 référentiels stables en `freshness: null` (désactivé) : `evs_company_type`, `evs_product_type`, `evs_resources_type`, `evs_task_status`, `evs_task_type`, `evs_label`, `evs_label_family`, `evs_contract_parsed` — **intentionnel et justifié**

### ✅ Configurée — `oracle_lcdp`

Symétrique de `oracle_neshu` : mêmes seuils 26h/36h, mêmes overrides
redondants à nettoyer, mêmes référentiels en `null`.

### ✅ Configurée — `yuman`

- `loaded_at_field: _sdc_extracted_at`
- **Défaut source** : warn 26h / error 36h
- **Overrides par table** : 5 tables référentielles stables en `freshness: null` (`yuman_products`, `yuman_material_categories`, `yuman_workorder_categories`, `yuman_workorder_demands_categories`, `yuman_storehouses`) — **intentionnel**

> Seuil error actuel **36h** alors que tier cible *Standard* est **48h**.
> À aligner (passer error à 48h) — l'API Yuman est moins critique qu'Oracle,
> 12h de marge supplémentaire évite des alertes inutiles.

### ✅ Configurée — `mssql_sage`

- `loaded_at_field: _sdc_extracted_at` au défaut
- **Override** sur `dbo_f_ecriturea` et `dbo_f_ecriturec` :
  `loaded_at_field: _sdc_received_at`
- **Défaut source** : warn 26h / error 36h

> Deux questions :
> 1. **Le tier cible *Standard* attend `error_after: 48h`** — la conf actuelle
>    (36h) est plus stricte. À aligner ?
> 2. **Pourquoi `_sdc_received_at` sur deux tables seulement ?** Si c'est un
>    drift, harmoniser sur `_sdc_extracted_at`. Si c'est intentionnel (les
>    écritures comptables seraient timestampées différemment), documenter le
>    pourquoi dans la YAML.

### ⚠️ Désactivée explicitement — `gac`

- `loaded_at_field: _sdc_batched_at`
- `freshness: null` (désactivé)

> Tier cible *Relaxe* (7j/14j). La désactivation totale est défendable (240
> lignes, fichier livré ponctuellement), mais on **perd toute visibilité**
> si l'export GAC s'arrête. **Proposition** : activer 7j/14j plutôt que
> `null`.

### ❌ Non configurée — `nesp_tech`

- Pas de `loaded_at_field`, pas de bloc `freshness`. `dbt source freshness`
  **ignore** cette source.

> Pipeline hebdomadaire (lundi 07:30). **Proposition** :
> ```yaml
> loaded_at_field: _sdc_extracted_at   # ou cast(extracted_at as timestamp)
> freshness:
>   warn_after: {count: 8, period: day}
>   error_after: {count: 14, period: day}
> ```
> Tier *Hebdomadaire* à créer formellement.

### ❌ Non configurée — `nesp_co`

- Pas de `loaded_at_field`, pas de bloc `freshness`.

> Pipeline quotidien (08:00) pour `activite` + `opportunite`. **Proposition** :
> tier *Standard* (26h/48h) sur ces deux tables. Pour `nespresso_base_client`
> (déclenchement **manuel**), garder `freshness: null` ou tier *Relaxe*
> très large (30j ?) — à débattre.

### ❌ Non configurée — `oracle_neshu_gcs`

- Aucun bloc `freshness` ni `loaded_at_field`. External table pointant sur
  un bucket GCS.

> Pipeline quotidien (23:00). **Proposition** : tier *Relaxe* (7j/14j) sur
> `extracted_at` (colonne déjà présente après staging).

### ❌ Non configurée — `yuman_gcs`

- Aucun bloc `freshness` ni `loaded_at_field`.

> Pipeline Mon-Sat 06:00. **Proposition** : tier *Relaxe* (7j/14j) sur
> `export_date` ou `_sdc_received_at` selon ce qui est exposé.

### ⛔ Non applicable — `zoho_desk`

- `loaded_at_field` documenté en commentaire : `_dlt_load_id` est une
  **STRING** (unix timestamp en texte) issue de `dlt`, donc
  `dbt source freshness` ne peut pas l'utiliser.

> **Workaround possible** : exposer un champ calculé
> `cast(_dlt_load_id as int64) → timestamp_seconds(...)` dans la source ou
> dans une vue technique. À évaluer selon l'effort vs. l'utilité (Zoho est
> une source secondaire aujourd'hui).

---

## Synthèse — tableau de couverture

| Source | Tier cible | `loaded_at_field` | Config actuelle | État | Action |
|---|---|---|---|---|---|
| `oracle_neshu` | Critique | `_sdc_extracted_at` | 26h/36h + 8 nulls référentiels | ✅ | Nettoyer overrides redondants |
| `oracle_lcdp` | Critique | `_sdc_extracted_at` | 26h/36h + nulls référentiels | ✅ | Idem |
| `yuman` | Standard | `_sdc_extracted_at` | 26h/**36h** + 5 nulls référentiels | ⚠️ | Passer error 36h → **48h** |
| `mssql_sage` | Standard | `_sdc_extracted_at` (+ 2 overrides `_sdc_received_at`) | 26h/**36h** | ⚠️ | Passer error → **48h** + clarifier override `_sdc_received_at` |
| `nesp_co` | Standard | — | aucune | ❌ | Ajouter 26h/48h sur `activite` + `opportunite` ; `base_client` à part |
| `nesp_tech` | Hebdomadaire | — | aucune | ❌ | Ajouter 8j/14j (créer tier *Hebdomadaire*) |
| `gac` | Relaxe | `_sdc_batched_at` | **null** explicite | ⚠️ | Activer 7j/14j au lieu de désactiver |
| `oracle_neshu_gcs` | Relaxe | — | aucune | ❌ | Ajouter 7j/14j |
| `yuman_gcs` | Relaxe | — | aucune | ❌ | Ajouter 7j/14j |
| `zoho_desk` | N/A | — (string) | aucune | ⛔ | Workaround `dlt_load_id` à évaluer |

**Bilan** :
- 4 sources sur 10 ✅ configurées correctement
- 2 sources ⚠️ configurées mais à harmoniser (seuils trop stricts)
- 4 sources ❌ pas configurées du tout
- 1 source ⛔ non applicable techniquement

---

## Convention cible (à valider)

Pour chaque nouvelle source :
1. **Déterminer le tier** parmi Critique / Standard / Hebdomadaire / Relaxe
2. **Définir `loaded_at_field`** au niveau source (`_sdc_extracted_at`
   par défaut Singer ; sinon adapter)
3. **Appliquer le `freshness` du tier au niveau source** (pas par table)
4. **Désactiver explicitement** (`freshness: null`) sur les tables
   référentielles stables (codes, libellés, types) — **documenter le pourquoi
   en commentaire YAML**
5. **Ne jamais répéter** le freshness par table quand il est identique au
   défaut source — c'est du bruit YAML

> Cette convention est à reporter dans `CONVENTIONS.md` § Source freshness
> une fois validée.
