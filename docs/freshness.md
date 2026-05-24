# Source freshness — audit et conventions

> Dernière mise à jour : 2026-05-24

---

## À quoi sert ce document

Référence unique pour la stratégie de monitoring de fraîcheur des 10 sources
du projet : tiers définis, configuration par source, mécanisme de fallback
pour les sources à timestamp STRING.

---

## Tiers de freshness

| Tier | Cadence typique | `warn_after` | `error_after` | Sources |
|---|---|---|---|---|
| **Critique** | quotidien, business-day SLA | 26h | 36h | `oracle_neshu`, `oracle_lcdp` |
| **Standard** | quotidien tolérant | 26h | 48h | `yuman`, `mssql_sage`, `nesp_co` (activite/opportunite) |
| **Hebdomadaire** | extraction 1×/semaine | 8 jours | 14 jours | `nesp_tech` |
| **Relaxe** | batch journalier non critique / GCS | 7 jours | 14 jours | `gac`, `oracle_neshu_gcs`, `yuman_gcs`, `zoho_desk` |
| **Manuel** | livraison ponctuelle uniquement | 60 jours | 90 jours | `nesp_co.nespresso_base_client` |

> **Principe directeur** : `freshness: null` se justifie *uniquement* sur les
> référentiels vraiment immuables (codes, libellés, types). Toute source qui
> porte des événements ou des données vivantes mérite un seuil — quitte à
> le mettre très large.

---

## Mécanismes de monitoring — deux familles

### A. `dbt source freshness` natif

Pour les sources dont la table brute expose un champ **TIMESTAMP ou DATE
nativement** (Singer `_sdc_*`, ou colonne dédiée typée).

Configuré dans `_<source>__sources.yml` au niveau source :

```yaml
sources:
  - name: <source>
    config:
      loaded_at_field: _sdc_extracted_at
      freshness:
        warn_after: {count: 26, period: hour}
        error_after: {count: 48, period: hour}
    tables:
      - name: <table_référentiel_stable>
        config:
          freshness: null   # désactivé — table immuable (codes, libellés)
```

> **Ne jamais répéter** le `freshness` par table quand il est identique au
> défaut source. C'est du bruit YAML.

### B. Tests `dbt_expectations` sur le staging

Pour les sources dont la table brute n'a **que des champs STRING** (les
external tables GCS et certains taps custom). Le staging fait déjà le cast
en TIMESTAMP via `safe.parse_timestamp` — on monitore donc **à ce niveau**.

Configuré dans `_<source>__models.yml` :

```yaml
models:
  - name: stg_<source>__<table>
    tests:
      - dbt_expectations.expect_row_values_to_have_recent_data:
          column: extracted_at   # déjà cast TIMESTAMP en staging
          datepart: day
          interval: 8            # ex. tier Hebdomadaire
          config:
            severity: warn
```

**Avantage** : pas de vue technique en amont. **Inconvénient** : pas
remonté dans `dbt source freshness` ni dans le rapport HTML standard — c'est
un test data quality classique.

---

## État cible par source

### `oracle_neshu` — tier *Critique*, méthode A
- `loaded_at_field: _sdc_extracted_at`
- Défaut source : **26h / 36h**
- Référentiels en `freshness: null` (intentionnel) : `evs_company_type`,
  `evs_product_type`, `evs_resources_type`, `evs_task_status`, `evs_task_type`,
  `evs_label`, `evs_label_family`, `evs_contract_parsed`
- **Action** : nettoyer les overrides 26h/36h redondants sur les tables actives

### `oracle_lcdp` — tier *Critique*, méthode A
Symétrique de `oracle_neshu`. Même action : nettoyer les overrides redondants.

### `yuman` — tier *Standard*, méthode A
- `loaded_at_field: _sdc_extracted_at`
- Défaut source : **26h / 48h** (à passer de 36h actuel)
- Référentiels en `null` : `yuman_products`, `yuman_material_categories`,
  `yuman_workorder_categories`, `yuman_workorder_demands_categories`,
  `yuman_storehouses`

### `mssql_sage` — tier *Standard*, méthode A
- `loaded_at_field: _sdc_extracted_at` partout (supprimer l'override
  `_sdc_received_at` sur `dbo_f_ecriturea` / `dbo_f_ecriturec` — écart
  observé = 0 minute, override inutile)
- Défaut source : **26h / 48h** (à passer de 36h actuel)

### `gac` — tier *Relaxe*, méthode A
- `loaded_at_field: _sdc_batched_at`
- Défaut source : **7j / 14j** (à activer, actuellement `null`)

### `yuman_gcs` — tier *Relaxe*, méthode A
- `loaded_at_field: export_date` (DATE — dbt accepte)
- Défaut source : **7j / 14j**

### `nesp_co.nespresso_base_client` — tier *Manuel*, méthode A
- `loaded_at_field: _sdc_extracted_at`
- **60j / 90j** (table-level override, distinct du défaut source si défini
  pour activite/opportunite)

### `nesp_co.nespresso_commerce_activite` + `nespresso_commerce_opportunite` — tier *Standard*, méthode B
Source brute : `extracted_at` est STRING → fallback `dbt_expectations` sur
le staging.
- Test sur `stg_nesp_co__activite` et `stg_nesp_co__opportunite`
- Colonne : `extracted_at` (cast en TIMESTAMP en staging)
- Seuil : **2 jours / warn** (équivalent 26h/48h en granularité jour)

### `nesp_tech` — tier *Hebdomadaire*, méthode B
Source brute : `extracted_at` est **STRING et NULL sur les fichiers récents**
(le champ a été abandonné par l'export Arbiter). Bascule sur les colonnes
business qui restent peuplées :
- `stg_nesp_tech__interventions` : test sur **`date_heure_fin`** (TIMESTAMP)
- `stg_nesp_tech__articles` : test sur **`date_intervention`** (DATE)
- Seuil : **8 jours warn / 14 jours error**

### `oracle_neshu_gcs` — tier *Relaxe*, méthode B
Source brute : `extracted_at` STRING. Test sur
`stg_oracle_neshu_gcs__stock_theorique`.
- Seuil : **7 jours warn / 14 jours error**

### `zoho_desk` — tier *Relaxe*, méthode B
Source brute : `_dlt_load_id` est un STRING (Unix epoch). Test sur les
staging zoho_desk qui exposent un timestamp cast.
- Seuil : **7 jours warn / 14 jours error**

---

## Synthèse — couverture finale visée

| Source | Tier | Méthode | Niveau | Seuils |
|---|---|---|---|---|
| `oracle_neshu` | Critique | A | source | 26h / 36h |
| `oracle_lcdp` | Critique | A | source | 26h / 36h |
| `yuman` | Standard | A | source | 26h / 48h |
| `mssql_sage` | Standard | A | source | 26h / 48h |
| `gac` | Relaxe | A | source | 7j / 14j |
| `yuman_gcs` | Relaxe | A | source | 7j / 14j |
| `nesp_co.base_client` | Manuel | A | table | 60j / 90j |
| `nesp_co.activite/opp` | Standard | B | staging | 2j |
| `nesp_tech` | Hebdomadaire | B | staging | 8j / 14j |
| `oracle_neshu_gcs` | Relaxe | B | staging | 7j / 14j |
| `zoho_desk` | Relaxe | B | staging | 7j / 14j |

**Toutes les sources sont désormais monitorées**, soit nativement, soit via
`dbt_expectations`.
