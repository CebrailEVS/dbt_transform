# Architecture — Yuman GCS

> Dernière mise à jour : 2026-05-24

---

## Vue d'ensemble

Source **complémentaire** à `yuman` (API) qui apporte le **stock théorique des
entrepôts Yuman** — information non exposée par l'API REST Yuman.

Un export SFTP côté Yuman dépose un **fichier JSON** sur Google Cloud Storage.
Une **external table** BigQuery (`ext_gcs_yuman__stock_theorique`) pointe sur
ce bucket, et un modèle staging unique nettoie / dédoublonne les lignes.

> Pourquoi un second pipeline pour Yuman ? L'API Yuman expose les mouvements
> et catalogues (workorders, materials, purchase_orders) mais **pas la photo
> stock**. L'export SFTP/GCS comble ce trou.

---

## Flux de données

```
┌──────────────────────┐   SFTP / push      ┌─────────────────────┐
│   Yuman              │ ─────────────────► │   GCS bucket        │
│   (export stock)     │   JSON / jour      │   *.json            │
└──────────────────────┘                    └──────────┬──────────┘
                                                       │ external table
                                                       ▼
                                       ┌──────────────────────────────────┐
                                       │  prod_raw                        │
                                       │  ext_gcs_yuman__stock_theorique  │
                                       └──────────┬───────────────────────┘
                                                  │ dbt staging
                                                  ▼
                                       ┌──────────────────────────────────┐
                                       │  prod_staging                    │
                                       │  stg_yuman_gcs__                 │
                                       │  stock_theorique (table)         │
                                       └──────────┬───────────────────────┘
                                                  │ dbt marts (direct)
                                                  ▼
                                       ┌──────────────────────────────────┐
                                       │  marts/supply_chain/             │
                                       │  fct_supply_chain__stock_yuman   │
                                       └──────────────────────────────────┘
```

**Fraîcheur** : tier *Relaxe* — warn 7j / error 14j.

**Pas de couche intermediate** — le staging est consommé directement par
`fct_supply_chain__stock_yuman`.

---

## Le modèle staging — `stg_yuman_gcs__stock_theorique`

Grain : **1 ligne par (`_sdc_source_file`, `_sdc_source_lineno`, `export_date`)**
— soit une ligne par article × entrepôt × jour d'export.

Test de grain en place : `dbt_utils.unique_combination_of_columns` sur la clé
ci-dessus.

| Colonne | Type | Source / Transformation |
|---|---|---|
| `reference` | string | `trim(r_f_rence)` — référence article |
| `designation` | string | `trim(d_signation)` — libellé article |
| `quantite` | float64 | `cast(replace(quantit_, ',', '.') as float64)` — gestion virgule décimale FR |
| `nom_du_stock` | string | `nullif(trim(nom_du_stock), '')` — entrepôt / stock |
| `export_date` | date | Date d'export (clé naturelle) |
| `_sdc_source_file` | string | Fichier source (clé naturelle) |
| `_sdc_source_lineno` | int | Numéro de ligne (clé naturelle) |

Étapes du modèle :
1. `source` — lecture brute de l'external table
2. `cleaned` — trim, normalisation décimale FR, `nullif` sur les vides
3. `deduped` — `row_number()` sur la clé naturelle pour ne garder qu'une ligne en cas de duplication d'ingestion

Configuration :
```sql
{{ config(
    materialized='table',
    description='Stocks théoriques Yuman normalisés depuis GCS'
) }}
```

---

## Marts consommateurs

| Modèle | Rôle |
|---|---|
| `fct_supply_chain__stock_yuman` | Photo stock par entrepôt × article × jour |

Pas de jointure directe avec `dim_*` Yuman aujourd'hui — la table fonctionne
en standalone. Si un cross-référencement est nécessaire avec
`stg_yuman__products`, joindre sur `reference = product_reference`
(attention : pas de FK technique, jointure textuelle).

---

## Points d'attention

### Noms de colonnes brutes corrompus par l'encodage
Le CSV/JSON brut expose des colonnes avec accents mal encodés :
`r_f_rence` (= référence), `d_signation` (= désignation), `quantit_` (=
quantité). Le staging réintroduit le nommage propre. **Toujours partir du
staging**, jamais de la source brute.

### Pas de PK technique — clé naturelle imposée par Singer
La déduplication s'appuie sur `(_sdc_source_file, _sdc_source_lineno, export_date)`.
Si l'extraction Meltano change de stratégie (ex. concaténation de fichiers),
cette clé peut casser. Surveiller le test `unique_combination_of_columns`.

### Format décimal français
Les quantités arrivent en `"1,5"` (virgule). Le `replace(quantit_, ',', '.')`
gère la conversion vers `float64`. Si une valeur reste non-castable, BigQuery
lèvera une erreur dur — pas de `safe_cast` aujourd'hui.

### Pas de jointure FK avec le reste de Yuman
La table porte `reference` (texte) mais pas `product_id`. Pour relier au
catalogue Yuman (`stg_yuman__products`), faire un `join on reference = product_reference`
— et accepter le risque de désalignement (typo, espace, casse). Si besoin
récurrent, envisager un mart helper de mapping.

### Pas de freshness stricte
Tier *Relaxe* (7j/14j). Comme pour `oracle_neshu_gcs`, donnée d'analyse
supply chain, pas du temps réel.
