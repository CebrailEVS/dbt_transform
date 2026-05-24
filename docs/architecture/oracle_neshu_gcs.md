# Architecture — Oracle Neshu GCS

> Dernière mise à jour : 2026-05-24

---

## Vue d'ensemble

Source **complémentaire** à `oracle_neshu` qui apporte une donnée non disponible
via l'extraction Meltano standard : le **stock théorique journalier** par
entité (ressource ou dépôt) et par produit.

Un job externe (extraction nocturne) dépose chaque jour un **fichier CSV** sur
Google Cloud Storage. Une **external table** BigQuery (`ext_gcs_oracle_neshu__stock_theorique`)
pointe sur ce bucket, et un modèle staging unique nettoie / type les colonnes.

> Pourquoi un second pipeline pour Oracle ? Le stock théorique est calculé
> côté Oracle par batch et n'est pas disponible via les tables `evs_*`
> extraites par Meltano. Le CSV est le format de sortie natif de ce batch.

---

## Flux de données

```
┌──────────────────────┐   batch nocturne   ┌─────────────────────┐
│   Oracle Neshu       │ ─────────────────► │   GCS bucket        │
│   (batch stock)      │   CSV / jour       │   *.csv             │
└──────────────────────┘                    └──────────┬──────────┘
                                                       │ external table
                                                       ▼
                                       ┌──────────────────────────────────┐
                                       │  prod_raw                        │
                                       │  ext_gcs_oracle_neshu__          │
                                       │  stock_theorique                 │
                                       └──────────┬───────────────────────┘
                                                  │ dbt staging
                                                  ▼
                                       ┌──────────────────────────────────┐
                                       │  prod_staging                    │
                                       │  stg_oracle_neshu_gcs__          │
                                       │  stock_theorique (table)         │
                                       │  partitionné date_system         │
                                       └──────────┬───────────────────────┘
                                                  │ dbt marts (direct)
                                                  ▼
                                       ┌──────────────────────────────────┐
                                       │  marts/supply_chain/             │
                                       │  fct_supply_chain__stock_neshu   │
                                       │  fct_supply_chain__flux_neshu    │
                                       └──────────────────────────────────┘
```

**Fraîcheur** : tier *Relaxe* — warn 7j / error 14j. Le batch tourne tous les
jours mais peut sauter un cycle sans impact métier critique.

**Pas de couche intermediate** — le staging est consommé directement par les
marts `supply_chain`.

---

## Le modèle staging — `stg_oracle_neshu_gcs__stock_theorique`

Grain : **1 ligne par (entité, produit, date_system)**.

**Volumétrie (mai 2026)** :
- ~620 k lignes, ~3 000 lignes/jour (très stable : min 2 630 / max 3 322)
- 204 jours d'historique (depuis ~2025-11)
- 76 entités stockantes × 204 produits
- Couverture quotidienne — pas de jour manquant sur la fenêtre observée

**Répartition des entités (`entity_type`)** :

| `entity_type` | # entités | % lignes | Exemples |
|---|---|---|---|
| `company` | 15 | 22 % | `06 - atelier rungis depot`, `02 - lyon depot produits`, `13 - marseille depot produits`, `10 - rebus depot`, `05 - perimes depot`… (dépôts physiques EVS, préfixe numérique) |
| `resource` | 65 | 78 % | `anim rungis`, `prepa rungis`, `ksaadouni`, `asoumare`… (roadmen + ressources de préparation) |

> Les deux seules valeurs possibles aujourd'hui sont `company` et `resource` —
> à durcir éventuellement avec un test `accepted_values`.

| Colonne | Type | Source / Transformation |
|---|---|---|
| `id_entity` | int64 | Cast de la colonne brute |
| `entity_name` | string | `lower(...)` |
| `entity_type` | string | `lower(...)` — `company` (dépôt) ou `resource` (roadman / ressource interne) |
| `date_system` | timestamp | **Partition** — date du système d'inventaire |
| `resources_code` | string | Code ressource (camion / roadman) |
| `product_code` | string | Renommé depuis `code_source` |
| `product_name` | string | Renommé depuis `code_name` |
| `date_inventaire` | timestamp | `safe.parse_timestamp('%d/%m/%Y %H:%M', ...)` — date du dernier inventaire physique |
| `stock_inventaire` | numeric | Stock physique constaté à `date_inventaire` |
| `plus` | numeric | Quantité en plus par rapport au théorique |
| `moins` | numeric | Quantité en moins par rapport au théorique |
| `stock_at_date` | numeric | **Stock théorique** à `date_system` |
| `dpa` | numeric | Dernier prix d'achat |
| `pump` | numeric | Prix moyen pondéré |
| `purchase_price` | numeric | Prix d'achat unitaire |
| `extracted_at` | timestamp | Timestamp d'extraction du CSV |
| `row_count` | int | Nombre de lignes du fichier source (audit) |
| `file_datetime` | datetime | Extrait du nom de fichier via regex `(\d{4}_\d{2}_\d{2}_\d{4})` |

Configuration :
```sql
{{ config(
    materialized='table',
    partition_by={'field': 'date_system', 'data_type': 'timestamp'},
    description='Table des stocks théoriques depuis les fichiers GCS Oracle Neshu'
) }}
```

---

## Marts consommateurs

| Modèle | Rôle |
|---|---|
| `fct_supply_chain__stock_neshu` | Photo stock par entité × produit × date |
| `fct_supply_chain__flux_neshu` | Flux supply chain mensuel — utilise le stock comme état initial / final |

Les dimensions associées (produit, ressource) viennent de `dim_neshu__product`
et `dim_neshu__resource`. Joindre via `product_code` (et non `idproduct` —
le CSV ne porte pas la PK Oracle).

---

## Points d'attention

### Pas de PK technique — clé naturelle `(id_entity, product_code, date_system)`
La table n'expose pas d'identifiant unique technique. Le grain est garanti par
la clé naturelle. **Aucun test `unique` n'est posé** côté staging — ajouter un
`dbt_utils.unique_combination_of_columns` si une régression de grain est
suspectée.

### `date_system` ≠ `date_inventaire`
- `date_system` = date à laquelle Oracle a **calculé** le stock théorique (la
  date du batch, partition de la table)
- `date_inventaire` = date du **dernier inventaire physique** réel utilisé
  comme point de départ du calcul

Pour une photo « stock à date X », filtrer sur `date_system = X`, pas sur
`date_inventaire`.

### Format de date français dans le CSV
`date_inventaire` est parsée en `%d/%m/%Y %H:%M` via `safe.parse_timestamp` —
les valeurs malformées remontent en `NULL` plutôt qu'en erreur. Surveiller via
un test `not_null` si la qualité du CSV se dégrade.

### `file_datetime` est extrait du nom de fichier
Pattern : `..._YYYY_MM_DD_HHMM...`. Si le nommage côté job Oracle change, la
regex échoue silencieusement (`NULL`). Audit possible via `where file_datetime is null`.

### Pas de freshness stricte
Tier *Relaxe* (7j/14j) — utiliser ce modèle pour des analyses de stock pas
pour du temps réel. Une donnée à J-1 est attendue mais pas garantie.

### Taux de NULL observés (mai 2026)

| Colonne | % NULL | Lecture |
|---|---|---|
| `stock_at_date` | 0 % | Toujours renseigné — c'est la mesure principale |
| `stock_inventaire` | 0 % | Toujours renseigné |
| `resources_code` | 0 % | Toujours renseigné |
| `pump` | 0 % | Toujours renseigné |
| `purchase_price` | 2 % | Quelques produits sans prix |
| `dpa` | 9 % | Produits sans historique d'achat récent |
| `date_inventaire` | **25 %** | Significatif — produits sans inventaire physique récent |

Le quart des lignes sans `date_inventaire` correspond à des produits sur
lesquels aucun inventaire physique n'a été remonté côté Oracle. La colonne
`stock_at_date` reste utilisable (calculée même sans point d'ancrage récent),
mais les écarts `plus`/`moins` sont à interpréter avec prudence sur ces lignes.
