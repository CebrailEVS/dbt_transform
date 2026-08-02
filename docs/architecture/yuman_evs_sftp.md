# Architecture — Yuman stock théorique (SFTP -> dlt)

> Dernière mise à jour : 2026-08-02 — migration Meltano/GCS vers dlt

---

## Vue d'ensemble

Source **complémentaire** à `yuman` (API) qui apporte le **stock théorique des
entrepôts Yuman** — information non exposée par l'API REST Yuman.

Le fournisseur dépose chaque nuit un **CSV** sur son SFTP. Le pipeline dlt
`yuman_evs_stock` le lit et le charge dans la table native
`prod_raw.sftp_yuman_evs_stock_theorique`, partitionnée sur `export_date`.

> **Migration du 2026-08-02.** Auparavant Meltano déposait le fichier sur GCS
> et une external table (`ext_gcs_yuman__stock_theorique`) servait de source.
> Ce chemin **existe toujours** : le job `elt-yuman` continue d'alimenter
> l'archive GCS, seule trace fichier d'une source non rétroactive. Mais plus
> rien dans dbt ne la lit.

> Pourquoi un second pipeline pour Yuman ? L'API Yuman expose les mouvements
> et catalogues (workorders, materials, purchase_orders) mais **pas la photo
> stock**. L'export SFTP/GCS comble ce trou.

---

## Flux de données

```
┌──────────────────────┐                  ┌──────────────────────────────┐
│  SFTP fournisseur    │ ───── dlt ─────► │  prod_raw                    │
│  /stocks/stocks.csv  │   30 6 * * *     │  sftp_yuman_evs_stock_       │
│  CSV ; latin-1       │                  │  theorique (part. export_date)│
└──────────┬───────────┘                  └──────────┬───────────────────┘
           │                                         │ dbt staging
           │ Meltano (elt-yuman, 0 6 * * 1-6)        ▼
           │                              ┌──────────────────────────────┐
           ▼                              │  prod_staging                │
┌──────────────────────┐                  │  stg_yuman_evs_sftp__        │
│  GCS — archive       │                  │  stock_theorique (table)     │
│  export_date=…/*.jsonl│                 └──────────┬───────────────────┘
│  PLUS LUE PAR DBT    │                             │ dbt marts (direct)
└──────────────────────┘                             ▼
                                       ┌──────────────────────────────────┐
                                       │  marts/supply_chain/             │
                                       │  fct_supply_chain__stock_yuman   │
                                       │  fct_supply_chain__rupture_depot │
                                       └──────────────────────────────────┘
```

**Fraîcheur** : tier *Quotidien 7j/7* — warn 36h / error 48h. Resserré à la
migration : le cron Meltano sautait le dimanche, plus le pipeline dlt.

**Pas de couche intermediate** — le staging est consommé directement par les
deux marts supply chain.

---

## Le modèle staging — `stg_yuman_evs_sftp__stock_theorique`

Grain : **1 ligne par (article × emplacement × jour d'export)** — mais ce
triplet **n'est PAS unique** : le fichier du fournisseur contient de vrais
doublons, 1 463 lignes sur 2 002 398. Le test de grain porte donc sur
`_dlt_id`, l'identifiant de ligne généré par dlt, qui tient le rôle que
`_sdc_source_lineno` tenait sous Meltano.

**Volumétrie (août 2026)** :
- ~2,00 M lignes, ~4 004 références distinctes
- 209 jours d'historique (2025-11-28 → 2026-08-02)
- **39 jours absents sur 248** : ~34 dimanches jamais captés par l'ancien cron
  Meltano, plus **5 jours ouvrés définitivement perdus** (29/11/2025, 06, 07,
  09 et 10/02/2026, dont quatre consécutifs). La source n'est pas rétroactive :
  ces jours sont irrécupérables. C'est le test de fraîcheur qui empêche que ça
  se reproduise.

**Répartition par stock (top 5)** :

| `nom_du_stock` | # refs | # lignes |
|---|---|---|
| *(NULL)* — voir point d'attention | 3 234 | 383 109 (28 %) |
| `06 - ATELIER RUNGIS DEPOT` | 817 | 89 630 |
| `07 - ATELIER LYON DEPOT` | 654 | 73 493 |
| `ST - DIDION FRANCK` (stock perso roadman) | 324 | 43 073 |
| `ST - HEIDINGER YANNICK` | 295 | 34 773 |

Les stocks `ST - NOM PRENOM` correspondent aux **stocks personnels embarqués
des techniciens** (équivalent des `storehouses` Yuman — cf. `docs/architecture/yuman.md`
§ storehouses). Les `XX - DEPOT` sont les ateliers physiques.

| Colonne | Type | Source / Transformation |
|---|---|---|
| `reference` | string | `trim(r_f_rence)` — référence article |
| `designation` | string | `trim(d_signation)` — libellé article |
| `quantite` | float64 | `cast(replace(quantit_, ',', '.') as float64)` — gestion virgule décimale FR |
| `nom_du_stock` | string | `nullif(trim(nom_du_stock), '')` — entrepôt / stock |
| `export_date` | date | Date de **modification du fichier** sur le SFTP, non date du run |
| `_dlt_id` | string | Identifiant de ligne dlt — seule clé disponible |

Le cast de `quantite` part de `quantitx` et non `quantit_` : dlt et Meltano ne
mutilent pas l'accent final de `Quantité` de la même façon.

Étapes du modèle :
1. `source` — lecture de la table native
2. `cleaned` — trim, normalisation décimale FR, `nullif` sur les vides

**Plus d'étape `deduped`.** L'ancien modèle dédoublonnait pour absorber le
16/02/2026, journée déposée DEUX fois dans GCS. Le pipeline dlt écrit en
merge/delete-insert sur `export_date` : une journée rejouée écrase la
précédente au chargement, le doublon ne peut plus atteindre dbt.

---

## Marts consommateurs

| Modèle | Rôle |
|---|---|
| `fct_supply_chain__stock_yuman` | Photo stock par entrepôt × article × jour |
| `fct_supply_chain__rupture_depot_yuman` | Rupture reconstruite par dépôt × référence × jour |

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

### Pas de clé naturelle — et ce n'est pas un défaut d'extraction
Le fichier contient de vrais doublons : 1 463 lignes sur 2 002 398 partagent
`(export_date, reference, nom_du_stock)`, désignation comprise. Prétendre que
ce triplet est unique ferait échouer le build sur une propriété que la source
n'a jamais eue. Le test porte donc sur `_dlt_id`.

À savoir : ces doublons sont **comptés deux fois** dans les sommes des marts.
C'est le comportement historique, conservé à l'identique à la migration.

### Format décimal français
Les quantités arrivent en `"1,5"` (virgule). Le `replace(quantit_, ',', '.')`
gère la conversion vers `float64`. Si une valeur reste non-castable, BigQuery
lèvera une erreur dur — pas de `safe_cast` aujourd'hui.

### Pas de jointure FK avec le reste de Yuman
La table porte `reference` (texte) mais pas `product_id`. Pour relier au
catalogue Yuman (`stg_yuman__products`), faire un `join on reference = product_reference`
— et accepter le risque de désalignement (typo, espace, casse). Si besoin
récurrent, envisager un mart helper de mapping.

### Freshness — 36h / 48h, et c'est le seul filet
Tier *Quotidien 7j/7*. Le pipeline tourne à 06:30 tous les jours et
`export_date` suit le `mtime` du fichier : le pire cas normal est ~30h30, juste
avant le run du lendemain. 36h laisse ~5h de marge, 48h ne se déclenche que si
une journée entière manque.

C'est **la seule détection d'un jour perdu**, et cinq l'ont été sans que rien
ne le signale. Un fichier que le fournisseur cesse de rafraîchir est aussi
couvert : dlt ne le recharge pas, `max(export_date)` cesse d'avancer, et le
test finit par mordre.

### **`nom_du_stock IS NULL` ⇔ `quantite = 0` — règle stricte (sémantique, pas data quality)**

Constat investigué en mai 2026 sur l'ensemble du dataset (1,38 M lignes) :

| Bucket | Lignes | `quantite = 0` | `quantite > 0` |
|---|---|---|---|
| `nom_du_stock IS NULL` | 383 109 (28 %) | **100 %** | 0 % |
| `nom_du_stock` renseigné | 1 001 079 (72 %) | 0 % | **100 %** |

L'équivalence est **universelle** : aucune exception sur la fenêtre observée.
Ce n'est donc pas une perte d'information à l'extraction, mais une **règle de
l'export Yuman lui-même** : une référence sans stock physique n'est rattachée
à aucun entrepôt (logique — un article à 0 n'est dans aucun stock).

Au niveau brut (`prod_raw.sftp_yuman_evs_stock_theorique`), ces lignes
arrivent avec `nom_du_stock = ''` (chaîne vide), converties en NULL par le
`nullif(trim(nom_du_stock), '')` du staging.

**Conséquence métier** : le « catalogue stockable » Yuman = ~4 000 références,
mais seulement ~1 000 sont effectivement détenues en stock à un instant donné
(jour 2026-05-23 : 922 refs avec stock, 3 073 refs à zéro partout). Sur
`fct_supply_chain__stock_yuman`, filtrer `quantite > 0` (ou
`nom_du_stock IS NOT NULL`, équivalent) si l'on veut seulement les positions
réelles de stock. Garder les NULL si l'on veut le catalogue complet.

> Aucun ticket à ouvrir côté Yuman : comportement attendu confirmé par
> profiling. Documenter ce point dans les marts consommateurs pour éviter une
> ré-investigation future.
