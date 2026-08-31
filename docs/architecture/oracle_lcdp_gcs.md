# Architecture — Oracle LCDP GCS (stock théorique)

> Dernière mise à jour : 2026-08-31

---

Le stock théorique des deux ERP Distrilog passe par **un moteur d'ingestion
commun** (`shared/oracle_stock/` dans le dépôt `ingestion`) et un staging de forme
identique. Tout ce qui est mécanique — pourquoi le nom porte encore « gcs », ce qui
est figé et ce qui ne l'est pas, le piège du fuseau, l'invariant de calcul, la
raison d'être de `entity_type` dans le grain — est décrit une seule fois, dans
[`oracle_neshu_gcs.md`](oracle_neshu_gcs.md). **Le lire d'abord.**

Ce document ne consigne que ce qui diffère côté LCDP.

---

## Ce qui diffère

| | Neshu | **LCDP** |
|---|---|---|
| Pipeline dlt | `oracle_neshu_stock` | **`oracle_lcdp_stock`** |
| Schéma Oracle | `EVS` | **`LCDP`** |
| Table cible | `oracle_neshu_stock_theorique` | **`oracle_lcdp_stock_theorique`** |
| Cron | `0 23 * * *` | **`15 23 * * *`** (23:15 Paris) |
| Début de l'historique | 2025-11-01 | **2026-06-05** — pas d'antérieur, aucune reprise CSV |
| Filtre du mart | 7 dépôts, véhicules **actifs seulement** | **aucun filtre** |
| `is_active` dans le mart | non exposée (elle filtre) | **exposée en colonne** |
| Modèle de flux mensuel | `fct_supply_chain__flux_neshu` | aucun |

**La différence qui compte** : `fct_supply_chain__stock_lcdp` **ne filtre pas** les
véhicules, il aplatit `is_active` depuis `dim_lcdp__resource` et laisse la BI
décider. Son historique est donc **stable**, là où celui de son homologue Neshu
bouge au gré des activations/désactivations de véhicules (cf.
[`oracle_neshu_gcs.md`](oracle_neshu_gcs.md) § Ce qui est figé). C'est la forme
vers laquelle Neshu devrait converger.

---

## Volumétrie (2026-08-31)

| | |
|---|---|
| Lignes | **167 731** |
| Journées | **87**, du 2026-06-05 au 2026-08-30 — aucun jour manquant |
| Entités | 11 dépôts (`company`) + 16 véhicules (`resource`) |
| Articles | **523** — deux fois plus que Neshu, pour cinq fois moins de lignes |
| Durée du run côté Oracle | ~65 s pour 1 826 lignes |

Les 11 dépôts sont une **liste blanche de libellés** déclarée côté ingestion
(`pipelines/oracle_lcdp_stock/tables.py`) : `DEPOT ATELIER`, `DEPOT BOUTIQUE`,
`DEPOT CASSE`, `DEPOT FABRICATION`, `DEPOT LOGISTIQUE`, `DEPOT NEUF`,
`DEPOT PERIME`, `DEPOT PERSONNEL`, `DEPOT TAMPON`, `DEPOT TORREFIE VRAC`,
`DEPOT VERT`. Les 11 sont présents dans le raw, et le mart les expose tous.

`date_inventaire` est NULL sur **31,0 %** des lignes (contre 24,6 % côté Neshu).

---

## Fraîcheur

Tier **Standard**, méthode B : **26 h warn / 48 h error** sur `extracted_at`.
Autorité : [`docs/freshness.md`](../freshness.md).

---

## Marts consommateurs

| Modèle | Rôle |
|---|---|
| `fct_supply_chain__stock_lcdp` | Photo stock par entité × article × jour |
| `fct_supply_chain__disponibilite_article_lcdp_depot_mensuel` | Taux de disponibilité mensuel par dépôt |
