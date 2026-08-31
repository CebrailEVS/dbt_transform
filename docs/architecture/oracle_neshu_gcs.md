# Architecture — Oracle Neshu GCS (stock théorique)

> Dernière mise à jour : 2026-08-31

---

## Pourquoi le nom porte encore « gcs »

La source dbt s'appelle toujours `oracle_neshu_gcs`, héritage de l'époque où le
stock théorique arrivait sous forme d'un CSV quotidien déposé sur Cloud Storage,
relu par une table externe BigQuery. **Depuis le 2026-08-06, il n'y a plus ni CSV
ni table externe** : la source lit `prod_raw.oracle_neshu_stock_theorique`, chargée
directement depuis Oracle par le pipeline dlt `oracle_neshu_stock` (dépôt
`ingestion`). L'historique CSV a été repris dans cette même table, à l'identique.

Le nom de la source est conservé pour ne pas casser les `source()` existants.
La table externe `ext_gcs_oracle_neshu__stock_theorique` n'existe plus.

---

## Vue d'ensemble

Source **complémentaire** à `oracle_neshu` : le **stock théorique journalier** par
entité (dépôt ou véhicule) et par article, que l'extraction standard ne fournit
pas. Il est calculé côté Oracle par la fonction PL/SQL `PCK_STOCK.GET_STOCK`,
appelée **une fois par entité** — une boîte noire dont on ne voit pas le code.

```
┌────────────────────────┐  dlt oracle_neshu_stock  ┌──────────────────────────────┐
│  Oracle EVS            │  quotidien 23:00 Paris   │  prod_raw                    │
│  PCK_STOCK.GET_STOCK   │ ───────────────────────► │  oracle_neshu_stock_theorique│
│  (1 appel par entité)  │  merge / delete-insert    │  clé de merge snapshot_date  │
└────────────────────────┘  sur snapshot_date        └──────────────┬───────────────┘
                                                                    │ dbt staging
                                                                    ▼
                                       ┌────────────────────────────────────────┐
                                       │  prod_staging                          │
                                       │  stg_oracle_neshu_gcs__stock_theorique │
                                       │  table, partition date_system          │
                                       └──────────────┬─────────────────────────┘
                                                      │ dbt marts (direct)
                                                      ▼
                                       ┌────────────────────────────────────────┐
                                       │  marts/supply_chain/                   │
                                       │  fct_supply_chain__stock_neshu         │
                                       │  fct_supply_chain__flux_neshu          │
                                       └────────────────────────────────────────┘
```

**Pas de couche intermediate** — le staging est consommé directement par les marts.

---

## Ce qui est figé, et ce qui ne l'est pas

C'est la question qui a coûté un aller-retour avec la logistique le 2026-08-31.
La réponse n'est pas uniforme.

**La valeur est figée.** Chaque nuit Oracle calcule, dlt écrit, et la journée
n'est plus retouchée. La clé de merge est `snapshot_date`, donc seul un rejeu
explicite pourrait réécrire une journée passée. Contrôlé sur les 303 journées
chargées : les seules dont la date de chargement est postérieure à leur journée
sont **2025-11-01 à 2025-11-11**, la reprise d'historique initiale. Un mouvement
saisi en retard côté Oracle **n'altère donc pas** une ligne passée — il se
manifeste dans les `plus`/`moins` des journées suivantes.

**Rejouer une journée est possible mais ne l'a jamais été.** Le pipeline accepte
`--business-date`, et le `delete-insert` rendrait l'opération idempotente. Mais
`PCK_STOCK.GET_STOCK` est une boîte noire : **rien ne garantit qu'elle recalcule
fidèlement un passé**. À trancher avec le DBA Distrilog avant tout usage en
production. Un rejeu se lirait sur `extracted_at`, qui porterait la date du rejeu.

**Le périmètre véhicules du mart ne l'était pas — corrigé le 2026-08-31.**
`fct_supply_chain__stock_neshu` filtrait les ressources sur `is_active`, un attribut
d'**état courant** de `dim_neshu__resource`, dimension sans snapshot SCD2. Un
véhicule désactivé côté Oracle disparaissait donc rétroactivement de tout
l'historique publié, et un véhicule réactivé y réapparaissait : au 2026-08-31, 802
des 2 571 lignes ressource du 2026-07-01 étaient écartées. C'est ce qui a fait
conclure à tort, côté logistique, à un solde recalculé.

`is_active` n'est plus filtrante. Elle est **exposée** sous le nom
`is_vehicle_active`, et le seul filtre conservé sur les ressources est
`resources_type = 'VEHICLE'`, qui exclut la PERSON et ne bouge pas. Le jeu de lignes
est donc entièrement déterminé par le raw. Un rapport qui veut le parc roulant filtre
sur la colonne, en acceptant qu'elle porte l'état courant et non l'état à la date.

Deux conséquences à connaître. Le total non filtré inclut le **résidu des véhicules
sortis du parc** : 24 véhicules, 796 lignes, dont le stock est **figé** (aucun
mouvement depuis le 30/06) et cumule +47 695 unités de résidu réel contre −61 951
unités de théorique négatif jamais soldé — un sujet de qualité de donnée à traiter
avec Distrilog, pas de modélisation. Et le taux de disponibilité véhicule passe de
87,2 % à 82,3 % tant qu'on ne filtre pas.

**Le même défaut subsiste dans `fct_supply_chain__flux_neshu`**, qui applique son
propre `is_active` sur la dim à deux endroits (tâches d'inventaire, valorisation de
repli). Ses chiffres mensuels bougent donc encore rétroactivement. Non traité : la
correction touche une chaîne de valorisation et demande une revue métier.

---

## Les deux horloges, et le piège du fuseau

| Colonne | Nature | Usage |
|---|---|---|
| `snapshot_date` | `DATE`, sans heure ni fuseau | **La clé de filtre.** Jour métier de la photo, et clé de merge du pipeline |
| `date_system` | `TIMESTAMP` — `SYSDATE` Oracle, heure comprise (23:00) | Audit : elle porte l'heure du batch, donc un rejeu s'y voit (il écrit minuit). **Pas une clé de filtre** |
| `extracted_at` | `TIMESTAMP` du run dlt | Témoin de révision (cf. ci-dessus) |
| `date_inventaire` | `TIMESTAMP` — `VARCHAR2` côté Oracle | Dernier inventaire physique |

`SYSDATE` est une **horloge murale sans fuseau**, et le serveur Oracle est en
Europe/Paris. Jusqu'au 2026-08-31 le staging faisait un `cast(... as timestamp)`
dessus : BigQuery, faute de fuseau déclaré, la lisait comme UTC. L'instant stocké
était donc faux de 1 h l'hiver et de 2 h l'été — et comme le batch tourne à 23:00,
**toute lecture convertie en heure locale basculait la photo sur le lendemain**.

> **Incident 2026-08-31.** La logistique compare le stock dépôts « fin juin » entre
> deux extractions et trouve 11 680 € d'écart (258 014 € contre 246 333 €). Aucune
> correction rétroactive : les deux chiffres étaient les photos du 30/06 et du 01/07,
> lues sous deux conventions de fuseau différentes.

Le staging déclare désormais le fuseau d'origine :
`timestamp(datetime(date_system), 'Europe/Paris')`, et
`safe.parse_timestamp('%d/%m/%Y %H:%M', date_inventaire, 'Europe/Paris')`.
`extracted_at` n'est pas touchée : elle portait déjà son décalage côté ingestion.

**Recette du fuseau** — `date_system` et `extracted_at` décrivent le même run à une
ou deux minutes près, donc leur écart mesure directement un décalage indûment
appliqué. Il valait 59 min l'hiver et 117-120 min l'été (bascule au jour près le
2026-03-29) ; il doit rester sous 2 minutes.

---

## Le grain

**1 ligne par (`snapshot_date`, `entity_type`, `id_entity`, `product_code`).**

`entity_type` est **indispensable** : `idcompany` et `idresources` viennent de deux
séquences Oracle distinctes et **collisionnent**. Sans lui, 17 698 lignes Neshu
apparaissent en doublon — « 06 - atelier rungis depot » partage son id avec la
ressource « tlucet », « 05 - perimes depot » avec « elise khatchadourian ». Avec
lui : 0 doublon sur 949 723 lignes. Le test vit sur les marts
(`dbt_utils.unique_combination_of_columns`), pas sur le staging.

---

## Volumétrie (2026-08-31)

| | |
|---|---|
| Lignes | **949 723** |
| Journées | **303**, du 2025-11-01 au 2026-08-30 — **aucun jour manquant** |
| Lignes par jour | 2 630 min / 3 463 max |
| Entités | 15 dépôts (`company`) + 67 ressources (`resource`) |
| Articles | 225 |
| Durée du run côté Oracle | ~53 s pour 3 300 lignes — le coût est dans la fonction PL/SQL |

Les 15 dépôts viennent d'une **liste blanche de libellés** déclarée côté ingestion
(`pipelines/oracle_neshu_stock/tables.py`), contrôlée contre `EVS.company` avant
chaque run. Le mart n'en retient que **7**.

---

## Colonnes du staging

| Colonne | Type | Source / Transformation |
|---|---|---|
| `snapshot_date` | date | Jour métier, posé par le pipeline |
| `id_entity` | int64 | Cast |
| `entity_name` | string | `lower(...)` |
| `entity_type` | string | `lower(...)` — `company` ou `resource` |
| `date_system` | timestamp | **Partition** — déclaré `Europe/Paris` |
| `resources_code` | string | Code de l'entité (`DEPOTRUNGIS`, immatriculation…) |
| `product_code` | string | Renommé depuis `code_source` |
| `product_name` | string | Renommé depuis `code_name` |
| `date_inventaire` | timestamp | `safe.parse_timestamp(..., 'Europe/Paris')` |
| `stock_inventaire` | numeric | Stock physique constaté à `date_inventaire` |
| `plus` | numeric | Cumul des entrées depuis `date_inventaire` |
| `moins` | numeric | Cumul des sorties depuis `date_inventaire` |
| `stock_at_date` | numeric | **Stock théorique** — la mesure principale |
| `dpa` | numeric | Dernier prix d'achat |
| `pump` | numeric | Prix moyen pondéré — **toujours à 0, inexploitable** |
| `purchase_price` | numeric | Prix d'achat unitaire |
| `extracted_at` | timestamp | Horodatage du run dlt |

Les huit `NUMBER` Oracle sans précision arrivent en `NUMERIC(38,9)` depuis la
bascule dlt (`fetch_decimals=True` côté ingestion) — `dpa` et `purchase_price`
portent jusqu'à 6 décimales. Avant, tout était `STRING`.

---

## L'invariant de calcul

```
stock_at_date = stock_inventaire + plus - moins
```

Vérifié à **0 violation** sur les 949 723 lignes, et testé sur les deux marts de
stock. `plus` et `moins` sont des **cumuls depuis le dernier inventaire physique**,
remis à zéro à chaque nouvel inventaire — c'est cette remise à zéro, et non une
correction rétroactive, qui explique les ruptures de série sur `plus`/`moins`.

---

## Taux de NULL et valeurs à connaître (2026-08-31)

| Colonne | % NULL | Lecture |
|---|---|---|
| `stock_at_date` | 0 % | Toujours renseigné |
| `resources_code` | 0 % | Toujours renseigné |
| `purchase_price` | 2,1 % | Quelques articles sans prix |
| `dpa` | 9,4 % | Articles sans historique d'achat récent |
| `date_inventaire` | **24,6 %** | Articles sans inventaire physique remonté |

`stock_at_date` est **négatif sur 70 861 lignes (7,5 %)** — c'est un stock
théorique, pas un comptage. Les `plus`/`moins` des lignes sans `date_inventaire`
sont à interpréter avec prudence : elles n'ont pas de point d'ancrage récent.

---

## Fraîcheur

Tier **Standard**, méthode B : **26 h warn / 48 h error** sur `extracted_at`, via
`dbt_expectations.expect_row_values_to_have_recent_data`. Cron `0 23 * * *`, gap
observé 23-24 h très régulier. Autorité : [`docs/freshness.md`](../freshness.md).

---

## Marts consommateurs

| Modèle | Rôle |
|---|---|
| `fct_supply_chain__stock_neshu` | Photo stock par entité × article × jour |
| `fct_supply_chain__flux_neshu` | Flux mensuel — le stock sert d'état initial / final |

En aval : `disponibilite_article_neshu_depot_mensuel`,
`disponibilite_article_neshu_vehicule_mensuel`, `couverture_stock_neshu`,
`point_commande_neshu`. Les dimensions viennent de `dim_neshu__product` et
`dim_neshu__resource` — joindre sur `product_code`, et non `idproduct` : la source
ne porte pas la PK Oracle de l'article.

---

## Le jumeau LCDP

Le même moteur d'ingestion sert les deux ERP Distrilog. Voir
[`oracle_lcdp_gcs.md`](oracle_lcdp_gcs.md) pour ce qui diffère.
