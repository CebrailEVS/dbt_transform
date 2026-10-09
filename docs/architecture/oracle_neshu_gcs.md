# Architecture — Stock théorique Oracle (NESHU et LCDP)

> Le fichier garde le nom `oracle_neshu_gcs.md` pour ne pas casser les liens ; il couvre les deux ERP.

| | NESHU | LCDP |
|---|---|---|
| Source dbt | `oracle_neshu_gcs` (`models/staging/oracle_neshu_gcs/_oracle_neshu_gcs__sources.yml`) | `oracle_lcdp_gcs` (`models/staging/oracle_lcdp_gcs/_oracle_lcdp_gcs__sources.yml`) |
| Pipeline dlt | `oracle_neshu_stock` (`ingestion/pipelines/oracle_neshu_stock`) | `oracle_lcdp_stock` (`ingestion/pipelines/oracle_lcdp_stock`) |
| Table raw | `prod_raw.oracle_neshu_stock_theorique` | `prod_raw.oracle_lcdp_stock_theorique` |
| Chargement | `merge` / `delete-insert` sur `snapshot_date` : une photo par jour, un jour rejoué écrase le sien | idem |

Fraîcheur : `docs/freshness.md`. Cadence et orchestration : `docs/pipeline-schedule.md`.

Source **complémentaire** à `oracle_neshu` / `oracle_lcdp` (`docs/architecture/oracle_neshu.md`) :
le **stock théorique journalier** par entité (dépôt ou véhicule) et par article, que
l'extraction standard ne fournit pas. Il est calculé côté Oracle par la fonction PL/SQL
`PCK_STOCK.GET_STOCK`, appelée une fois par entité : une boîte noire dont on ne voit pas le code.
Les deux pipelines partagent un moteur (`ingestion/shared/oracle_stock`) et les deux staging
`stg_oracle_<erp>_gcs__stock_theorique` sont identiques. Pas de couche intermediate : les
marts `supply_chain/` lisent le staging.

Le suffixe `_gcs` des sources est historique (ancien dépôt CSV sur Cloud Storage) : il n'y a
plus ni CSV ni table externe. Le nom est conservé pour ne pas casser les `source()`.

---

## Grain et clés

**1 ligne par (`snapshot_date`, `entity_type`, `id_entity`, `product_code`).**

- `entity_type` (`company` = dépôt, `resource` = ressource, véhicule en pratique) est **indispensable** :
  `idcompany` et `idresources` viennent de deux séquences Oracle distinctes et collisionnent.
  Le test de grain vit sur les marts (`unique_combination_of_columns`), pas sur le staging.
- La source ne porte pas l'`idproduct` Oracle : joindre les dims produit sur `product_code`.
- Les dépôts sont une liste blanche de libellés (`DEPOTS` dans `tables.py` de chaque
  pipeline) ; les véhicules ne sont pas filtrés à l'extraction.

| Colonne | Nature | Usage |
|---|---|---|
| `snapshot_date` | `DATE`, dérivée de `SYSDATE` Oracle | Jour métier et clé de merge : **la** clé de filtre |
| `date_system` | `TIMESTAMP`, `SYSDATE` heure comprise | Audit (un jour rejoué y porte minuit) et partition du staging ; pas une clé de filtre |
| `extracted_at` | `TIMESTAMP` du run dlt | Témoin de rechargement |
| `date_inventaire` | `VARCHAR2` Oracle, parsé en timestamp | Dernier inventaire physique ; NULL si aucun n'est remonté |

---

## Pièges

### Les horloges Oracle n'ont pas de fuseau
- **Symptôme** : la photo du soir bascule sur le lendemain dès qu'on la convertit en heure
  locale ; deux extractions « du même jour » ne concordent pas.
- **Règle** : `SYSDATE` est une horloge murale et le serveur est en Europe/Paris. Un `cast`
  direct la ferait lire en UTC. Le staging déclare le fuseau d'origine :
  `timestamp(datetime(date_system), 'Europe/Paris')` et
  `safe.parse_timestamp('%d/%m/%Y %H:%M', date_inventaire, 'Europe/Paris')`. Filtrer sur
  `snapshot_date`, jamais sur le jour de `date_system`.
- **Contrôle** : `date_system` et `extracted_at` décrivent le même run, leur écart reste de
  quelques minutes. Un écart d'une ou deux heures trahit un fuseau mal déclaré.
- **Appliqué par** : `stg_oracle_neshu_gcs__stock_theorique`, `stg_oracle_lcdp_gcs__stock_theorique`.

### Une journée chargée est figée
- **Règle** : la clé de merge étant `snapshot_date`, une journée n'est plus réécrite. Un
  mouvement saisi en retard dans Oracle apparaît dans les `plus` / `moins` des journées
  suivantes, jamais dans une photo passée.
- **Rejeu** : `--business-date` est idempotent côté raw, mais rien ne garantit que
  `GET_STOCK` recalcule fidèlement un passé. Pas de rejeu en production sans validation de
  l'éditeur. Un jour rejoué se repère à `date_system` (minuit) et à `extracted_at`.

### `is_active` des véhicules : exposé, jamais filtrant
- **Symptôme** : l'historique publié d'un véhicule disparaît ou réapparaît au gré de son
  activation dans l'ERP.
- **Règle** : `is_active` est un état **courant** d'une dimension non historisée. Les marts
  l'exposent en `is_vehicle_active` sans filtrer, et le jeu de lignes est déterminé par le raw.
  Le total non filtré inclut donc des véhicules sortis du parc, à stock figé. Un rapport qui
  veut le parc roulant filtre sur la colonne, en acceptant un état courant et non un état à la date.
- **Appliqué par** : `fct_supply_chain__stock_neshu`, `fct_supply_chain__stock_lcdp`.

### Le filtre NESHU dépend de libellés et de la dim ressource
- **Règle** : `fct_supply_chain__stock_neshu` garde une sous-liste de dépôts, par libellé en
  minuscules, et les seules ressources de type `VEHICLE` lues dans `dim_neshu__resource` (la
  PERSON présente dans le stock est exclue). Une ressource absente de la dim sort donc du mart.
  Un dépôt renommé dans l'ERP fait échouer l'extraction, qui contrôle `DEPOTS` contre
  `company` : corriger `tables.py` **et** la liste du mart.

---

## Règles de calcul

- **Invariant** : `stock_at_date = stock_inventaire + plus - moins`, testé sur les deux marts.
  `plus` et `moins` sont des cumuls d'entrées et de sorties **depuis le dernier inventaire
  physique**, remis à zéro à chaque inventaire : c'est la cause des ruptures de série, pas une
  correction rétroactive.
- `stock_at_date` est un stock théorique, pas un comptage : il peut être négatif. Sans
  `date_inventaire`, `plus` / `moins` n'ont pas de point d'ancrage récent.
- **Prix** : les marts exposent `dpa` (dernier prix d'achat), à défaut `purchase_price`.
  `pump` n'est repris par aucun mart.

---

## LCDP : ce qui diffère

| | NESHU | LCDP |
|---|---|---|
| Schéma Oracle | `EVS` | `LCDP` |
| Filtre du mart | sous-liste de dépôts, ressources `VEHICLE` seulement | **aucun** : tous les dépôts du pipeline et toutes les ressources |
| Flux mensuel | `fct_supply_chain__flux_neshu` | aucun |

---

## Consommateurs

`dbt ls -s source:oracle_neshu_gcs+` et `dbt ls -s source:oracle_lcdp_gcs+`.
