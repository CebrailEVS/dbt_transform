# Architecture — Stock théorique Oracle (NESHU, LCDP et Nayka)

| | NESHU | LCDP | Nayka |
|---|---|---|---|
| Source dbt | `oracle_neshu_gcs` (`models/staging/oracle_neshu_gcs/_oracle_neshu_gcs__sources.yml`) | `oracle_lcdp_gcs` (`models/staging/oracle_lcdp_gcs/_oracle_lcdp_gcs__sources.yml`) | `oracle_nayka_gcs` (`models/staging/oracle_nayka_gcs/_oracle_nayka_gcs__sources.yml`) |
| Pipeline dlt | `ingestion/pipelines/oracle_neshu_stock` | `ingestion/pipelines/oracle_lcdp_stock` | `ingestion/pipelines/oracle_nayka_stock` |
| Chargement | `merge` / `delete-insert` sur `snapshot_date` : une photo par jour, un jour rejoué écrase le sien | idem | idem |

Fraîcheur : [`docs/freshness.md`](../freshness.md). Cadence : [`docs/pipeline-schedule.md`](../pipeline-schedule.md).

Source **complémentaire** à `oracle_neshu` / `oracle_lcdp` / `oracle_nayka` ([`oracle_neshu.md`](oracle_neshu.md)) :
le **stock théorique journalier** par entité (dépôt ou véhicule) et par article, que
l'extraction standard ne fournit pas. Il est calculé côté Oracle par une fonction de l'éditeur,
appelée une fois par entité : une boîte noire dont on ne voit pas le code. Les trois pipelines
partagent un moteur (`ingestion/shared/oracle_stock`) et les trois staging
`stg_oracle_<erp>_gcs__stock_theorique` sont identiques. Le suffixe `_gcs` est un nom hérité,
sans lien avec Cloud Storage.

---

## Clés et grain

**1 ligne par (`snapshot_date`, `entity_type`, `id_entity`, `product_code`).**

- `entity_type` (`company` = dépôt, `resource` = ressource, véhicule en pratique) est **indispensable** :
  `idcompany` et `idresources` viennent de deux séquences Oracle distinctes et collisionnent.
  Le test de grain vit en aval (marts, intermediate), pas sur le staging.
- La source ne porte pas l'`idproduct` Oracle : joindre les dims produit sur `product_code`.
- Les dépôts sont une liste blanche de libellés (`DEPOTS` dans `tables.py` de chaque
  pipeline) ; les véhicules ne sont pas filtrés à l'extraction.
- `snapshot_date` est **la** clé de filtre (jour métier et clé de merge) ; `date_system` sert à
  l'audit et à la partition du staging.

---

## Pièges

### Les horloges Oracle n'ont pas de fuseau
- **Symptôme** : la photo du soir bascule sur le lendemain dès qu'on la convertit en heure
  locale ; deux extractions « du même jour » ne concordent pas.
- **Règle** : `SYSDATE` est une horloge murale et le serveur est en Europe/Paris. Un `cast`
  direct la ferait lire en UTC. Le staging déclare le fuseau d'origine : `paris_vers_utc` pour
  `date_system`, `safe.parse_timestamp('%d/%m/%Y %H:%M', date_inventaire, 'Europe/Paris')` pour
  `date_inventaire`. Filtrer sur `snapshot_date`, jamais sur le jour de `date_system`.
- **Contrôle** : `date_system` et `extracted_at` décrivent le même run, leur écart reste de
  quelques minutes. Un écart d'une ou deux heures trahit un fuseau mal déclaré.
- **Appliqué par** : `stg_oracle_neshu_gcs__stock_theorique`, `stg_oracle_lcdp_gcs__stock_theorique`,
  `stg_oracle_nayka_gcs__stock_theorique`.

### Une journée chargée est figée
- **Règle** : la clé de merge étant `snapshot_date`, une journée n'est plus réécrite. Un
  mouvement saisi en retard dans Oracle apparaît dans les `plus` / `moins` des journées
  suivantes, jamais dans une photo passée.
- **Rejeu** : `--business-date` est idempotent côté raw, mais rien ne garantit que la fonction
  de l'éditeur recalcule fidèlement un passé. Pas de rejeu en production sans validation de
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
  `int_oracle_neshu__stock_theorique` reprend la même sous-liste pour l'application Cockpit.
  Un dépôt renommé dans l'ERP fait échouer l'extraction, qui contrôle `DEPOTS` contre
  `company` : corriger `tables.py`, le mart **et** l'intermediate.

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

- **Filtre du mart** : aucun. `fct_supply_chain__stock_lcdp` garde tous les dépôts du pipeline et
  toutes les ressources.
- **Flux mensuel** : pas d'équivalent de `fct_supply_chain__flux_neshu`.

---

## Nayka : ce qui diffère

- **Pas de véhicule** : la photo ne porte que des dépôts (`entity_type = company`). Un test en
  `warn` sur le staging signale l'apparition d'une ressource.
- **`dpa` n'est pas renseigné** par la fonction de stock : le prix se lit dans `purchase_price`,
  le prix de la fiche au moment de la photo. `int_oracle_nayka__stock_theorique` applique le même
  repli que LCDP.
- **Les modèles de machines sont dans la photo** (`product_type = MODELE`), en quantité, alors que
  chaque machine est aussi un équipement de `int_oracle_nayka__device`. Ne jamais additionner les
  deux : l'une des sources sert de contrôle à l'autre.
- **Rôle du dépôt** : `depot_role` (macro `nayka_depot_role`) distingue les agences, le dépôt
  marketing et le rebut, avec la même règle pour les produits et pour les équipements.
- **Le staging teste lui-même son grain**, en plus de l'intermediate.
- **Pas de mart** : seuls l'intermediate et la couche `apps` lisent cette source.

---

## Consommateurs

`dbt ls -s source:oracle_neshu_gcs+`, `dbt ls -s source:oracle_lcdp_gcs+` et
`dbt ls -s source:oracle_nayka_gcs+`.
