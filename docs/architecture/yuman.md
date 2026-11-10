# Architecture — Yuman (API)

| | |
|---|---|
| Source dbt | `yuman_api` (`models/staging/yuman/_yuman__sources.yml`) |
| Pipeline | `ingestion/pipelines/yuman_evs` : une table `prod_raw.yuman_evs_*` par endpoint, périmètre dans `tables.py` |
| Chargement | snapshot complet (`replace`) à chaque run : le raw est l'état courant de l'API, sans historique, et une suppression dans Yuman disparaît du raw |

Fraîcheur : [`docs/freshness.md`](../freshness.md). Cadence : [`docs/pipeline-schedule.md`](../pipeline-schedule.md).

Yuman est l'outil de gestion des interventions terrain (workorders) et des demandes
d'intervention. Il alimente la facturation, le pilotage du SAV et le suivi des partenaires. Le
stock théorique, que l'API n'expose pas, arrive par un second pipeline : voir
[`yuman_evs_sftp.md`](yuman_evs_sftp.md).

---

## Clés et grain

- `stg_yuman__purchase_orders` : 1 commande × 1 article (clé `purchase_order_line_id`).
- `int_yuman__demands_workorders_enriched` : 1 couple (demande, workorder) issu d'un `FULL JOIN`,
  avec un léger fan-out non dédupliqué. `int_yuman__interventions` le déduplique sur
  `(demand_id, workorder_id)`.
- Le `replace` n'a pas d'historique : les snapshots de `snapshots/yuman/` historisent en SCD2,
  suppressions comprises, les colonnes suivies de quelques tables. Ils sont pilotés par Cloud
  Workflows, pas par le CI/CD.

---

## Pièges

### `stg_yuman__contacts` n'a ni `client_id` ni `site_id`
- **Symptôme** : une jointure `contacts.site_id` ou `contacts.client_id` ne compile pas.
- **Règle** : ces colonnes ne sont pas exposées. Relier un contact à un site ou un client par
  `workorders.contact_id` ou `workorder_demands.contact_id`.
- **Appliqué par** : `int_yuman__demands_workorders_enriched`.

### Les champs custom EVS vivent dans le JSON `_embed`
- **Symptôme** : une valeur métier (motif de non-intervention, raison de mise en pause, catégorie
  client EVS, code postal du site, `ID NOMAD`, article interdit…) est introuvable en colonne.
- **Règle** : les champs personnalisés sont dans `_embed` sous `$.fields`, extraits par `name`.
  Pour en ajouter un, modifier le staging concerné.
- **Appliqué par** : les staging qui lisent `_embed` (`grep -l _embed models/staging/yuman/`).

### Deux staging sont dépliés depuis un tableau JSON
- **Symptôme** : une somme sur `purchase_orders` compte l'en-tête de commande autant de fois qu'il
  y a d'articles.
- **Règle** : `purchase_orders` déplie `lines` (en-têtes dupliqués par ligne). `workorder_products`
  déplie `$.products` de `_embed` sur les workorders. Ne pas re-parser le JSON dans les marts.
- **Appliqué par** : `stg_yuman__purchase_orders`, `stg_yuman__workorder_products`.

### Demandes et workorders ne se recouvrent pas
- **Symptôme** : des demandes sans workorder, des workorders sans demande ; un `LEFT JOIN` perd
  l'une des deux populations.
- **Règle** : `workorder_id` est nullable sur les demandes (rejet, annulation, attente) et un
  workorder peut être créé sans demande. Jointure `FULL JOIN`. `int_yuman__interventions` écarte
  ensuite les workorders sans demande (`is_orphan_workorder`), qui fausseraient la clé
  d'intervention et la tarification.
- **Appliqué par** : `int_yuman__demands_workorders_enriched`, `int_yuman__interventions`.

### Le produit d'un workorder porte deux libellés
- **Symptôme** : `product_reference` introuvable sur `stg_yuman__products`.
- **Règle** : le catalogue expose `product_code` et `product_name` ; `product_reference` et
  `product_designation` n'existent que sur `stg_yuman__workorder_products`, dénormalisés depuis le
  JSON. Pour joindre, passer par `product_id`.

### `storehouses_id = user_id`, sauf pour les ateliers
- **Symptôme** : un entrepôt sans technicien ni manager correspondant.
- **Règle** : un storehouse est le stock embarqué d'un user ; son id est celui du user
  propriétaire. Seuls les ateliers physiques n'ont pas de user. Le test `relationships` vers
  `stg_yuman__users` exclut les ateliers et reste en `warn` : un orphelin signale une dérive.
- **Appliqué par** : `stg_yuman__storehouses` (test dans `_yuman__models.yml`).

### Libellé de l'atelier de Rungis précédé d'une espace
- **Symptôme** : l'atelier de Rungis ne se rapproche pas du libellé des dépôts de l'app
  (« 06 - ATELIER RUNGIS DEPOT »).
- **Règle** : Yuman renvoie ce libellé avec une espace en tête, dans les entrepôts comme dans
  l'entrepôt des bons de commande. `stg_yuman__purchase_orders` retire les espaces de bord ;
  `stg_yuman__storehouses` garde le libellé brut : joindre les deux sur `trim()`.
- **Appliqué par** : `stg_yuman__purchase_orders` (`storehouse_name`).

### Technicien → agence : jointure sur le nom
- **Symptôme** : technicien sans agence après un changement d'orthographe côté Yuman.
- **Règle** : Yuman ne porte pas l'agence. Le seed `ref_yuman__tech_agence` est joint sur le nom
  complet normalisé (majuscules, trim, préfixe `[INACTIF]` retiré). Tout nom modifié casse la
  jointure : mettre à jour le seed. Le mapping vit en intermediate, pas dans les marts.
- **Appliqué par** : `int_yuman__demands_workorders_enriched`.

### `partner_name` est déjà résolu
- **Règle** : `partner_id` pointe vers un autre `client_id` de la même table ; le staging expose
  `partner_name`. Ne pas refaire le self-join.
- **Appliqué par** : `stg_yuman__clients`.

### Pause historique ou pause active
- **Symptôme** : un workorder `Closed` apparaît « en pause ».
- **Règle** : un workorder clos garde ses champs de pause, qui décrivent une pause historique.
  Seul un workorder encore en cours est en pause active.
- **Appliqué par** : `int_yuman__demands_workorders_enriched` (`is_workorder_currently_paused`).

---

## Règles métier

Le workorder est la base de la facturation. Sa qualification (`intervention_state`, flags,
délai en jours ouvrés, tarification automatique) est calculée une seule fois dans
`int_yuman__interventions`, pour éviter les chaînes fait → fait.

---

## Consommateurs

```bash
dbt ls -s source:yuman_api+
```
