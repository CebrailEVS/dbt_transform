# Architecture — Yuman (API)

| | |
|---|---|
| Source dbt | `yuman_api` (`models/staging/yuman/_yuman__sources.yml`) |
| Pipeline dlt | `yuman_evs` (`ingestion/pipelines/yuman_evs`) |
| Tables raw | `prod_raw.yuman_evs_*` (une par entité : workorders, workorder_demands, purchase_orders, clients, sites, materials, users, contacts, products, catégories, products_storehouses) |
| Chargement | Snapshot complet (`replace`) à chaque run : le raw est l'état courant de l'API, sans historique |

Fraîcheur : `docs/freshness.md`. Cadence et orchestration : `docs/pipeline-schedule.md`.

Yuman est l'outil de gestion des interventions terrain (workorders) et des
demandes d'intervention. Il alimente la facturation, le pilotage du SAV et le
suivi des partenaires. Un second pipeline, `yuman_evs_stock`, apporte le stock
théorique que l'API n'expose pas : voir `docs/architecture/yuman_evs_sftp.md`.

Chaîne : `prod_raw` → `stg_yuman__*` → `int_yuman__*` → marts `technique/` et `neshu/`.

---

## Grain et clés

| Modèle | Grain | Clé |
|---|---|---|
| `stg_yuman__workorders` | 1 workorder (table centrale) | `workorder_id` |
| `stg_yuman__workorder_demands` | 1 demande d'intervention | `demand_id` |
| `stg_yuman__workorder_products` | 1 produit utilisé sur 1 workorder | `workorder_product_id` |
| `stg_yuman__purchase_orders` | 1 commande **x** 1 article | `purchase_order_line_id` |
| `stg_yuman__storehouses` | 1 entrepôt | `storehouses_id` |
| `stg_yuman__users` | 1 technicien ou manager interne | `user_id` |
| `stg_yuman__clients` / `sites` / `materials` / `contacts` / `products` | 1 entité | `<entité>_id` |
| `int_yuman__demands_workorders_enriched` | 1 couple (demande, workorder), `FULL JOIN` | `(demand_id, workorder_id)`, léger fan-out non dédupliqué |
| `int_yuman__interventions` | 1 couple (demande, workorder), dédupliqué | `(demand_id, workorder_id)` |

Valeurs de statut : demande = `Open`, `Accepted`, `Rejected`, `Closed` ;
workorder = `Scheduled`, `In progress`, `Closed`. Le détail est dans le YAML de
`int_yuman__demands_workorders_enriched`.

Les snapshots `snap_yuman__users` et `snap_yuman__storehouses` historisent les
changements de rattachement et les suppressions (SCD2). Ils sont pilotés par
Cloud Workflows, pas par le CI/CD (voir `CLAUDE.md`).

---

## Pièges

### `stg_yuman__contacts` n'a ni `client_id` ni `site_id`
- **Symptôme** : une jointure `contacts.site_id` ou `contacts.client_id` ne compile pas.
- **Règle** : ces colonnes ne sont pas exposées. Relier un contact à un site ou
  un client en passant par `workorders.contact_id` ou `workorder_demands.contact_id`.
- **Appliqué par** : `int_yuman__demands_workorders_enriched`.

### Les champs custom EVS vivent dans le JSON `_embed`
- **Symptôme** : une valeur métier (motif de non-intervention, raison de mise en
  pause, catégorie client EVS, localisation matériel, code postal du site,
  `ID NOMAD`, secteur, inactif) est introuvable en colonne.
- **Règle** : les champs personnalisés sont dans `_embed` sous `$.fields`
  (tables `workorders`, `clients`, `materials`, `users`, `sites`), extraits par
  `name`. Pour en ajouter un, modifier le staging concerné.
- **Appliqué par** : les staging de ces cinq tables.

### Deux staging sont dépliés depuis un tableau JSON
- **Symptôme** : une somme sur `purchase_orders` compte l'en-tête de commande
  autant de fois qu'il y a d'articles.
- **Règle** : `purchase_orders` déplie `lines` (en-têtes dupliqués par ligne,
  clé = `purchase_order_line_id`). `workorder_products` déplie `$.products` de
  `_embed` sur `yuman_evs_workorders`. Ne pas re-parser le JSON dans les marts.
- **Appliqué par** : `stg_yuman__purchase_orders`, `stg_yuman__workorder_products`.

### Demandes et workorders ne se recouvrent pas
- **Symptôme** : des demandes sans workorder, des workorders sans demande ; un
  `LEFT JOIN` perd l'une des deux populations.
- **Règle** : `workorder_id` est nullable sur les demandes (rejet, annulation,
  attente) et un workorder peut être créé sans demande. Jointure `FULL JOIN`.
  `int_yuman__interventions` déduplique le fan-out de l'enriched.
- **Appliqué par** : `int_yuman__demands_workorders_enriched`.

### Le produit d'un workorder porte deux libellés
- **Symptôme** : `product_reference` introuvable sur `stg_yuman__products`.
- **Règle** : le catalogue expose `product_code` et `product_name` ;
  `product_reference` et `product_designation` n'existent que sur
  `stg_yuman__workorder_products`, dénormalisés depuis le JSON. Pour joindre,
  passer par `product_id`.

### `storehouses_id = user_id`, sauf pour les ateliers
- **Symptôme** : un entrepôt sans technicien ni manager correspondant.
- **Règle** : un storehouse est le stock embarqué d'un user ; son id est celui
  du user propriétaire. Seuls les 4 ateliers physiques n'ont pas de user. Le
  test `relationships` vers `stg_yuman__users` est en `warn` : un orphelin
  hors de ces 4 ateliers signale une dérive.
- **Appliqué par** : `stg_yuman__storehouses`.

### Technicien → agence : jointure sur le nom
- **Symptôme** : technicien sans agence après un changement d'orthographe côté Yuman.
- **Règle** : Yuman ne porte pas l'agence. Le seed `ref_yuman__tech_agence` est
  joint sur le nom complet normalisé (majuscules, trim, préfixe `[INACTIF]`
  retiré). Tout nom modifié casse la jointure : mettre à jour le seed.
- **Appliqué par** : `int_yuman__demands_workorders_enriched`.

### `partner_name` est déjà résolu
- **Règle** : `partner_id` pointe vers un autre `client_id` de la même table ;
  le staging expose `partner_name`. Ne pas refaire le self-join.
- **Appliqué par** : `stg_yuman__clients`.

---

## Règles métier

- Le workorder est la base de la facturation ; sa qualification
  (`intervention_state`, flags, délai en jours ouvrés, tarification automatique)
  est calculée une seule fois dans `int_yuman__interventions`, pour éviter les
  chaînes fait → fait.
- Le mapping technicien → agence est centralisé en intermediate, pas dupliqué
  dans les marts.
- Sur un workorder `Closed`, les champs de pause décrivent une pause
  historique, pas une pause active.

---

## Consommateurs

```bash
dbt ls -s source:yuman_api+
```

`fct_commerce__machine_intervention` ne lit pas Yuman.
