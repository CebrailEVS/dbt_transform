# Architecture — Oracle LCDP (`oracle_lcdp`)

> Dernière mise à jour : 2026-05-24

---

## Vue d'ensemble

Oracle LCDP est une **seconde instance** de l'ERP Oracle utilisée par EVS
pour le périmètre **LCDP** (entité métier dédiée), distincte de l'instance
principale `oracle_neshu`.

**Le schéma source, les conventions et les patterns dbt sont identiques à
`oracle_neshu`** : mêmes tables (`task`, `company`, `device`, `product`,
`resources`, `contract`, etc., préfixées `lcdp_` en raw — contre `evs_` côté
Neshu), mêmes systèmes EAV de labels, mêmes types de tâches.

> **Voir `docs/architecture/oracle_neshu.md`** pour le détail complet :
> ERD, système EAV, jointures, points d'attention génériques (filtre
> `idtask_type`, `code_status_record = '1'`, incrémental 7j, coefficients
> d'unité, `idcompany_peer` vs `idcompany`, etc.). Le présent document se
> limite aux **spécificités LCDP**.

> Orchestration et régime de cadence : `docs/pipeline-schedule.md`.
> L'horaire exact vit dans `infra/workflows_el.tf`.

---

## Spécificités LCDP par rapport à Neshu

### Volumétrie (mai 2026)

Périmètre nettement plus petit que Neshu :

| Entité | LCDP | Neshu (ordre de grandeur) |
|---|---|---|
| Sociétés (`stg_oracle_lcdp__company`) | 918 | beaucoup plus |
| Machines (`stg_oracle_lcdp__device`) | 2 776 | beaucoup plus |
| Produits (`stg_oracle_lcdp__product`) | 882 | comparable |
| Tâches (`stg_oracle_lcdp__task`) | ~5,3 M | beaucoup plus |
| Produits sur tâches (`stg_oracle_lcdp__task_has_product`) | ~4,8 M | beaucoup plus |

### Couche staging

Même préfixe (`stg_oracle_lcdp__`), mêmes patterns qu'oracle_neshu (cast IDs,
harmonisation timestamps, filtre `code_status_record = '1'`,
`stg_oracle_lcdp__task` en incrémental partitionné sur `real_start_date`).
Liste à jour des modèles : [dbt docs](https://cebrailevs.github.io/dbt_transform/)
(filtre `tag:oracle_lcdp`).

### Couche intermediate — un modèle par type de tâche

Même découpage que Neshu, **avec un périmètre de types de tâche plus large**
côté LCDP : en plus des types communs (appro `32`, chargement `13`, commande
interne `132`, écart inventaire `163`, inter technique `131`, inventaire
`162`, invendus `11`, livraison `101`, livraison interne `161`, pointage
`194`, réception `121`/commande fournisseur `120`, télémétrie `3`), LCDP a des
types spécifiques à son activité de fabrication/distribution : appel SAV
(`130`), comptage (`30`), entrée fabrication (`296`), sortie fabrication
(`297`). `__appro_tasks_enriched` (vue enrichie des passages appro) existe
aussi côté LCDP. Absents côté LCDP : `__appro_machine_context`,
`__valorisation_parc_machines`.

Liste à jour des modèles : [dbt docs](https://cebrailevs.github.io/dbt_transform/)
(filtre `tag:oracle_lcdp`).

> Note : `inter_technique` (vs `inter_techinique` côté Neshu — typo
> historique côté Neshu qui n'a pas été reproduite ici).

### Couche marts

Les marts LCDP vivent dans `models/marts/lcdp/` (dimensions company, device, product,
resource et faits d'activité) ; il n'y a pas de dimension contrat. Liste à jour et rôle de
chaque modèle : [dbt docs](https://cebrailevs.github.io/dbt_transform/) (filtre `tag:lcdp`).

### Snapshots

Un snapshot LCDP : `snap_lcdp__device` (vs Neshu qui en a 3 :
`snap_oracle_neshu__company`, `snap_oracle_neshu__device`,
`snap_oracle_neshu__valo_parc_machines`).

---

## Marts consommateurs

Domaine `marts/lcdp/` principalement, mais LCDP alimente aussi deux facts
`marts/supply_chain/` : `fct_supply_chain__stock_lcdp` et
`fct_supply_chain__disponibilite_article_lcdp_depot_mensuel`.
