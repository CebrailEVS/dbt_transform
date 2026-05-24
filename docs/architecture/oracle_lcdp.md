# Architecture — Oracle LCDP (`oracle_lcdp`)

> Dernière mise à jour : 2026-05-24

---

## Vue d'ensemble

Oracle LCDP est une **seconde instance** de l'ERP Oracle utilisée par EVS
pour le périmètre **LCDP** (entité métier dédiée), distincte de l'instance
principale `oracle_neshu`.

**Le schéma source, les conventions et les patterns dbt sont identiques à
`oracle_neshu`** : mêmes tables (`evs_task`, `evs_company`, `evs_device`,
`evs_product`, `evs_resources`, `evs_contract`, etc.), mêmes systèmes EAV
de labels, mêmes types de tâches.

> **Voir `docs/architecture/oracle_neshu.md`** pour le détail complet :
> ERD, système EAV, jointures, points d'attention génériques (filtre
> `idtask_type`, `code_status_record = '1'`, incrémental 7j, coefficients
> d'unité, `idcompany_peer` vs `idcompany`, etc.). Le présent document se
> limite aux **spécificités LCDP**.

> Voir `docs/pipeline-schedule.md` pour le cron et l'orchestration.

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

Identique à Neshu — 25 modèles, mêmes noms (préfixe `stg_oracle_lcdp__`),
mêmes patterns (cast IDs, harmonisation timestamps, filtre
`code_status_record = '1'`, `stg_oracle_lcdp__task` en incrémental
partitionné sur `real_start_date`).

### Couche intermediate — 11 modèles par type de tâche

Même découpage que Neshu, **avec des différences** :

| Présent dans LCDP | Absent côté LCDP (vs Neshu) |
|---|---|
| `int_oracle_lcdp__appro_tasks` (32) | `__invendus_tasks` (11) |
| `int_oracle_lcdp__chargement_tasks` (13) | `__appro_machine_context` |
| `int_oracle_lcdp__commande_interne_tasks` (132) | `__appro_tasks_enriched` |
| `int_oracle_lcdp__ecart_inventaire_tasks` (163) | `__valorisation_parc_machines` |
| `int_oracle_lcdp__inter_technique_tasks` (131) | |
| `int_oracle_lcdp__inventaire_tasks` (162) | |
| `int_oracle_lcdp__livraison_interne_tasks` (161) | |
| `int_oracle_lcdp__livraison_tasks` (101) | |
| `int_oracle_lcdp__pointage_tasks` (194) | |
| `int_oracle_lcdp__reception_tasks` (121) | |
| `int_oracle_lcdp__telemetry_tasks` (3) | |

> Note : `inter_technique` (vs `inter_techinique` côté Neshu — typo
> historique côté Neshu qui n'a pas été reproduite ici).

### Couche marts — 3 dimensions seulement

| Modèle | Rôle |
|---|---|
| `dim_lcdp__company` | Sociétés LCDP avec labels pivotés (région, secteur, statut client, ISACTIVE…) |
| `dim_lcdp__device` | Machines LCDP avec marque, gamme, catégorie, modèle économique |
| `dim_lcdp__product` | Produits LCDP avec marque, famille, groupe |

**Pas de facts LCDP en dbt aujourd'hui** — la BU LCDP ne consomme que les
dims + la table externe de monitoring (cf. section suivante). Pas de
`dim_lcdp__contract`, `dim_lcdp__resource`, ni `fct_lcdp__*` à ce stade.

### Snapshots

**Aucun snapshot LCDP** aujourd'hui (vs Neshu qui en a 3 :
`snap_oracle_neshu__company`, `snap_oracle_neshu__device`,
`snap_oracle_neshu__valo_parc_machines`).

### Source externe Cloud Run

`fct_lcdp__monitoring_passage_appro` — table écrite directement dans
`prod_marts` par le Cloud Run `ingest-oracle-lcdp-passages-appro` (cron
`pipeline-passages-appro-lcdp`). Déclarée comme source dans
`_lcdp__marts_sources.yml` au sein de `marts/lcdp/`. Pattern identique au
monitoring Neshu :
- Mode : TRUNCATE + full reload
- Historique 15 jours, statuts `PREVU` / `ENCOURS` / `FAIT`
- Refresh horaire jours ouvrés 07h–15h Europe/Paris
- Consommée par Power BI workspace **"Lcdp"** / dataset **"DEV APPRO MONITORING"**

Référencer via `source('marts_lcdp_external', 'fct_lcdp__monitoring_passage_appro')` —
ne pas wrapper dans un modèle dbt.

---

## Marts consommateurs

Domaine `marts/lcdp/` uniquement. Pas de croisement avec d'autres sources
aujourd'hui (LCDP est un périmètre métier isolé).
