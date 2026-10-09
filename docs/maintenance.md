# Maintenance — nettoyage des tables orphelines

Une table orpheline existe encore dans un dataset prod alors qu'aucun modèle, seed ou snapshot
dbt ne la produit plus (modèle supprimé ou renommé). dbt ne la supprime jamais seul.
Procédure manuelle, réservée au data engineer. Les datasets de dev n'en ont pas besoin : leurs
tables expirent après 14 jours sans rebuild.

## Outil

[`dbt_orphan`](https://github.com/Matts52/dbt-orphan) (`packages.yml`, installé depuis git). Il
liste les tables d'un dataset et signale celles qui ne correspondent à aucun nœud du projet
**résolu dans ce dataset**. Les snapshots sont reconnus.

Conséquence : il faut le lancer avec la **config prod**. Lancé depuis le target `dev`, tous les
nœuds résolvent vers `dbt_<toi>` et chaque table de `prod_*` apparaît orpheline.

## 1. Lister (lecture seule)

Avec sa propre identité Google (`gcloud auth application-default login`), qui doit pouvoir lire
les datasets scannés :

```bash
set -a && . ./.env && set +a
DBT_TARGET=prod DBT_BIGQUERY_METHOD=oauth DBT_BIGQUERY_DATASET_PROD=prod \
  ./dbt_venv/bin/dbt run-operation dbt_orphan.cleanup_orphans \
  --args '{schemas: ["prod_staging", "prod_intermediate", "prod_marts", "prod_reference"], dry_run: true}'
```

`dry_run: true` ne fait que lister. C'est le seul mode à utiliser depuis un poste.

## 2. Vérifier chaque table listée

- aucune exposure (rapport ou application) ne la déclare ;
- aucune lecture récente : `INFORMATION_SCHEMA.JOBS` (`referenced_tables`) sur 90 jours ;
- ce n'est pas une table créée hors dbt (externe, chargée par un pipeline).

## 3. Supprimer

Table par table, après validation, avec une identité qui a les droits en prod :
`bq rm -t evs-datastack-prod:<dataset>.<table>`. Jamais en post-hook ni dans un workflow
planifié. Jamais sur le dataset `snapshots` (historique SCD2 irréversible).

## Limite

La macro compare le **nom** du nœud, pas son `alias` : un modèle dont l'alias diffère du nom de
fichier serait vu comme orphelin.
