# Conventions — Intermediate

Écrire un modèle `int_*`. Règles transversales : [CONVENTIONS.md](../../CONVENTIONS.md).

## 1. Rôle

La logique métier **à l'intérieur d'une source** : déduplication, enrichissement par les
référentiels, champs calculés, découpage par type de tâche, réconciliation de grain.

- **Aligné sur une source.** Le croisement de plusieurs sources se fait en marts.
- Lit uniquement `ref('stg_*')`, `ref('int_*')` et les seeds `ref('ref_*')`. Jamais `source()`.

## 2. Nommage

| Élément | Règle | Exemple |
|---|---|---|
| Fichier | `models/intermediate/<source>/int_<source>__<entite>.sql` | `int_oracle_lcdp__appro_tasks.sql` |
| Doc + tests | `_<source>__intermediate_models.yml` | `_oracle_lcdp__intermediate_models.yml` |

## 3. Pattern SQL

Une CTE par entrée, puis les transformations, puis l'assemblage :

```sql
{{ config(materialized='table') }}

with tasks as (
    select * from {{ ref('stg_oracle_lcdp__task') }}
),

companies as (
    select * from {{ ref('stg_oracle_lcdp__company') }}
),

enriched as (
    select
        t.idtask,
        t.idcompany_peer,
        c.name as company_name,
        date_diff(t.real_end_date, t.real_start_date, day) as duree_jours
    from tasks as t
    left join companies as c on t.idcompany_peer = c.idcompany
)

select * from enriched
```

Déduplication : `qualify row_number() over (partition by ... order by ...) = 1`.

## 4. Matérialisation

- `table` par défaut.
- `incremental` (`merge`) pour les gros volumes, soit 10 modèles aujourd'hui (tâches Oracle,
  `int_mssql_sage__pnl_bu`). Même clause `is_incremental()` qu'en staging ; `partition_by` sur la
  date filtrée, `cluster_by` sur les FK les plus jointes.

## 5. Documentation

La description se met **en YAML uniquement**, pas dans le `config()`. Elle est lue par l'agent
NL→SQL via le manifest : on documente pour lui **et** pour l'humain.

> Dette connue : 41 `int_*` portent encore une `description=` dans le `config()`. On la retire
> quand on touche le modèle, et on n'en ajoute pas.

**Description du modèle** : la même trame en 4 blocs que les marts
(`[QUOI MÉTIER]`, `[COMMENT CONSTRUITE]`, `[GRAIN]`, `[NOTES]`) ; cf. [marts.md § 2](marts.md#2-description-en-4-blocs).

**Colonnes** : documenter ce qu'un lecteur ne peut pas deviner.

| Type | À écrire |
|---|---|
| FK | l'entité cible (« FK → `stg_yuman__sites` ») |
| Enum | les valeurs et leur sens, **relevées dans BigQuery**, pas devinées ; + `accepted_values` (`warn`) |
| Mesure | l'unité, et si elle est additive |
| Date | l'événement qu'elle marque |
| Booléen | la condition exacte qui le met à `true` |

Modèle de référence : `_yuman__intermediate_models.yml` (`int_yuman__interventions`).

## 6. Tests minimum

- Grain : `unique` + `not_null` (composite → `dbt_utils.unique_combination_of_columns`).
- Recommandé : `relationships` sur les FK **introduites** ici, `dbt_utils.expression_is_true`
  sur les invariants des champs calculés.

## 7. Checklist

- [ ] `int_<source>__<entite>.sql` + entrée YAML
- [ ] `ref()` uniquement, une seule source
- [ ] Description YAML en 4 blocs, pas de `description=` dans le `config()`
- [ ] Grain testé
- [ ] `dbt lint` OK
