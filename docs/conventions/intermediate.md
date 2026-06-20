# Conventions — couche Intermediate

> Doc de référence pour écrire un modèle `int_*`. Règles transversales : voir
> [`../../CONVENTIONS.md`](../../CONVENTIONS.md). Staging : [`staging.md`](staging.md) ·
> Marts : [`marts.md`](marts.md).

## 1. Rôle de la couche

Consolider et enrichir **à l'intérieur d'une source** : déduplication, jointures
staging ↔ référentiels, champs calculés/dérivés, réconciliation de grain. C'est la
couche de logique métier au niveau entité/grain.

- **Aligné par source**, pas cross-source : aujourd'hui 0 modèle intermediate ne
  croise plusieurs sources. **L'unification multi-sources se fait en marts**, pas ici.
- Lit **uniquement `ref('stg_*')`** (et `ref('ref_*')` pour les seeds). Jamais
  `source()` (réservé au staging).

## 2. Nommage

- Fichier : `int_<source>__<entity>.sql`, dans le dossier de la source
  (`models/intermediate/<source>/`).
- YAML : `_<source>__intermediate_models.yml`.

## 3. Colonnes

- Passthrough des colonnes staging utiles + colonnes **dérivées/calculées**
  (délais, flags, statuts canoniques, comptages).
- snake_case ; booléens `is_`/`has_` ; dates `_at`/`_date`.
- Les colonnes système staging (`created_at`…) sont relayées telles quelles si
  pertinentes en aval.

## 4. Pattern SQL

CTEs nommées : base(s) `ref('stg_*')` → référentiels `ref('ref_*')` →
transformations → assemblage final.

```sql
{{ config(materialized='table') }}

with base as (
    select * from {{ ref('stg_oracle_lcdp__task') }}
),

company as (
    select * from {{ ref('stg_oracle_lcdp__company') }}
),

enriched as (
    select
        b.task_id,
        b.company_id,
        c.company_name,                 -- enrichissement via lookup
        date_diff(b.real_end_date, b.real_start_date, day) as duration_days  -- champ calculé
    from base as b
    left join company as c on b.company_id = c.idcompany
)

select * from enriched
```

- **Déduplication** : `row_number() over (...)` + `qualify` (cf. patterns staging).
- **Champs calculés** : délais, jours ouvrés, flags de validation, etc.

## 5. Matérialisation

- **`table`** par défaut (~82 %).
- **`incremental`** (stratégie `merge`) pour les gros volumes événementiels :
  tâches Oracle (`int_oracle_lcdp__*_tasks` sur `task_id`, partition
  `task_start_date`), `int_mssql_sage__pnl_bu` (clé composite). `partition_by` sur
  la date de filtre, `cluster_by` sur les FK les plus jointes en aval
  (ex. `int_yuman__interventions` : partition `date_done`, cluster `client_id,
  site_id, material_id`).
- Clause `is_incremental()` : même pattern que le staging (`updated_at >` max +
  fenêtre glissante de sécurité).

## 6. Description

- **En YAML uniquement**, pas dans le `{{ config() }}` (`persist_docs` pousse la
  description YAML vers BigQuery). Tout modèle a une entrée YAML.
- **Dette connue** : une partie des `int_*` existants portent encore une
  `description=` dans le config — à retirer au fil des modifications, ne pas en
  ajouter de nouvelle.

## 7. Tests minimum

Syntaxe dbt ≥ 1.11 (`arguments:` + `config:`).

### Obligatoire

```yaml
columns:
  - name: <pk_ou_grain>
    tests: [unique, not_null]
```

- Grain composite → `dbt_utils.unique_combination_of_columns` (model-level).

### Recommandé

- `relationships` sur les FK structurantes (souvent déjà validé en staging — ne
  pas redoubler partout, cibler les FK nouvelles introduites par l'intermediate).
- `expression_is_true` (`dbt_utils`) sur les invariants métier des champs calculés.

## 8. Freshness

Non applicable à l'intermediate : la fraîcheur se monitore en amont (sources /
staging). Voir [`../freshness.md`](../freshness.md).

## 9. Anti-patterns

| Anti-pattern | À faire à la place |
|---|---|
| Jointure cross-source en intermediate | Rester source-aligné ; unifier en marts |
| `source()` au lieu de `ref()` | `ref('stg_*')` uniquement |
| Agrégation au grain d'un rapport / pivot BI | Pousser en marts |
| `description=` dans le `config()` | Description en YAML uniquement |
| Logique de star schema (FK dimensionnelles, mesures finales) | C'est le rôle des marts |

## 10. Checklist avant PR

- [ ] Fichier `int_<source>__<entity>.sql` + entrée dans `_<source>__intermediate_models.yml`
- [ ] `ref()` uniquement (aucun `source()`)
- [ ] Pas de jointure cross-source
- [ ] PK/grain testé `unique` + `not_null` (composite → `unique_combination_of_columns`)
- [ ] Description en YAML (pas dans le config)
- [ ] `sqlfluff lint` OK
