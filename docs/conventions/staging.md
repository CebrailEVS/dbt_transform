# Conventions — Staging

Écrire un modèle `stg_*`. Règles transversales : [CONVENTIONS.md](../../CONVENTIONS.md).

## 1. Rôle

Nettoyer et typer **une table source**, sans logique métier. Le staging est la **seule** couche
qui lit `source()`.

- Une table source = un modèle de staging.
- Pas de jointure, pas d'agrégation, pas de déduplication métier (elle se fait en intermediate).

## 2. Nommage

| Élément | Règle | Exemple |
|---|---|---|
| Fichier | `stg_<source>__<table>.sql` | `stg_oracle_neshu__company.sql` |
| Doc + tests | `_<source>__models.yml` | `_oracle_neshu__models.yml` |
| Déclaration des sources | `_<source>__sources.yml` | `_oracle_neshu__sources.yml` |

## 3. Colonnes

**Passthrough** : on garde les noms de la source. Seuls trois renommages sont admis :

1. les colonnes système ci-dessous ;
2. un `id` nu → `<entite>_id` (`id` → `ticket_id`) ;
3. le préfixe par entité des noms génériques (`name`, `code`, `address`), **uniquement** si la
   source réutilise ces noms sur des entités jointes en aval. C'est le cas de Yuman
   (`client_name`, `site_address`) : `int_yuman__demands_workorders_enriched` joint 9 entités
   aux colonnes homonymes.

### Colonnes système

| Colonne | Règle | Construction |
|---|---|---|
| `extracted_at` | **obligatoire** : sert à la fraîcheur | `timestamp(_extracted_at)` (colonne posée par dlt) |
| `created_at` | si la source a une date de création | `timestamp(<date_creation>)` |
| `updated_at` | si la source a une date de modification | `timestamp(coalesce(<date_modif>, <date_creation>))` |

`deleted_at` n'existe plus : dlt ne réplique pas les suppressions, la colonne venait de Meltano
et était toujours `NULL`. Ne pas l'ajouter.

> Dette connue : `zoho_desk` expose encore `_extracted_at` sans le renommer.

Autres colonnes : snake_case, booléens `is_` / `has_`, timestamps `_at`, dates `_date`.

## 4. Pattern SQL

```sql
{{ config(
    materialized='table',
    description='Sociétés clientes NESHU (Oracle evs_company).'
) }}

with source_data as (
    select * from {{ source('oracle_neshu', 'evs_company') }}
),

cleaned_data as (
    select
        cast(idcompany as int64) as idcompany,
        nullif(trim(code), '') as code,
        name,
        timestamp(creation_date) as created_at,
        timestamp(coalesce(modification_date, creation_date)) as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select * from cleaned_data
```

- **Cast explicite de chaque colonne** qui traverse un modèle incrémental : sans cast, elle hérite
  du type du raw, et le `MERGE` casse au premier changement de type (incident `spantime`, juillet 2026).
- `safe_cast` / `safe.parse_timestamp` quand la source est en `STRING` ou instable (fichiers, API).
- `nullif(trim(x), '')` pour vider les chaînes vides.

## 5. Matérialisation

- `table` par défaut.
- `incremental` (`merge`, `unique_key` obligatoire) pour les grosses tables de tâches Oracle, soit
  6 modèles aujourd'hui (`stg_oracle_neshu__task`, `stg_oracle_lcdp__task`, `*_task_has_*`…) :

```sql
{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
       or updated_at >= timestamp_sub(current_timestamp(), interval 7 day)
{% endif %}
```

Pour tester un changement sur un incrémental : `dbt clone -s <modele>` puis `dbt run -s <modele>`.
C'est le vrai `MERGE` qui s'exécute, pas un `--full-refresh`.

## 6. Description

**Obligatoire dans le `config()`** (une ligne : quoi + source). Une description YAML
complémentaire est tolérée, mais ne doit pas répéter celle du `config()`.

## 7. Tests minimum

```yaml
columns:
  - name: idcompany               # PK
    tests: [unique, not_null]
  - name: idcompany_type          # FK obligatoire
    tests:
      - not_null
      - relationships:
          arguments:
            to: ref('stg_oracle_neshu__company_type')
            field: idcompany_type
```

- PK composite → `dbt_utils.unique_combination_of_columns` au niveau du modèle.
- Recommandé : `accepted_values` sur les statuts et les types.

## 8. Fraîcheur

Méthode et seuils par source : [docs/freshness.md](../freshness.md), la seule référence.

## 9. Checklist

- [ ] `stg_<source>__<table>.sql` + entrée dans `_<source>__models.yml`
- [ ] `description` dans le `config()`
- [ ] `extracted_at` exposé ; `created_at` / `updated_at` si la source les porte
- [ ] Colonnes castées explicitement (surtout en incrémental)
- [ ] PK `unique` + `not_null`, FK `relationships`
- [ ] `dbt lint` OK
