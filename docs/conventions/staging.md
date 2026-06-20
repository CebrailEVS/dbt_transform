# Conventions — couche Staging

> Doc de référence pour écrire un modèle `stg_*`. Règles transversales (format de
> nommage, SQLFluff, tags) : voir [`../../CONVENTIONS.md`](../../CONVENTIONS.md).
> Pattern marts : [`marts.md`](marts.md) · Intermediate : [`intermediate.md`](intermediate.md).

## 1. Rôle de la couche

Nettoyer et normaliser **une table source** : renommer, caster, harmoniser les
timestamps, exposer tous les champs utiles. **Une source de staging = une table
source.** Pas de logique métier, pas de jointure cross-source (ça vit en
intermediate/marts). Le staging est la seule couche qui lit `source()`.

## 2. Nommage

- Fichier : `stg_<source>__<entity>.sql` (`__` sépare source et entité).
- YAML : `_<source>__models.yml` (doc + tests) et `_<source>__sources.yml`
  (déclaration source + freshness), dans le dossier de la source.
- Entité en snake_case, au plus proche de la table source.

## 3. Colonnes

### Colonnes système — obligatoires sur tout staging

Chaque `stg_*` expose ces 4 colonnes harmonisées :

| Colonne | Type | Construction |
|---|---|---|
| `created_at` | TIMESTAMP | `timestamp(<date_creation_source>)` |
| `updated_at` | TIMESTAMP | `timestamp(coalesce(<date_modif>, <date_creation>))` |
| `extracted_at` | TIMESTAMP | `timestamp(_sdc_extracted_at)` (ou cast `safe.parse_timestamp` si STRING) |
| `deleted_at` | TIMESTAMP | `timestamp(_sdc_deleted_at)` |

### Nommage des colonnes — passthrough par défaut

**Règle : raw → staging ne renomme pas les colonnes.** On conserve les noms de la
source. Seules **trois** transformations de nom sont autorisées :

1. **Harmonisation des 4 colonnes système** (`creation_date`→`created_at`,
   `_sdc_extracted_at`→`extracted_at`, etc. — cf. colonnes système ci-dessus).
2. **Nettoyage d'un nom ambigu / non parlant**, typiquement un `id` nu →
   `<entity>_id`.
3. **Préfixage par l'entité** des colonnes **génériques** (`name`, `address`,
   `code`, `category`) → `client_name`, `site_address`… — **uniquement** quand la
   source expose ces noms passe-partout sur des entités qui sont **co-jointes en
   aval** (sinon, passthrough simple).

Vérifié sur les données (diff colonnes raw vs staging) :

| Source | Passthrough | Renommages |
|---|---|---|
| Oracle / company | 9/13 | uniquement les 4 colonnes système |
| Zoho / tickets | 42/43 | uniquement `id` → `ticket_id` |

> La normalisation complète en `<entity>_id` pour **toutes** les sources se fait
> au plus tard en marts. En staging, on reste fidèle à la source.

> **Cas du préfixage par entité — Yuman (pattern justifié).** Les `stg_yuman__*`
> préfixent les colonnes métier (`name`→`client_name`, `address`→`site_address`,
> `code`→`material_code`). Ce n'est **pas** une déviation : `int_yuman__demands_workorders_enriched`
> joint **9 entités Yuman** qui partagent toutes des noms génériques — sans préfixe,
> chaque jointure aval devrait désambiguïser à la main (≈190 références). Le préfixe
> est donc **load-bearing**. Règle : préfixer en staging **si et seulement si** la
> source a des noms génériques co-joints (cas Yuman) ; sinon, passthrough strict
> (cas Oracle, Zoho).

### Autres colonnes

- snake_case partout.
- Booléens : préfixe `is_` / `has_`.
- Dates : `_at` (timestamp) / `_date` (date).

## 4. Pattern SQL

CTE en 2-3 étapes (`source_data` → `cleaned_data` → `select` final) :

```sql
{{ config(
    materialized='table',
    description='<une ligne : quoi + source>'
) }}

with source_data as (
    select * from {{ source('oracle_neshu', 'evs_company') }}
),

cleaned_data as (
    select
        cast(idcompany as int64) as idcompany,   -- IDs castés
        code,
        name,                                     -- colonnes texte
        timestamp(creation_date) as created_at,   -- timestamps harmonisés
        timestamp(coalesce(modification_date, creation_date)) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
    from source_data
)

select * from cleaned_data
```

- **Casting** : `cast(x as int64/float64)`, `timestamp(x)`. Utiliser `safe_cast` /
  `safe.parse_timestamp` / `safe.parse_date` quand la source est STRING ou sujette
  au drift (fichiers GCS, taps custom).
- **Nettoyage** : `nullif(trim(x), '')` pour vider les chaînes vides.
- **Déduplication** (quand la source a des doublons) : 3ᵉ CTE `deduplicated_data`
  avec `row_number() over (...)` + `qualify row_number() = 1`.

## 5. Matérialisation

- **`table`** par défaut.
- **`incremental`** (stratégie `merge`) réservé aux grosses tables événementielles
  à PK stable — aujourd'hui les `task` Oracle (`stg_oracle_neshu__task`,
  `stg_oracle_lcdp__task`, `*_has_product`). `unique_key` obligatoire,
  `partition_by` sur la colonne du filtre incrémental. Clause :

  ```sql
  {% if is_incremental() %}
      where updated_at > (select max(updated_at) from {{ this }})
         or updated_at >= timestamp_sub(current_timestamp(), interval 7 day)
  {% endif %}
  ```
- `partition_by` / `cluster_by` : voir [`../../CONVENTIONS.md`](../../CONVENTIONS.md) (règles BigQuery) — cluster sur les FK les plus jointes en aval.

## 6. Description

- **Obligatoire dans le `{{ config() }}`** (`description='...'`, 1 ligne) — règle
  historique du projet, présente sur 100% des staging.
- YAML : description complémentaire (contexte métier) tolérée mais **non
  obligatoire** ; ne pas dupliquer mot pour mot la ligne du config.

## 7. Tests minimum

Syntaxe dbt ≥ 1.11 : arguments sous `arguments:`, severity sous `config:`.

### Obligatoire (severity `error` par défaut)

```yaml
columns:
  - name: <pk>                 # idcompany, ticket_id...
    tests: [unique, not_null]
  - name: <fk_obligatoire>
    tests:
      - not_null
      - relationships:
          arguments: {to: "ref('stg_<source>__<parent>')", field: <pk_parent>}
```

- PK composite → `dbt_utils.unique_combination_of_columns` (sous `tests:` model-level).

### Recommandé fort

- `accepted_values` sur les colonnes à liste fermée (statuts, types) — **peu
  répandu aujourd'hui (0/93), à généraliser** : un drift = un nouveau code à
  connaître.
- Pour les sources à timestamp STRING (pas de freshness native), test de
  fraîcheur **méthode B** : `dbt_expectations.expect_row_values_to_have_recent_data`
  sur `extracted_at` (cf. § 8).

## 8. Freshness

Deux mécanismes selon le type de timestamp source — **détail et état par source :
[`../freshness.md`](../freshness.md)** (autorité unique, ne pas redupliquer ici).

- **Méthode A** (`dbt source freshness`) : source avec TIMESTAMP/DATE natif →
  `loaded_at_field` + seuils dans `_<source>__sources.yml`.
- **Méthode B** (test `dbt_expectations` sur le staging) : source à timestamp
  STRING → test de récence sur la colonne castée du `stg_*`.

## 9. Anti-patterns

| Anti-pattern | À faire à la place |
|---|---|
| Jointure cross-source en staging | Rester sur 1 table source ; joindre en intermediate/marts |
| Logique métier / agrégation en staging | Pousser en intermediate |
| `source()` en intermediate/marts | `source()` **uniquement** en staging ; `ref()` ensuite |
| Oublier une des 4 colonnes système | Les exposer toutes (même `deleted_at` null) |
| `description` seulement en YAML | Obligatoire dans le `config()` pour le staging |
| Caster une source STRING fragile en `cast` dur | `safe_cast` / `safe.parse_*` |

## 10. Checklist avant PR

- [ ] Fichier `stg_<source>__<entity>.sql` + entrée dans `_<source>__models.yml`
- [ ] 4 colonnes système exposées et harmonisées
- [ ] `description='...'` dans le `config()`
- [ ] PK testée `unique` + `not_null` ; FK testées `relationships`
- [ ] Freshness configurée (méthode A ou B) si la table porte des événements
- [ ] `sqlfluff lint` OK
