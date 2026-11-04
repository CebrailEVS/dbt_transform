# Conventions — Staging

Écrire un modèle `stg_*`. Règles transversales : [CONVENTIONS.md](../../CONVENTIONS.md).

## 1. Rôle

Nettoyer et typer **une table source**, sans logique métier. Le staging est la **seule** couche
qui lit `source()`.

- Une table source = un modèle de staging, 1 pour 1 : ni jointure, ni agrégation, ni filtre métier.
- **Pas de dédoublonnage préventif.** La clé porte un test `unique` en `error` ; si un doublon
  apparaît, le build casse et on corrige en connaissant la cause. Un `qualify row_number()` n'est
  admis que pour un doublon technique ainsi révélé, avec un commentaire qui en dit la cause. Un
  dédoublonnage préventif garderait une ligne en silence et masquerait l'anomalie. Le dédoublonnage
  métier (choisir la ligne qui fait foi) se fait en intermediate.

## 2. Nommage

`models/staging/<source>/stg_<source>__<table>.sql` ; doc et tests dans `_<source>__models.yml`,
tables déclarées dans `_<source>__sources.yml` ([CONVENTIONS.md § Nommage](../../CONVENTIONS.md#nommage)).

## 3. Colonnes

**Passthrough** : on garde les noms de la source. Seuls trois renommages sont admis :

1. les colonnes système ci-dessous ;
2. un `id` nu → `<entite>_id` (`id` → `ticket_id`) ;
3. le préfixe par entité des noms génériques (`name`, `code`, `address`), **uniquement** si la
   source réutilise ces noms sur des entités jointes en aval. C'est le cas de Yuman
   (`client_name`, `site_address`), dont l'intermediate joint plusieurs entités aux colonnes
   homonymes.

### Colonnes système

| Colonne | Règle | Construction |
|---|---|---|
| `extracted_at` | **obligatoire** : sert à la fraîcheur | `timestamp(_extracted_at)` (colonne posée par dlt) |
| `created_at` | si la source a une date de création | `timestamp(<date_creation>)` ; ERP Oracle : voir plus bas |
| `updated_at` | si la source a une date de modification | `timestamp(coalesce(<date_modif>, <date_creation>))` ; ERP Oracle : voir plus bas |

Pas de `deleted_at` : dlt ne réplique pas les suppressions, la colonne serait toujours `NULL`.

### Dates des ERP Oracle

Oracle (Distrilog) stocke l'heure de Paris sans fuseau, et dlt la charge en `TIMESTAMP` comme si
c'était de l'UTC : l'instant est faux de 1 h l'hiver, de 2 h l'été. Le staging déclare le fuseau
d'origine, y compris pour `created_at` et `updated_at`.

| La colonne porte | Construction | Type |
|---|---|---|
| un instant (heure significative) | `{{ paris_vers_utc('<col>') }}` | `TIMESTAMP` (UTC) |
| une date calendaire (toujours à minuit) | `date(<col>)` | `DATE` |

Une date calendaire convertie en instant recule d'un jour : minuit à Paris, c'est la veille en UTC.
En aval, le jour métier d'un instant se prend avec `date(<col>, 'Europe/Paris')` (règle des autres
couches : [CONVENTIONS.md § Fuseau horaire](../../CONVENTIONS.md#fuseau-horaire)). L'heure qui se
répète au passage à l'heure d'hiver est ambiguë : BigQuery retient la première (heure d'été), la
source n'ayant rien pour trancher.

### Montants des ERP Oracle

Un `NUMBER` Oracle sans précision arrive en `FLOAT64`. Les montants passent en `numeric`
(`cast(<col> as numeric)`) : un flottant ne représente pas exactement les centimes.

Une colonne que la source nomme déjà `is_` / `has_` est castée en `bool` : son nom promet un
booléen. Autres règles de colonnes : [CONVENTIONS.md § Nommage](../../CONVENTIONS.md#nommage).

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
        {{ paris_vers_utc('creation_date') }} as created_at,
        {{ paris_vers_utc('coalesce(modification_date, creation_date)') }} as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select * from cleaned_data
```

- **Cast explicite de chaque colonne** qui traverse un modèle incrémental : sans cast, elle hérite
  du type du raw, et le `MERGE` casse au premier changement de type. `--full-refresh` ne le révèle
  pas, puisqu'il recrée la table : après une évolution de type, construire **sans**.
- `safe_cast` / `safe.parse_timestamp` quand la source est en `STRING` ou instable (fichiers, API).
- `nullif(trim(x), '')` pour vider les chaînes vides.

## 5. Matérialisation

- `table` par défaut.
- `incremental` (`merge`, `unique_key` obligatoire) pour les grosses tables de tâches Oracle. La
  fenêtre de recouvrement rattrape les lignes modifiées en retard :

```sql
{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
       or updated_at >= timestamp_sub(current_timestamp(), interval 7 day)
{% endif %}
```

Pour tester un changement sur un incrémental : `dbt clone -s <modele>` puis `dbt run -s <modele>`.
C'est le vrai `MERGE` qui s'exécute, pas un `--full-refresh`.

### Table d'historique sans clé : l'ajout seul

Une table d'historique sans clé, chargée en ajout seul et où la source écrit des doublons exacts
légitimes (ex. `stg_oracle_neshu__task_historic`), s'écarte du modèle ci-dessus :

- `incremental` (`merge`) **sans `unique_key`** : dbt insère les lignes nouvelles sans rien fusionner ;
- un filtre sur `extracted_at` **sans fenêtre de rattrapage** : sans clé, une ligne relue serait doublée ;
- la lecture des seuls chargements dlt complets (`_dlt_load_id` présent dans `_dlt_loads`,
  `status = 0`) : un run qui échoue après le chargement de la table y laisse des lignes que sa relance
  réajoute.

Pas de test d'unicité ; le regroupement des doublons se fait en intermediate. Un rechargement
complet du raw réécrit chaque ligne avec un `extracted_at` neuf : le modèle se reconstruit alors en
`--full-refresh`, sinon il doublerait.

## 6. Description

**Obligatoire à deux endroits** :
- dans le `config()` : une ligne (quoi, table source), pour qui lit le SQL ;
- dans le YAML, au niveau du modèle : c'est la seule que dbt v2 publie. La description du
  `config()` n'atteint ni le manifest ni BigQuery ; sans description YAML, la table sort vide dans
  la doc dbt et pour l'agent text-to-SQL.

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

- PK composite, ou table de jonction → `dbt_utils.unique_combination_of_columns` au niveau du modèle.
- Recommandé : `accepted_values` (`warn`) sur les statuts et les types.
- Sévérités : [CONVENTIONS.md § Tests](../../CONVENTIONS.md#tests).

## 8. Fraîcheur

Seuils dans `_<source>__sources.yml` ; méthodes et paliers : [docs/freshness.md](../freshness.md).

## 9. Checklist

- [ ] `stg_<source>__<table>.sql` + entrée dans `_<source>__models.yml`
- [ ] `description` dans le `config()` **et** dans le YAML
- [ ] Pas de `qualify` préventif ; PK testée `unique` en `error`
- [ ] `extracted_at` exposé ; `created_at` / `updated_at` si la source les porte
- [ ] Colonnes castées explicitement (surtout en incrémental)
- [ ] PK `unique` + `not_null`, FK `relationships`
- [ ] `dbt lint` OK

## 10. Écrire un staging avec Claude

Claude peut écrire un staging. Le staging fixe le sens et le type de chaque colonne pour toutes les
couches au-dessus : il suit donc un cadre strict, et l'utilisateur valide deux fois.

1. **Comprendre la source.** Lire sa page `docs/architecture/<source>.md` (pièges connus). Profiler
   la table dans `prod_raw` : types réels, part de null, cardinalité, valeurs des colonnes d'état ou
   de type, plage des dates et leur fuseau, clé candidate (`count(*)` contre
   `count(distinct <clé>)`).
2. **Proposer avant d'écrire.** Présenter à l'utilisateur, en une fois : la clé retenue et sa preuve,
   les renommages, les casts, la matérialisation, les tests, et chaque hypothèse sur la donnée.
   Rien ne s'écrit sans son accord.
3. **Écrire selon cette page.** Une table source, un modèle : ni jointure, ni filtre métier, ni
   dédoublonnage préventif ; `extracted_at` ; casts explicites ; description dans le `config()` et
   le YAML ; PK `unique` + `not_null`, FK `relationships`. Source déclarée dans
   `_<source>__sources.yml` avec sa fraîcheur, tag dans `dbt_project.yml`, page
   `docs/architecture/` si la source a des pièges.
4. **Vérifier.** Build en dev (dataset vidé en début de branche), tests, `dbt lint`, `dbt check` ;
   nombre de lignes égal à celui du raw.
5. **Faire valider la PR.** L'utilisateur relit et valide avant le merge, par exception à
   l'autonomie de merge de Claude.
