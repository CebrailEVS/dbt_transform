# Conventions — Marts

Écrire un mart (`dim_*` / `fct_*`), la couche exposée à Power BI. Règles transversales :
[CONVENTIONS.md](../../CONVENTIONS.md).

## 1. Nommage

Les marts sont rangés **par BU**, pas par source : `models/marts/<bu>/`.

| Élément | Règle |
|---|---|
| Préfixe | `dim_` (dimension) ou `fct_` (fait) |
| BU | le nom du dossier : `neshu`, `lcdp`, `technique`, `commerce`, `finance`, `services_generaux`, `supply_chain`, `bi` |
| Entité | au singulier, en snake_case, avec un nom **métier** (ni le nom de la source, ni celui du rapport) |
| Suffixe de grain | seulement si le fait est agrégé au-dessus de son grain naturel : `_mensuel`, `_quinzaine` |
| Suffixe de source | seulement en cas de collision dans une même BU : `fct_supply_chain__stock_neshu` / `_stock_yuman` |
| YAML | `_<bu>__marts_models.yml` |

Exemples : `dim_neshu__company`, `fct_neshu__consommation`, `fct_neshu__chargement_quinzaine`,
`dim_technique__material`. Le nom du rapport Power BI va dans l'**exposure**, jamais dans le mart.

## 2. Description en 4 blocs

Toute description de mart suit cette trame. Le **grain est obligatoire**.

```yaml
- name: fct_neshu__chargement_consommation
  description: >
    [QUOI MÉTIER]
    Chargements et consommations télémétrie, par passage d'appro.

    [COMMENT CONSTRUITE]
    Croise les passages appro et la télémétrie en reconstruisant l'intervalle
    entre deux passages successifs (LAG sur task_start_date). Tâches FAIT depuis 2024.

    [GRAIN]
    1 ligne par (device, passage_appro, product). ~1,4 M lignes.

    [NOTES]
    Hors livraisons : voir fct_neshu__consommation pour la vue consolidée.
```

- Pas de nom de rapport dans la description.
- `[NOTES]` est facultatif : exclusions, pièges, renvois vers d'autres marts.

## 3. Modélisation : schéma en étoile strict

**Faits** (`fct_*`) : des événements ou des mesures, rattachés aux dimensions par des FK
`<entite>_id`. Un fait ne peut s'appuyer sur un autre que dans deux cas :
- un **agrégat** à un grain plus grossier (`GROUP BY`), par exemple
  `fct_supply_chain__disponibilite_article_neshu_depot_mensuel` sur `fct_supply_chain__stock_neshu` ;
- une **extension** à grain strictement identique (1:1), qui ajoute des colonnes calculées.

**Interdit** : joindre deux faits sur une dimension partagée pour combiner leurs mesures. Le
many-to-many double-compte. Pour croiser deux faits, pré-agréger chacun au grain commun, *puis*
joindre. Le critère est **le grain et la cardinalité**, jamais la BU.

**Dimensions** (`dim_*`) : Type 1 (état courant) par défaut, 1 ligne par entité.
- **Attributs d'affichage du parent direct** : 1 à 3 colonnes au maximum (`company_name` dans
  `dim_neshu__device`), la dim restant reliée au parent par `company_id`. Jamais la dim parente entière :
  ce serait de l'OBT déguisée.
- **Dimension conforme** : une même dim sert plusieurs faits, et même plusieurs BU
  (`fct_neshu__workorder_delai` → `dim_technique__client`).
- **SCD2 historisée** : suffixe `_history`, construite depuis un `snap_*`. Grain = 1 ligne par
  entité × période ; PK `<entite>_version_key` ; bornes `valid_from` / `valid_to` semi-ouvertes et
  `is_current`. Les bornes gardent leurs noms standard, sans suffixe `_at`. Jointure point-in-time :
  `event_ts >= valid_from and event_ts < valid_to`. Exemple : `dim_neshu__device_history`, à côté de
  `dim_neshu__device`.

**Clés** : quand l'entité n'a pas de PK naturelle, surrogate par `dbt_utils.generate_surrogate_key`.

### Dimension Oracle : pivot des labels

Les ERP Oracle (NESHU, LCDP) portent leurs attributs dans un système de labels (EAV). Les `ref()`
de staging restent par source ; la dim produite est par BU :

```sql
with entity_labels as (
    select e.*, l.code as label_code, lf.code as label_family_code
    from {{ ref('stg_oracle_neshu__entity') }} as e
    left join {{ ref('stg_oracle_neshu__label_has_entity') }} as lhe
        on e.identity = lhe.identity and lhe.idlabel is not null
    left join {{ ref('stg_oracle_neshu__label') }} as l on lhe.idlabel = l.idlabel
    left join {{ ref('stg_oracle_neshu__label_family') }} as lf on l.idlabel_family = lf.idlabel_family
),

aggregated_labels as (
    select
        ...,
        max(case when label_family_code = 'ISACTIVE' then label_code end) as is_active
    from entity_labels
    group by ...
)

select
    ...,
    coalesce(lower(is_active) = 'yes', false) as is_active
from aggregated_labels
```

## 4. Config

Le `config()` ne porte **que** la matérialisation. La description vit en YAML (`persist_docs` la
pousse dans BigQuery), et les tags dans `dbt_project.yml`.

```sql
{{ config(
    materialized='table',
    partition_by={'field': 'consumption_date', 'data_type': 'date'},
    cluster_by=['company_id', 'device_id']
) }}
```

Partition sur la date filtrée dans Power BI ; cluster sur les FK les plus jointes (4 au maximum).
Pas de partition sur les petites dimensions.

## 5. Ordre des colonnes (grain-first)

1. **Grain** : dimension temporelle → PK → FK du grain
2. FK restantes
3. Attributs texte (`*_name`, `*_code`, `*_status`)
4. Dates secondaires
5. Booléens (`is_*`, `has_*`)
6. Mesures
7. Métadonnées (`created_at`, `updated_at`, `extracted_at`)

Le grain en tête et les métadonnées en queue sont systématiques. Le reste est indicatif : on peut
coller `x_code` et `x_name` à leur FK si c'est plus lisible.

## 6. Nommage des mesures

Le préfixe dit **comment la mesure se calcule** :

| Préfixe | Nature | Calcul | Exemple |
|---|---|---|---|
| `qty_` | quantité physique | `sum` | `qty_chargee` |
| `nb_` | nombre d'événements | `count` | `nb_ventes` |
| `ca_` / `montant_` | euros | `sum(... * prix)` | `ca_cash_eur` |
| `taux_` / `pct_` | ratio, **non additif** | `safe_divide` | `taux_ecoulement_volume_4wk` |

- Un périmètre commun à tout le mart se documente au niveau du modèle, pas dans chaque nom de
  colonne : `qty_chargee`, pas `qty_vendable_chargee`.
- Préférer le mot métier (`ventes`, `invendus`) au mot technique (`entree`, `sortie`).
- Indiquer « additif / non additif » dans la description de chaque mesure.
- Suffixe de fenêtre glissante en fin de nom : `_4wk`, `_ytd`.

## 7. Tests minimum

| Test | Dim | Fait | Sévérité |
|---|---|---|---|
| PK `unique` + `not_null` | obligatoire | — | `error` |
| FK `not_null` | — | obligatoire | `error` |
| FK `relationships` vers la dim | — | obligatoire | `warn` |
| Clé composite `dbt_utils.unique_combination_of_columns` | — | obligatoire | `error` |
| `accepted_values` sur les statuts | recommandé | recommandé | `error` |
| Invariants (`expression_is_true`, bornes ≥ 0) | — | recommandé | `warn` |
| Volume (`expect_table_row_count_to_be_between`) | recommandé | recommandé | `warn` |

```yaml
- name: fct_<bu>__<entite>
  tests:
    - dbt_utils.unique_combination_of_columns:
        arguments:
          combination_of_columns: [event_date, device_id, product_id]
  columns:
    - name: device_id
      tests:
        - not_null
        - relationships:
            arguments:
              to: ref('dim_<bu>__device')
              field: device_id
            config:
              severity: warn
```

Une FK légitimement NULL (ex. `device_id` des livraisons dans `fct_neshu__consommation`) garde
son `relationships`, mais son `not_null` devient un `expression_is_true` qui décrit l'exception.

Explorer la source avant de fixer les valeurs (MCP BigQuery) : `select distinct` pour
`accepted_values`, `count(*)` pour les bornes de volume.

## 8. Anti-patterns

| À refuser | À la place |
|---|---|
| Jointure fait-à-fait pour combiner des mesures | pré-agréger au grain commun, puis joindre |
| Dim qui pointe vers une autre dim (snowflake) | aplatir 1 à 3 attributs d'affichage du parent |
| Un mart = un rapport, tout déjà joint (OBT) | schéma en étoile + mesures dans Power BI |
| `description=` ou `tags=` dans le `config()` | YAML / `dbt_project.yml` |
| Description sans grain | bloc `[GRAIN]` |
| Nom de rapport dans le nom ou la description | l'exposure |

## 9. Checklist

- [ ] `models/marts/<bu>/<dim|fct>_<bu>__<entite>.sql` + entrée dans `_<bu>__marts_models.yml`
- [ ] Description en 4 blocs, avec le grain
- [ ] Schéma en étoile : FK `<entite>_id`, pas de fait-à-fait, pas de snowflake
- [ ] Tests minimum (§ 7)
- [ ] `config()` = matérialisation uniquement
- [ ] Mesures nommées par nature ; colonnes dans l'ordre grain-first
- [ ] Exposure à jour si un rapport consomme le mart
- [ ] Revue par l'agent `mart-reviewer`, puis `dbt lint` OK
