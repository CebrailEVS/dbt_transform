# Marts — pattern complet

> Doc de référence pour écrire un mart (`dim_*` / `fct_*`) : nommage, modélisation,
> description, config, tests. Règles transversales : [`../../CONVENTIONS.md`](../../CONVENTIONS.md).
> Staging : [`staging.md`](staging.md) · Intermediate : [`intermediate.md`](intermediate.md).

## Nommage des marts — convention by BU

Les marts sont organisés par **BU/domaine** (folder = BU), pas par source. Le nom
reflète la BU et l'entité métier, pas l'implémentation source.

| Élément | Règle |
|---------|-------|
| Préfixe | `dim_` (dimension) ou `fct_` (fait) |
| Clé BU/domaine | nom exact du folder : `neshu`, `lcdp`, `technique`, `commerce`, `finance`, `services_generaux`, `supply_chain` |
| Séparateur | `__` entre BU et entité |
| Entité | singulier, snake_case, nom métier (pas le nom source, pas le nom du rapport BI) |
| Suffixe de grain | uniquement si agrégé au-dessus du grain naturel (`_quinzaine`, `_mensuel`) |
| Suffixe de source | uniquement en cas de collision dans le même folder (ex. `fct_supply_chain__stock_neshu` vs `fct_supply_chain__stock_yuman`) |
| Nom du rapport BI | jamais dans le nom du mart — va dans l'`exposure` |

Exemples (avant → après) :

| Avant | Après | Pourquoi |
|-------|-------|----------|
| `fct_oracle_neshu__conso_business_review` | `fct_neshu__consommation` | source droppée, nom de rapport BI déplacé vers exposure |
| `fct_mssql_sage__pnl_bu_kpis` | `fct_finance__pnl_bu` | `_kpis` implicite dans un fait |
| `dim_yuman__materials` | `dim_technique__material` | singulier, pas de source |
| `fct_oracle_neshu__chargement_par_quinzaine` | `fct_neshu__chargement_quinzaine` | suffixe de grain conservé |

YAML : `_<bu>__marts_models.yml` (modèles) · `_<bu>__marts_sources.yml` (tables externes Cloud Run).

## 1. Principes de modelisation

Les marts suivent **strictement un modele en etoile** : un fait au centre,
des dimensions autour, jointures via `<entite>_id`. Pas de snowflake.
Pas de One Big Table.

- **Faits** (`fct_*`) : evenements ou mesures, pointent vers les dims via FK.
  Un fait **peut** en referencer un autre dans deux cas seulement :
  - (a) **fait agrege / rollup** a un grain plus grossier via `GROUP BY`
    (ex. `fct_supply_chain__disponibilite_article_neshu_depot_mensuel`, rollup
    mensuel de `fct_supply_chain__stock_neshu`) ;
  - (b) **fait derive / extension** a grain strictement identique 1:1 (ajout
    de colonnes calculees, grain inchange).

  **Interdit** : joindre deux faits sur leurs cles de dimension pour combiner
  leurs mesures — la dimension partagee cree un fan-out many-to-many qui
  double compte les mesures additives. Pour combiner deux faits, faire un
  drill-across (pre-agreger chacun au grain commun, *puis* joindre), dans
  l'intermediate ou dans la couche Power BI. Le critere de decision est
  **grain + cardinalite de jointure**, jamais l'appartenance BU : un
  fait-a-fait meme BU peut double compter, un agregat cross-BU peut etre sain.
- **Dimensions** (`dim_*`) : par defaut **Type 1** (etat courant), 1 ligne par
  entite metier (ticket, compte, agent, machine, contrat...).
  - **SCD Type 2 (dimension historisee)** : pour conserver l'historique des
    attributs et permettre une jointure « etat a la date » (point-in-time),
    le grain est **1 ligne par entite x periode de validite** (et non 1 ligne
    par entite). Conventions : PK = surrogate de version
    (`<entite>_version_key`), bornes `valid_from`/`valid_to` + flag
    `is_current`, suffixe **`_history`**. Elle coexiste avec la Type 1
    (ex. `dim_neshu__device` courant + `dim_neshu__device_history` versionnee).
    Construite a partir d'un snapshot dbt (`ref('snap_*')`) ; les faits y
    accedent en point-in-time join (`date between valid_from and valid_to`).
  - **Aplatir uniquement les attributs d'affichage du parent direct** (1-3
    colonnes max) pour eviter une jointure cote consommateur. Ex :
    `dim_neshu__device` contient `company_name` pour les tooltips
    et libelles, mais reste rattache a `dim_neshu__company` via `company_id`.
  - **Ne JAMAIS aplatir une dim parente entiere** (toutes ses colonnes :
    adresse, code postal, telephone, etc.) dans la dim enfant : c'est de
    l'OBT (One Big Table) deguise. Garder les dims separees et conformes,
    croiser via FK + relationships PBI ou SQL custom dans Power Query.
    Pattern hybride accepte : "star schema dbt + flatten en PBI quand besoin,
    promotion en mart si duplique 3+ fois".
- **Cles** : FKs en `<entite>_id` dans les faits. Cles surrogates via
  `dbt_utils.generate_surrogate_key` quand l'entite n'a pas de PK naturelle.
- **Conformed dimensions** : une meme dim sert plusieurs faits, voire
  plusieurs BU. Un fact `fct_neshu__workorder_delai` peut referencer
  `dim_technique__client` sans probleme — c'est exactement le pattern
  Kimball.
- **Grain explicite** : chaque fait declare son grain dans la description
  YAML. Pilier Kimball : "si tu ne sais pas le grain, tu ne sais pas le mart".

## 2. Trame de description (YAML)

Toute description de mart (dim ou fact) suit une trame en 4 blocs.
Separes par une ligne vide pour la lisibilite dans dbt docs.

```yaml
- name: fct_neshu__chargement_consommation
  description: >
    [QUOI METIER]
    Table de faits des chargements et consommations telemetrie par passage APPRO.

    [COMMENT CONSTRUITE]
    Croise les passages appro avec les telemetries via reconstruction
    d'intervalles entre 2 passages successifs (LAG sur task_start_date).
    Filtre sur taches statut FAIT depuis 2024.

    [GRAIN]
    1 ligne par (device, passage_appro, product). ~1.4M lignes.

    [NOTES]
    Ne contient pas les livraisons (voir fct_neshu__consommation pour la
    vue consolidee multi-sources).
```

Pour une dim, meme structure mais souvent plus courte :

```yaml
- name: dim_neshu__company
  description: >
    [QUOI METIER]
    Dimension client Neshu — societes enrichies des labels EAV
    (region, secteur, statut, KA, teletravail, etc.).

    [COMMENT CONSTRUITE]
    Pivot des labels via int_oracle_neshu__company_labels.
    Une ligne par societe active (label ISACTIVE='yes').

    [GRAIN]
    1 ligne par company_id (PK).
```

**Regles** :
- **Grain obligatoire** sur faits ET dims (1 ligne par X).
- **Pas de nom de rapport BI** dans la description — ca vit dans l'`exposure`.
- **[NOTES] facultatif** mais utile pour exclusions / pieges courants /
  references croisees vers d'autres marts.

## 3. Config block hygiene

Le `{{ config() }}` SQL gere uniquement la **materialisation**. La
**description** vit dans le YAML uniquement (single source of truth).
`persist_docs` est active dans `dbt_project.yml` — la description YAML
est automatiquement poussee vers BigQuery.

**Bon** :
```sql
{{ config(
    materialized='table',
    partition_by={'field': 'consumption_date', 'data_type': 'date'},
    cluster_by=['company_id', 'device_id']
) }}
```

**Anti-pattern (eviter)** :
```sql
{{ config(
    materialized='table',
    description='...'   -- doublon avec le YAML, source de drift
) }}
```

Pas non plus de `tags=[...]` dans le `{{ config() }}` — les tags sont
geres au niveau folder via `dbt_project.yml`. Exception : `cross_post_*`
si pertinent (cf. `docs/pipeline-schedule.md`).

## 4. Tests minimum par layer

Deux niveaux d'exigence : **obligatoire** (severity `error` par defaut)
et **recommande fort** (severity `warn` pour catcher les drifts silencieux).

### Dim — obligatoire

```yaml
columns:
  - name: <pk>                    # company_id, device_id, etc.
    tests:
      - unique
      - not_null
```

### Dim — recommande fort

```yaml
columns:
  - name: <date_col>              # created_at, updated_at, etc.
    tests:
      - dbt_expectations.expect_column_values_to_be_between:
          arguments:
            min_value: "timestamp('2010-01-01')"
            max_value: "current_timestamp()"
          config:
            severity: warn

  - name: <statut_col>            # is_active, status_code, etc.
    tests:
      - accepted_values:
          arguments:
            values: ['ACTIF', 'INACTIF']

tests:
  - dbt_expectations.expect_table_row_count_to_be_between:
      arguments:
        min_value: 100
        max_value: 50000
      config:
        severity: warn
```

### Fact — obligatoire

```yaml
columns:
  - name: <fk>                    # company_id, device_id
    tests:
      - not_null
      - relationships:
          arguments:
            to: ref('dim_neshu__company')
            field: company_id
          config:
            severity: warn

  - name: <partition_date>        # consumption_date, task_start_date
    tests:
      - not_null
```

### Fact — recommande fort

```yaml
columns:
  - name: <numeric_invariant>     # quantity, montant, etc.
    tests:
      - dbt_expectations.expect_column_values_to_be_between:
          arguments:
            min_value: 0
          config:
            severity: warn

tests:
  - dbt_utils.unique_combination_of_columns:    # cle composite
      arguments:
        combination_of_columns: [device_id, date, product_id]

  - dbt_expectations.expect_table_row_count_to_be_between:
      arguments:
        min_value: 1000
        max_value: 10000000
      config:
        severity: warn
```

### Severity strategy

| Test | Severity | Raison |
|------|----------|--------|
| PK `unique` + `not_null` sur dim | `error` | Casse les jointures sinon |
| `not_null` sur FK obligatoire | `error` | Orphelin = bug data |
| `relationships` sur FK | `warn` | Detecte les drifts sans bloquer le build |
| `accepted_values` | `error` | Liste fermee — drift = nouveau code a connaitre |
| Plages dates / numeriques | `warn` | Informatif, ne pas bloquer la prod |
| `expect_table_row_count_to_be_between` | `warn` | Alerte de volume anormal |

## 5. Anti-patterns a refuser

| Anti-pattern | Pourquoi c'est interdit | A faire a la place |
|--------------|------------------------|-------------------|
| Jointure fait-a-fait dans Power BI | Explosion cartesienne, mesures fausses | Croiser au niveau intermediate ou via dim partagee |
| Dim qui pointe vers une autre dim (`device → company`) | Snowflake, requetes plus lentes, complexite Power BI | Aplatir les attributs d'affichage parents dans la dim enfant (1-3 colonnes max) |
| Mart "OBT" 1 modele = 1 rapport, tout deja joint | Duplication, perte de reutilisabilite, refresh long | Star schema + mesures Power BI |
| FK manquante dans un fait | Ligne orpheline silencieuse | Test `relationships` sur la FK (severity warn ou error) |
| Nom de rapport BI dans la description du mart | La doc rote quand le rapport est renomme / supprime | Referencer le mart dans l'`exposure` du rapport |
| `description='...'` dans `{{ config() }}` ET dans YAML | Drift garanti | Description en YAML uniquement |
| `tags=[...]` au niveau model | Casse l'organisation par folder | Tags via `dbt_project.yml` (sauf exceptions `cross_post_*`) |
| Pas de grain dans la description | "Mart en aveugle", impossible a auditer | Ligne `[GRAIN]` explicite |

## 6. Pattern type SQL (squelette)

```sql
-- fct_<bu>__<event>.sql
{{ config(
    materialized='table',
    partition_by={'field': 'event_date', 'data_type': 'date'},
    cluster_by=['ticket_id']
) }}

with events as (
    select * from {{ ref('int_<source>__<event>') }}
)

select
    ticket_id,         -- FK vers dim_<bu>__ticket
    account_id,        -- FK vers dim_<bu>__account
    agent_id,          -- FK vers dim_<bu>__agent
    event_date,
    event_type,
    duration_minutes   -- mesures additives uniquement
from events
```

Description et tests vivent dans `_<bu>__marts_models.yml` (voir §2 et §4).

## 7. Ordre des colonnes du `select` final

Convention **indicative** (non verifiee par SQLFluff, mais attendue en review).
Regle dite **grain-first** : les colonnes du grain ouvrent le `select`, puis on
reprend un tri par role. Objectif : lire « de quoi parle la ligne » (cles →
contexte → quand) avant « combien » (mesures).

1. **Grain** — la/les colonne(s) qui definissent le grain, dans l'ordre
   `dimension temporelle → cle primaire → cles etrangeres`. Sur un fait agrege
   a grain temporel, la date de grain (`mois`, `event_date`) vient donc **en
   premier** (ex. `fct_supply_chain__disponibilite_article_neshu_depot_mensuel`
   ouvre sur `mois`, puis `company_id`, puis `product_code`).
2. **Cles etrangeres** restantes (`<entity>_id`) non incluses dans le grain.
3. **Attributs / dimensions** texte ou categoriels (`*_name`, `*_code`,
   `*_type`, `*_status`).
4. **Dates / timestamps metier** secondaires (hors date de grain).
5. **Booleens / flags** (`is_*`, `has_*`) — ils qualifient la ligne.
6. **Mesures** numeriques additives — toujours regroupees en fin de fait.
7. **Metadonnees / audit** (`created_at`, `updated_at`, `extracted_at`,
   `deleted_at`) — toujours en dernier.

> Regle, pas dogme : si un regroupement metier est plus lisible (ex. coller
> `entity_code`/`entity_name` juste apres leur FK), c'est tolere. Le grain en
> tete et les metadonnees en queue, en revanche, sont systematiques.

## 8. Checklist avant PR

- [ ] Fichier `<dim|fct>_<bu>__<entity>.sql` dans `models/marts/<bu>/` + entrée dans `_<bu>__marts_models.yml`
- [ ] Description YAML en 4 blocs (`[QUOI MÉTIER]`/`[COMMENT CONSTRUITE]`/`[GRAIN]`/`[NOTES]`), **grain obligatoire**
- [ ] Star schema : FK `<entity>_id` only, pas de jointure fait-à-fait, pas de snowflake, pas d'OBT
- [ ] Tests minimum (Dim : PK `unique`+`not_null` ; Fact : FK `relationships` + clé composite + invariants)
- [ ] `{{ config() }}` = matérialisation only (pas de `description`, pas de `tags`)
- [ ] Ordre des colonnes grain-first
- [ ] `exposure` mise à jour si un rapport Power BI consomme le mart
- [ ] `sqlfluff lint` OK
