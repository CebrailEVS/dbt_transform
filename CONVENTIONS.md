# Conventions - EVS dbt Project

Regles de nommage, qualite et style appliquees dans le projet.

---

## Nommage des modeles

### Format general

```
<couche>_<source>__<entite>.sql
```

Le double underscore `__` separe la source de l'entite.

### Par couche

| Couche | Prefixe | Exemple |
|--------|---------|---------|
| Staging | `stg_` | `stg_oracle_neshu__company.sql` |
| Intermediate | `int_` | `int_oracle_neshu__appro_tasks.sql` |
| Marts - Dimension | `dim_` | `dim_oracle_neshu__company.sql` |
| Marts - Fact (mono-source) | `fct_` | `fct_oracle_neshu__conso_business_review.sql` |
| Marts - Fact (cross-source) | `fct_<bu>__` | `fct_technique__neshu_maintenance_preventives.sql` |
| Snapshot | `snap_` | `snap_oracle_neshu__device.sql` |

> **Modeles cross-source :** un modele qui consomme des donnees de plusieurs sources differentes
> va dans un dossier BU (`marts/technique/`) et non dans un dossier source (`marts/oracle_neshu/`).
> Le prefixe du nom reflete la BU, pas la source.

### Nommage des marts — convention by BU

Les marts sont organises par **BU/domaine** (folder = BU), pas par source.
Le nom du modele reflete la BU et l'entite metier, pas l'implementation source.

| Element | Regle |
|---------|-------|
| Prefixe | `dim_` (dimension) ou `fct_` (fait) |
| Cle BU/domaine | nom exact du folder : `neshu`, `lcdp`, `technique`, `commerce`, `finance`, `services_generaux`, `supply_chain` |
| Separateur | `__` entre BU et entite |
| Entite | singulier, snake_case, nom metier (pas le nom source, pas le nom du rapport BI) |
| Suffixe de grain | uniquement si agrege au-dessus du grain naturel (`_quinzaine`, `_mensuel`) |
| Suffixe de source | uniquement en cas de collision dans le meme folder (ex. `fct_supply_chain__stock_neshu` vs `fct_supply_chain__stock_yuman`) |
| Nom du rapport BI | jamais dans le nom du mart — va dans l'`exposure` |

Exemples (avant → apres) :

| Avant | Apres | Pourquoi |
|-------|-------|----------|
| `fct_oracle_neshu__conso_business_review` | `fct_neshu__consommation` | source droppee, nom de rapport BI deplace vers exposure |
| `fct_mssql_sage__pnl_bu_kpis` | `fct_finance__pnl_bu` | `_kpis` implicite dans un fait |
| `dim_yuman__materials` | `dim_technique__material` | singulier, pas de source |
| `fct_oracle_neshu__chargement_par_quinzaine` | `fct_neshu__chargement_quinzaine` | suffixe de grain conserve |

### Fichiers YAML

Chaque source a deux fichiers YAML dans son dossier staging :

| Fichier | Contenu |
|---------|---------|
| `_<source>__sources.yml` | Declaration des sources, freshness, colonnes brutes |
| `_<source>__models.yml` | Documentation des modeles staging, tests |

---

## Nommage des colonnes

### Conventions generales

| Regle | Exemple |
|-------|---------|
| snake_case | `company_name`, `postal_code` |
| IDs : `id<entite>` (staging) | `idcompany`, `idtask` |
| IDs : `<entite>_id` (marts) | `company_id`, `task_id` |

> Staging conserve le nommage du systeme source (`idcompany`) pour faciliter le debug et le mapping. Les marts normalisent en `<entite>_id` pour la clarte cote Power BI et consommateurs finaux.
| Booleens : `is_` ou `has_` | `is_active`, `has_contract` |
| Dates : `_at` pour timestamps | `created_at`, `updated_at` |
| Dates : `_date` pour dates | `start_date`, `end_date` |

### Colonnes systeme (staging)

Chaque modele staging expose ces colonnes harmonisees :

| Colonne | Type | Description |
|---------|------|-------------|
| `created_at` | TIMESTAMP | Date de creation dans la source |
| `updated_at` | TIMESTAMP | Derniere modification (coalesce avec created_at) |
| `extracted_at` | TIMESTAMP | Date d'extraction Meltano (`_sdc_extracted_at`) |
| `deleted_at` | TIMESTAMP | Soft delete (`_sdc_deleted_at`) |

---

## Structure SQL

### Staging : pattern CTE standard

```sql
{{ config(materialized='table') }}

with source_data as (
    select * from {{ source('oracle_neshu', 'evs_company') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idcompany as int64) as idcompany,

        -- Colonnes texte
        code,
        name,

        -- Timestamps harmonises
        timestamp(creation_date) as created_at,
        timestamp(coalesce(modification_date, creation_date)) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at

    from source_data
)

select * from cleaned_data
```

### Intermediate / Marts : pattern CTE

- Utiliser des CTEs nommees pour chaque etape logique
- Preferer `ref()` vers staging (jamais `source()` en intermediate/marts)
- Commenter les sections avec `-- Identifiants`, `-- Metriques`, etc.

---

## Materialisation

| Couche | Materialisation | Raison |
|--------|----------------|--------|
| Staging | `table` | Exploration ad-hoc par les analystes |
| Intermediate | `table` | Idem, tables reutilisees par plusieurs marts |
| Marts | `table` | Exposition Power BI, performance requise |
| Snapshots | `timestamp` | SCD Type 2, suivi historique |

Certains modeles intermediate utilisent `incremental` pour les gros volumes (ex: `int_oracle_neshu__appro_tasks`).

---

## Marts — pattern complet

Section unifiee : modelisation, description, config, tests. Tout ce qui concerne
les marts est centralise ici. Pour le naming, voir §Nommage des marts.

### 1. Principes de modelisation

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
- **Dimensions** (`dim_*`) : 1 ligne par entite metier (ticket, compte,
  agent, machine, contrat...).
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

### 2. Trame de description (YAML)

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

### 3. Config block hygiene

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

### 4. Tests minimum par layer

Deux niveaux d'exigence : **obligatoire** (severity `error` par defaut)
et **recommande fort** (severity `warn` pour catcher les drifts silencieux).

#### Dim — obligatoire

```yaml
columns:
  - name: <pk>                    # company_id, device_id, etc.
    tests:
      - unique
      - not_null
```

#### Dim — recommande fort

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

#### Fact — obligatoire

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

#### Fact — recommande fort

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

#### Severity strategy

| Test | Severity | Raison |
|------|----------|--------|
| PK `unique` + `not_null` sur dim | `error` | Casse les jointures sinon |
| `not_null` sur FK obligatoire | `error` | Orphelin = bug data |
| `relationships` sur FK | `warn` | Detecte les drifts sans bloquer le build |
| `accepted_values` | `error` | Liste fermee — drift = nouveau code a connaitre |
| Plages dates / numeriques | `warn` | Informatif, ne pas bloquer la prod |
| `expect_table_row_count_to_be_between` | `warn` | Alerte de volume anormal |

### 5. Anti-patterns a refuser

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

### 6. Pattern type SQL (squelette)

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

### 7. Ordre des colonnes du `select` final

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

---

## Tests de qualite

> **Syntaxe obligatoire (dbt ≥ 1.11)** : tout test générique prenant des paramètres
> (`accepted_values`, `relationships`, `unique_combination_of_columns`, `expression_is_true`,
> tous les `dbt_expectations.*`) doit imbriquer ses arguments sous `arguments:`, et la severity
> sous `config:`. La forme à plat (`combination_of_columns:` directement sous le test) est
> **dépréciée** et lève `MissingArgumentsPropertyInGenericTestDeprecation` — c'est un warning
> aujourd'hui, une erreur en 1.12. Le `dbt build` du CI ne bloque PAS sur ce warning, donc
> à vérifier manuellement en relecture de PR. Voir les exemples de la section Marts ci-dessus.

### Tests dbt natifs

| Test | Usage |
|------|-------|
| `unique` | Cle primaire unique |
| `not_null` | Colonne obligatoire |
| `relationships` | Integrite referentielle entre modeles |
| `accepted_values` | Valeurs autorisees (a ajouter progressivement) |

### dbt_utils

| Test | Usage |
|------|-------|
| `unique_combination_of_columns` | Cle composite unique |
| `expression_is_true` | Assertions metier personnalisees |

### dbt_expectations

Package pour des tests de qualite avances. Configuration dans les fichiers `_<source>__models.yml`.

| Test | Usage | Exemple |
|------|-------|---------|
| `expect_table_row_count_to_be_between` | Volume attendu | min: 100, max: 100000 |
| `expect_column_values_to_be_between` | Plage de valeurs | Dates entre 2015 et maintenant |
| `expect_column_values_to_not_be_null` | Taux de null acceptable | `mostly: 0.95` |
| `expect_column_values_to_match_regex` | Format de donnees | Codes, emails, SIRET |

Exemple YAML :

```yaml
models:
  - name: stg_oracle_neshu__company
    tests:
      - dbt_expectations.expect_table_row_count_to_be_between:
          arguments:
            min_value: 100
            max_value: 100000
    columns:
      - name: real_start_date
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              arguments:
                min_value: "timestamp('2015-01-01')"
                row_condition: "real_start_date is not null"
              config:
                severity: warn
```

> **Note dbt 1.11** : les parametres des tests generiques doivent etre enveloppes dans `arguments:`.

### Strategie de severite

| Severite | Usage |
|----------|-------|
| `error` (defaut) | Tests critiques : unicite, not_null sur cles primaires |
| `warn` | Tests informatifs : plages de dates, volumes, formats |

---

## Source freshness

> Voir `docs/freshness.md` pour l'audit complet et la justification par source.

### Tiers

| Tier | warn_after | error_after | Sources |
|------|-----------|-------------|---------|
| Critique | 26h | 36h | oracle_neshu, oracle_lcdp |
| Standard | 26h | 48h | yuman, mssql_sage, nesp_co (activite/opportunite), oracle_neshu_gcs |
| Quotidien Mon-Sat | 36h | 80h | yuman_gcs |
| Hebdomadaire | 8j | 14j | nesp_tech |
| Relaxe | 7j | 14j | gac, zoho_desk |
| Manuel | 60j | 90j | nesp_co (nespresso_base_client) |

### Deux mecanismes selon le type de timestamp source

**Methode A — `dbt source freshness` natif** quand la source brute a un champ TIMESTAMP ou DATE :

```yaml
# _<source>__sources.yml
sources:
  - name: <source>
    config:
      loaded_at_field: _sdc_extracted_at
      freshness:
        warn_after: {count: 26, period: hour}
        error_after: {count: 48, period: hour}
    tables:
      - name: <referentiel_stable>
        config:
          freshness: null   # desactive — table immuable
```

Commande : `dbt source freshness`

**Methode B — test `dbt_expectations` sur staging** quand la source brute n'a qu'un champ STRING (external tables GCS, taps custom dlt). Le staging fait le cast en TIMESTAMP, on monitore a ce niveau :

```yaml
# _<source>__models.yml
models:
  - name: stg_<source>__<table>
    tests:
      - dbt_expectations.expect_row_values_to_have_recent_data:
          arguments:
            column_name: extracted_at
            datepart: day
            interval: 8
          config:
            severity: warn
```

Commande : couvert par `dbt test` standard.

### Regles

- **`freshness: null` uniquement sur les referentiels vraiment immuables** (codes, libellés, types). Toute source qui porte des evenements doit avoir un seuil, quitte a le mettre tres large.
- **Ne jamais repeter** le freshness par table quand il est identique au defaut source — c'est du bruit YAML.
- **Toujours commenter le tier** en regard du bloc freshness (`# Freshness: tier Standard — 26h warn / 48h error`).

---

## SQLFluff

Le projet utilise [SQLFluff v4](https://sqlfluff.com/) avec le templater dbt. Configuration dans `.sqlfluff`.

### Regles principales

| Regle | Parametre |
|-------|-----------|
| Mots-cles | `lowercase` (select, from, where, join...) |
| Fonctions | `lowercase` (cast, coalesce, timestamp...) |
| Types | `lowercase` (int64, string, float64...) |
| Alias tables | Explicite (`AS t`, pas juste `t`) |
| Alias colonnes | Explicite (`AS nom`, pas juste `nom`) |
| Virgule trailing | Interdit dans SELECT |
| Indentation | 4 espaces |
| Longueur de ligne | 120 caracteres max |

### Commandes

```bash
# Analyser
sqlfluff lint models/staging/oracle_neshu/
sqlfluff lint models/

# Corriger automatiquement
sqlfluff fix models/staging/oracle_neshu/

# Toujours verifier apres un fix
git diff
```

### Fichiers exclus

Configure dans `.sqlfluffignore` : `target/`, `dbt_packages/`, `logs/`, `.venv/`, `venv_dbt/`.

### `-- noqa`

Pour desactiver une regle sur une ligne specifique (faux positifs) :

```sql
select max(updated_at) from {{ ref('stg_oracle_neshu__task') }}  -- noqa: RF02
```

---

## Seeds et Snapshots

### Seeds (`data/reference_data/`)

Fichiers CSV charges via `dbt seed`. Utilises pour les donnees de reference statiques (mappings, parametres). Organises par domaine :

```
data/reference_data/
├── yuman/              # ref_yuman__cp_metropole.csv, ref_yuman__machine_clean.csv, ...
├── oracle_neshu/       # ref_oracle_neshu__valo_parc_machine.csv, ...
├── mssql_sage/         # ref_mssql_sage__code_analytique_bu.csv, ...
├── nesp_tech/          # ref_nesp_tech__articles_prix.csv, ...
├── nesp_co/            # ref_nesp_co__commerciaux.csv, ...
└── general/            # ref_general__feries_metropole.csv, ...
```

#### Declaration des types de colonnes

Les types sont declares **dans `data/schema.yml`** sous chaque seed, via le bloc `config: column_types:`. Ne pas utiliser `dbt_project.yml` pour les types par colonne.

```yaml
- name: ref_nesp_tech__key_facturation
  description: "Cle de facturation des interventions techniques"
  config:
    column_types:
      type_code: STRING
      prod_factu: INT64
      tarif_factu: FLOAT64
      valid_from: DATE
  columns:
    - name: type_code
      ...
```

#### Types BigQuery a utiliser

| Type | Utiliser | Ne pas utiliser |
|------|----------|-----------------|
| Texte | `STRING` | `string`, `str`, `VARCHAR` |
| Entier | `INT64` | `int`, `integer`, `INTEGER` |
| Decimal | `FLOAT64` | `float`, `FLOAT`, `NUMERIC` |
| Date | `DATE` | `date` |
| Timestamp | `TIMESTAMP` | `timestamp`, `DATETIME` |
| Booleen | `BOOLEAN` | `bool`, `BOOL` |

> **Regle critique** : toujours verifier les valeurs reelles du CSV avant d'assigner un type.
> Un code postal sans zero initial (`38000`) sera infere `INT64` par BigQuery — le typer
> `STRING` casserait les joins existants. Ne pas se fier au nom de la colonne seul.

#### Colonnes dans `data/schema.yml`

Chaque colonne du CSV doit avoir une entree dans `columns:` avec une description.
Les colonnes importantes doivent avoir des tests (`not_null`, `unique`, `relationships`).

#### BOM dans les fichiers CSV

Ne pas sauvegarder les CSV depuis Excel en UTF-8 avec BOM. Si un fichier contient un BOM
(visible via `head -c 3 fichier.csv | xxd`), le supprimer avec :

```bash
sed -i '1s/^\xef\xbb\xbf//' data/reference_data/<source>/<fichier>.csv
```

### Snapshots (`snapshots/`)

Suivi historique SCD Type 2 sur les entites qui evoluent :

| Snapshot | Entite | Strategie |
|----------|--------|-----------|
| `snap_oracle_neshu__company` | Clients | timestamp |
| `snap_oracle_neshu__device` | Machines | timestamp |
| `snap_oracle_neshu__valo_parc_machines` | Parc machines | timestamp |

Commande : `dbt snapshot`

---

## Tags

Chaque modele est tag par source pour permettre une execution selective :

```bash
dbt build --select tag:oracle_neshu    # Toute la chaine oracle_neshu
dbt build --select tag:yuman           # Toute la chaine yuman
dbt build --select tag:staging         # Tous les staging
dbt build --select tag:marts           # Tous les marts
```

Les tags sont definis dans `dbt_project.yml` et herites automatiquement par les modeles enfants.

---

## Exposition Power BI

Les modeles marts (`dim_*` et `fct_*`) sont la couche exposee a Power BI.

| Element | Convention |
|---------|-----------|
| Dataset BigQuery | `prod_marts` (production), `dev_marts` (developpement) |
| Dimensions | `dim_<source>__<entite>` - tables de reference (clients, produits, machines) |
| Facts | `fct_<source>__<metrique>` - tables de mesures (consommation, pricing, KPIs) |
| Jointures | Via les colonnes `<entite>_id` presentes dans les dims et facts |

Lors de la creation d'un nouveau mart, verifier que les noms de colonnes sont clairs pour un utilisateur Power BI (pas d'abreviations internes, pas d'IDs techniques exposes sans libelle).
