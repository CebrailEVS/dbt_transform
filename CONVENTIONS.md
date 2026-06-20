# Conventions - EVS dbt Project

Regles de nommage, qualite et style appliquees dans le projet.

---

## Sommaire

| Section | Contenu |
|---------|---------|
| [Nommage des modeles](#nommage-des-modeles) | Format, prefixes par couche, convention marts by BU, fichiers YAML |
| [Nommage des colonnes](#nommage-des-colonnes) | Conventions generales, colonnes systeme staging |
| [Structure SQL](#structure-sql) | Pattern CTE staging, intermediate / marts |
| [Materialisation](#materialisation) | Table / incremental / snapshot par couche |
| [Marts — pattern complet](#marts--pattern-complet) | **Externalise → [`docs/conventions/marts.md`](docs/conventions/marts.md)** (modelisation, description, config, tests) |
| [Tests de qualite](#tests-de-qualite) | Tests natifs, dbt_utils, dbt_expectations, severite |
| [Source freshness](#source-freshness) | **Detail → [`docs/freshness.md`](docs/freshness.md)** (tiers, mecanismes, etat par source) |
| [SQLFluff](#sqlfluff) | Regles, commandes, exclusions, `-- noqa` |
| [Seeds et Snapshots](#seeds-et-snapshots) | CSV de reference, types BigQuery, SCD2 |
| [Tags](#tags) | Selection par source / couche |
| [Exposition Power BI](#exposition-power-bi) | Couche marts exposee, conventions de jointure |

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

> **Cette section est externalisee** dans [`docs/conventions/marts.md`](docs/conventions/marts.md).
> Elle y est maintenue comme **source unique de verite** pour tout ce qui concerne les
> marts : principes de modelisation (star schema strict), trame de description YAML en
> 4 blocs, hygiene du config block, tests minimum par layer, anti-patterns, squelette SQL
> type et ordre des colonnes grain-first.
>
> Le **nommage** des marts reste ici, voir [§ Nommage des marts](#nommage-des-marts--convention-by-bu).

---

## Tests de qualite

> **Syntaxe obligatoire (dbt ≥ 1.11)** : tout test générique prenant des paramètres
> (`accepted_values`, `relationships`, `unique_combination_of_columns`, `expression_is_true`,
> tous les `dbt_expectations.*`) doit imbriquer ses arguments sous `arguments:`, et la severity
> sous `config:`. La forme à plat (`combination_of_columns:` directement sous le test) est
> **dépréciée** et lève `MissingArgumentsPropertyInGenericTestDeprecation` — c'est un warning
> aujourd'hui, une erreur en 1.12. Le `dbt build` du CI ne bloque PAS sur ce warning, donc
> à vérifier manuellement en relecture de PR. Voir les exemples de [`docs/conventions/marts.md`](docs/conventions/marts.md) § 4.

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

> **Detail complet dans [`docs/freshness.md`](docs/freshness.md)** : tiers definis,
> mecanismes de monitoring (methode A `dbt source freshness` natif / methode B test
> `dbt_expectations` sur staging pour les sources a timestamp STRING), et etat cible
> par source. Ce document fait autorite — ne pas redupliquer la table des tiers ici.

Resume operationnel :

- **6 tiers** de Critique (26h/36h) a Manuel (60j/90j) — voir le tableau complet dans `docs/freshness.md`.
- **`freshness: null`** uniquement sur les referentiels vraiment immuables (codes, libelles, types). Toute source porteuse d'evenements doit avoir un seuil, quitte a le mettre tres large.
- **Ne jamais repeter** le freshness par table quand il est identique au defaut source — bruit YAML.
- **Toujours commenter le tier** en regard du bloc freshness (`# Freshness: tier Standard — 26h warn / 48h error`).

Commandes : `dbt source freshness` (methode A) · couvert par `dbt test` (methode B).

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
