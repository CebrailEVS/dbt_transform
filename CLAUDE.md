# CLAUDE.md — dbt_warehouse (EVS Professionnelle France)

Entrepôt ELT d'EVS Professionnelle France. Équipe : 1 data engineer (propriétaire), 1 data
analyst (contribue aux marts).

**Chaîne** : dlt sur Cloud Run (repo `ingestion`) → BigQuery `prod_raw` → **dbt (ce repo)** →
Cloud Workflows (repo `infra`) → Power BI.

**dbt v2** (version : `requirements-lock.txt`), paquet `dbt` installé par pip. Moteur Rust livré
comme extension CPython : Python ≥ 3.11 requis à l'exécution. Toujours appeler
`./dbt_venv/bin/dbt` avec `.env` chargé (`set -a && . ./.env && set +a`) : un `dbt` nu peut
résoudre une autre installation (dbt-core 1.x), que `require-dbt-version` refuse. Licence propriétaire gratuite de dbt Labs, choisie
plutôt que `dbt-oss` (Apache 2.0) parce que ce dernier n'a pas `dbt lint`.

| Sujet | Référence |
|---|---|
| Vue d'ensemble, sources, commandes | [`README.md`](README.md) |
| Workflow, PR, ajouter un modèle | [`CONTRIBUTING.md`](CONTRIBUTING.md) |
| Conventions communes + une page par couche | [`CONVENTIONS.md`](CONVENTIONS.md), [`docs/conventions/`](docs/conventions/) |
| Dev / CI / prod, identités, defer | [`docs/environnements.md`](docs/environnements.md) |
| Fraîcheur des sources | [`docs/freshness.md`](docs/freshness.md) |
| Cadence de production | [`docs/pipeline-schedule.md`](docs/pipeline-schedule.md) |

---

## Règles strictes

- **Jamais `--target prod`**, sauf demande explicite.
- **Jamais supprimer ni dropper une table**, sauf demande explicite.
- **Jamais de `git push` ni de PR sans GO explicite, à chaque fois.** Un GO donné plus tôt ne
  vaut pas pour le push suivant. Les commits locaux sur une branche de feature sont libres.
- **Branche depuis `master` à jour**, jamais depuis la branche courante :
  `git fetch origin master && git checkout -b feature/<nom> origin/master`.
  Avant tout push, `git log --oneline origin/master..HEAD` et `git diff --stat origin/master...HEAD`
  ne doivent montrer que le chantier en cours. Branche polluée : nouvelle branche depuis
  `origin/master` + `git cherry-pick` des seuls commits du chantier.
- **Tout changement de code passe par une PR** : un push sur `master` construit en prod.
  Exception : la documentation pure (README, CONTRIBUTING, CONVENTIONS, `docs/`, ce fichier) part
  en push direct sur `master`, avec les droits owner (à défaut : PR + `gh pr merge --admin`).
- **`dbt lint`** sur chaque modèle modifié avant de le considérer terminé.
- **Staging** : écrit par le data engineer lui-même. Ne pas générer de modèle de staging.
- **Snapshots** : stratégie et colonnes intouchables (Cloud Workflows les exécute). Seule
  exception : mettre à jour un `ref()` interne quand une dim référencée est renommée. Ne jamais
  renommer un fichier snapshot ni sa table : l'historique SCD2 serait perdu.
- **Rangement** : brouillons, exports, rapports ponctuels → `tmp/` (ignoré). `scripts/` = scripts
  d'équipe versionnés. Jamais de clé ni de copie de clé dans le repo.
- **Commentaires** : un commentaire dit la règle et son pourquoi, en 1 à 3 lignes. Pas de date, de
  numéro de PR, de récit d'incident ni de compte qui dérive : l'historique va dans le message de
  commit.

---

## Environnements

Détail : [`docs/environnements.md`](docs/environnements.md). Variables locales : `.env.example`.

- **Un seul projet GCP** `evs-datastack-prod`, isolation par dataset ; la frontière est l'IAM.
  - `dev` → `dbt_<toi>` : toutes les couches dans un seul dataset, tables expirées après 14 jours
    sans rebuild ;
  - `ci` → `dbt_ci_pr_<N>` : créé par `pr-check`, supprimé à la fermeture de la PR ;
  - `prod` → `prod_<couche>`.
  `dbt-dev` et `dbt-ci` lisent `prod_*` et n'y écrivent jamais ; `generate_schema_name` refuse
  aussi un dataset `prod*` hors prod.
- **`--defer` par défaut** vers le manifest prod (`state/`), tenu à jour par les hooks git
  `post-merge` / `post-checkout`. À la main : `scripts/pull-state.sh --force`.
- **Recette dev ↔ prod** : `--favor-state`, et vérifier que dev et prod ont le même volume avant
  d'interpréter un écart (une table dev périmée fausse la comparaison).
- **Incrémental** : `dbt clone -s <modele>` puis `dbt run -s <modele>` exécute le vrai `MERGE`.
- **Snapshots** : jamais construits hors prod (`target_schema: snapshots`, lecture seule ailleurs).

## CI/CD — `.github/workflows/dbt-ci.yml`

| Événement | Job | Ce qui se passe |
|---|---|---|
| PR vers `master` | `pr-check` | `dbt lint` des modèles modifiés, `dbt parse`, `dbt compile --static-analysis strict` (bloquant, projet entier), build `state:modified+` avec defer dans `dbt_ci_pr_<N>` |
| PR fermée | `cleanup-ci-dataset` | suppression de `dbt_ci_pr_<N>` |
| Push sur `master`, **y compris direct** | `cd` | build `state:modified+` **en prod**, docs, dépôt du manifest dans `gs://evs-datastack-dbt-state`, image `dbt-runner:latest` |
| après `cd` | `deploy-docs` | publication de la doc dbt |

- Snapshots **toujours exclus** de la CI/CD : seul Cloud Workflows les exécute.
- `state:modified+` ne reconstruit que le modifié et son aval. Après un changement de forme du raw,
  un build vert ne prouve rien sur la chaîne complète : la reconstruire entièrement.
- L'image `dbt-runner:latest` est résolue à chaque exécution Cloud Run : le merge suffit à la déployer.
- Toute identité qui exécute dbt v2 a besoin de `roles/bigquery.readSessionUser` (lecture par la
  Storage Read API), sinon `DbDriverFailed (dbt1308)`.

---

## Architecture

| Couche | Dossier | Dataset prod | Matérialisation |
|---|---|---|---|
| Staging | `models/staging/<source>/` | `prod_staging` | `table` ; `incremental` pour les grosses tables de tâches Oracle |
| Intermediate | `models/intermediate/<source>/` | `prod_intermediate` | `table` ; `incremental` sur gros volume ; `ephemeral` ponctuel |
| Marts | `models/marts/<bu>/` | `prod_marts` | `table` ; `view` ponctuelle |
| Apps | `models/apps/<application>/` | `prod_app_<application>` | explicite par modèle ([`apps.md`](docs/conventions/apps.md)) |
| Seeds | `data/reference_data/<source>/` | `prod_reference` | — |

Sources : tableau du [README](README.md#sources-de-données). BUs des marts : `neshu`, `lcdp`,
`technique`, `commerce`, `finance`, `services_generaux`, `supply_chain`, `bi` (gouvernance du
parc Power BI, pas un domaine métier).

## Nommage

Règles complètes : [`CONVENTIONS.md`](CONVENTIONS.md) et [`marts.md` § 1](docs/conventions/marts.md#1-nommage).

- `<prefixe>_<source ou BU>__<entite>` : `stg_` / `int_` par source, `dim_` / `fct_` par BU,
  `app_` par application, `snap_` et `ref_` (seeds) par source. Entité au singulier, nom métier.
- Colonnes : snake_case ; IDs = nom source en staging, `<entite>_id` en marts ; booléens `is_` /
  `has_` ; timestamps `_at` ; dates `_date`.
- Staging : `extracted_at` obligatoire (depuis `_extracted_at` de dlt), `created_at` /
  `updated_at` quand la source les porte. Pas de `deleted_at`.
- YAML : `_<source>__sources.yml`, `_<source>__models.yml` (staging),
  `_<source>__intermediate_models.yml`, `_<bu>__marts_models.yml`, `_<application>__app_models.yml`,
  `_<source>__seeds.yml` (à côté des CSV), `_<source>__snapshots.yml`.

## Créer ou modifier un modèle

Ordre : staging → intermediate → marts, sans sauter de couche. **Lire la page de la couche avant
d'écrire** : [`staging.md`](docs/conventions/staging.md),
[`intermediate.md`](docs/conventions/intermediate.md), [`marts.md`](docs/conventions/marts.md),
[`apps.md`](docs/conventions/apps.md), [`seeds-snapshots.md`](docs/conventions/seeds-snapshots.md).
SQL et entrée YAML dans la même PR.

- **Staging** : une table source = un modèle, renommage passthrough, `description='…'` dans le
  `config()` (en plus du YAML).
- **Intermediate** : logique métier alignée sur **une** source, uniquement des `ref()`. Le
  croisement de sources se fait dans les marts. Description en YAML seulement.
- **Marts** — [`marts.md`](docs/conventions/marts.md) :
  1. description YAML en 4 blocs `[QUOI MÉTIER]` / `[COMMENT CONSTRUITE]` / `[GRAIN]` / `[NOTES]`,
     grain obligatoire ;
  2. tests minimum (§ 7) : dim → `unique` + `not_null` sur la PK ; fait → `relationships` sur chaque
     FK, clé composite unique, invariants. Sévérités : [`CONVENTIONS.md` § Tests](CONVENTIONS.md#tests) ;
  3. `config()` = matérialisation seulement ; ni description ni `tags` ;
  4. schéma en étoile strict (§ 3) : pas de jointure fait-à-fait (agréger à un grain plus grossier
     ou étendre en 1:1 est permis), pas de snowflake, pas d'OBT ; du parent direct, 1 à 3 attributs
     d'affichage au maximum ;
  5. colonnes dans l'ordre grain-first (§ 5).
- **Avec le MCP BigQuery**, explorer l'amont avant d'écrire un mart : `get_table_info`,
  `SELECT DISTINCT` pour les `accepted_values`, `COUNT(*)` pour les bornes de volume, `MIN/MAX`
  pour les plages de dates.
- **Exposures** : un fichier par BU dans `models/exposures/`, plus `cockpit_supply.yml` (exposure
  `application`). Mettre à jour l'exposure dès qu'un mart consommé par un rapport est créé ou modifié.

## BigQuery

- **Partition** sur la date filtrée par Power BI (faits) ou par l'incrémental (staging) ;
  `data_type: 'date'` ou `'timestamp'`. Pas de partition sur les petites dimensions.
- **Cluster** sur les FK les plus jointes ou filtrées, 4 au maximum.
- **Incrémental** :
  ```sql
  {{ config(materialized='incremental', unique_key='id', incremental_strategy='merge') }}
  ...
  {% if is_incremental() %}
      where updated_at > (select max(updated_at) from {{ this }})
         or updated_at >= timestamp_sub(current_timestamp(), interval 7 day)
  {% endif %}
  ```

## Frontières avec `ingestion/` et `infra/`

- **Le raw est fidèle à la source** : `ingestion/` ne fait aucun typage métier (`NUMBER` Oracle
  sans précision → `FLOAT64`, clés → `NUMERIC`, types inconnus → `STRING`). **Tous les casts se
  font ici** ; ne pas demander de changement de type côté extraction.
- **Un changement de type dans `prod_raw` casse un modèle incrémental** sur toute colonne passée
  sans cast : le `MERGE` échoue (`Value of type X cannot be assigned to <col>`). `--full-refresh`
  ne le révèle pas, puisqu'il reconstruit la table. Après une évolution de type : build **sans**
  `--full-refresh`, et caster explicitement toute colonne passée telle quelle.
- `--static-analysis strict` ne compare que les colonnes dont le `data_type` est déclaré en YAML,
  une minorité : il ne remplace pas le cast.
- **Un sélecteur de `selectors.yml` doit avoir un appelant** dans les workflows `infra/`. Retirer
  un workflow peut rendre un sélecteur mort : le supprimer avec.

## Lint — `dbt lint`, config `.sqlfluff`

Mots-clés, fonctions et types en minuscules ; indentation 4 espaces ; 120 caractères par ligne ;
alias explicites ; pas de virgule finale dans le `select`. `dbt lint` est natif, ne se connecte pas
à BigQuery, respecte les `-- noqa`, mais doit résoudre les `env_var()` de `profiles.yml` (`.env`
chargé). Sa parité avec SQLFluff n'est pas totale : l'indentation (LT02) diverge.

Piège : `capitalisation.functions` et `capitalisation.types` prennent
`extended_capitalisation_policy`. Avec `capitalisation_policy`, la règle est ignorée sans message.

## Délégation aux subagents

Ce qui lit beaucoup pour conclure peu part en subagent ; la session principale garde la décision.

| Situation | Délégation |
|---|---|
| Comprendre comment marche X, recherche large | agent `Explore` |
| Un mart semble fini | agent `mart-reviewer`, **avant** de le déclarer terminé |
| Changement d'un type de colonne, d'un nom de source, d'un sélecteur, d'un tag | agent `boundary-impact` |
| Audit lourd sur BigQuery (`audit-docs`, `audit-sources`, `check-staging-relationships`) | la skill dans un subagent |
| Revue de diff avant PR | `/code-review` |

Ne pas déléguer : l'écriture des stagings, les décisions d'architecture, les arbitrages métier.
Les agents-gardes (`mart-reviewer`, `boundary-impact`, `tf-plan-reviewer`, `pipeline-reviewer`)
sont seuls relecteurs avant la prod : `opus` + `effort: high`. `sonnet` sert au travail de
volume (audit d'une BU, scaffolding, migration en fan-out). Deux corrections ratées sur le même
point : `/clear` et reprompt plus précis.

## Tenir la doc à jour

Dans la même session que le changement :

| Changement | Mettre à jour |
|---|---|
| Nouvelle source | tableau du README, [`docs/freshness.md`](docs/freshness.md), tags dans `dbt_project.yml` |
| Nouveau rapport Power BI ou nouvelle lecture par une app | `models/exposures/<bu>.yml` |
| Nouvelle BU | dossier `models/marts/<bu>/`, `_<bu>__marts_models.yml`, fichier d'exposures, tags |
| Règle d'une couche (nommage, pattern, tests) | la page de la couche dans `docs/conventions/` |
| Règle commune (colonnes, lint, sévérités, matérialisation) | `CONVENTIONS.md` |
| Workflow, PR, checklist | `CONTRIBUTING.md` |
| Environnement, CI/CD, IAM | [`docs/environnements.md`](docs/environnements.md) (README et CONTRIBUTING ne font que résumer) |
