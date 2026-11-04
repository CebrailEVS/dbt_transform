# CLAUDE.md — dbt_transform (EVS Professionnelle France)

Sources → dlt (repo `ingestion`) → BigQuery `prod_raw` → **dbt (ce repo)** → Power BI ;
orchestration par Cloud Workflows (repo `infra`). Un data engineer, propriétaire du repo ; un
data analyst contribue aux marts.

dbt v2 (version : `requirements-lock.txt`, Python ≥ 3.11). Toujours `./dbt_venv/bin/dbt` avec
`.env` chargé (`set -a && . ./.env && set +a`) : un `dbt` nu peut résoudre une installation
dbt-core 1.x, que `require-dbt-version` refuse.

| Sujet | Référence |
|---|---|
| Vue d'ensemble, sources, commandes | [`README.md`](README.md) |
| Workflow, PR, ajouter un modèle | [`CONTRIBUTING.md`](CONTRIBUTING.md) |
| Conventions communes, puis une page par couche | [`CONVENTIONS.md`](CONVENTIONS.md), [`docs/conventions/`](docs/conventions/) |
| Dev / CI / prod, identités, defer, retour arrière | [`docs/environnements.md`](docs/environnements.md) |
| Fraîcheur, cadence de production | [`docs/freshness.md`](docs/freshness.md), [`docs/pipeline-schedule.md`](docs/pipeline-schedule.md) |
| Pièges d'une source | [`docs/architecture/`](docs/architecture/) |
| Pourquoi c'est fait ainsi | [`docs/decisions.md`](docs/decisions.md) |

---

## Règles strictes

- **Confirmation de l'utilisateur, à chaque fois** : une commande `--target prod`, un push direct
  sur `master`. Un accord passé ne vaut pas pour la fois suivante. Les règles `ask` de
  `.claude/settings.json` forcent la demande, auto mode compris.
- **Libres** : commit, push d'une branche de feature, ouverture et merge de PR. Le merge déclenche
  le `cd`, donc la prod : merger seulement avec `pr-check` vert (ou sans `pr-check`, PR sans code),
  puis suivre le `cd` jusqu'au vert. Exception : une PR qui crée ou modifie un staging attend la
  validation de l'utilisateur avant le merge.
- **Jamais supprimer ni dropper une table**, sauf demande explicite.
- **Branche depuis `master` à jour**, jamais depuis la branche courante :
  `git fetch origin master && git checkout -b feature/<nom> origin/master`. Avant tout push,
  `git log --oneline origin/master..HEAD` et `git diff --stat origin/master...HEAD` ne montrent
  que le chantier en cours ; sinon, nouvelle branche et `git cherry-pick` des seuls commits du
  chantier.
- **Tout changement de code passe par une PR** : un push sur `master` qui touche le code
  construit en prod. Seule la documentation pure (README, CONTRIBUTING, CONVENTIONS, `docs/`, ce
  fichier) peut partir en push direct, avec les droits owner (à défaut : PR + `gh pr merge --admin`).
- **`dbt lint`** sur chaque modèle modifié avant de le considérer terminé.
- **Staging** : Claude peut l'écrire, dans le cadre de
  [`staging.md` § 10](docs/conventions/staging.md#10-écrire-un-staging-avec-claude) : l'utilisateur
  valide la proposition avant l'écriture, et la PR avant le merge.
- **Snapshots** : stratégie et colonnes intouchables (Cloud Workflows les exécute). Ne jamais
  renommer un fichier snapshot ni sa table : l'historique SCD2 serait perdu. Seule exception :
  mettre à jour un `ref()` vers une dim renommée.
- **Rangement** : brouillons, exports, rapports ponctuels → `tmp/` (ignoré). `scripts/` = scripts
  d'équipe versionnés. Jamais de clé ni de copie de clé dans le repo.
- **Écrire** : un commentaire, une description ou une doc dit la règle et sa raison, en 1 à 3
  lignes. Pas de date, de numéro de PR, de récit ni de chiffre qui dérive : l'historique va dans
  le message de commit ([`CONVENTIONS.md`](CONVENTIONS.md#écrire--commentaires-descriptions-documentation)).

---

## Environnements et CI/CD

Détail et raisons : [`docs/environnements.md`](docs/environnements.md).

- **Un seul projet GCP** `evs-datastack-prod`, un dataset par usage ; la frontière est l'IAM.
  `dev` → `dbt_<toi>`, `ci` → `dbt_ci_pr_<N>`, `prod` → `prod_<couche>`. Dev et CI lisent `prod_*`
  et n'y écrivent jamais.
- **Dataset dev, un chantier à la fois** : une branche = une session = `dbt_<toi>`, toutes couches
  confondues. Seule la session du chantier en cours y construit. Avant son premier build en dev,
  `dbt run-operation vider_dataset_dev` liste les objets et leur date de création : un objet de
  moins de 2 h qui ne vient pas de la branche → demander à l'utilisateur avant de vider. Une
  urgence pendant un chantier : branche sans build en dev, validée par `pr-check`
  ([`docs/environnements.md` § 1](docs/environnements.md#un-chantier-à-la-fois-dataset-vidé-en-début-de-branche)).
- **`--defer` par défaut** vers le manifest prod (`state/`, tenu à jour par les hooks git ; à la
  main : `scripts/pull-state.sh --force`).
- **Recette dev ↔ prod** : `--favor-state`, et le même volume en dev et en prod avant
  d'interpréter un écart.
- **Incrémental** : `dbt clone -s <modele>` puis `dbt run -s <modele>` exécute le vrai `MERGE`.
- **Snapshots** : construits en prod seulement, par Cloud Workflows ; exclus de la CI/CD.
- **CI/CD** (`.github/workflows/dbt-ci.yml`) : une PR lance `pr-check` (lint, `dbt compile
  --static-analysis strict`, build `state:modified+` dans `dbt_ci_pr_<N>`). Un push sur `master`
  qui touche le code lance `cd` : build `state:modified+` **en prod**, manifest, image
  `dbt-runner` reprise par la production planifiée à sa prochaine exécution. Un changement de
  documentation seule ne déclenche rien.
- **`state:modified+` ne reconstruit que le modifié et son aval.** Après un changement de forme
  du raw, un build vert ne prouve rien sur la chaîne complète : la reconstruire entièrement.

---

## Architecture

| Couche | Dossier | Dataset prod | Matérialisation |
|---|---|---|---|
| Staging | `models/staging/<source>/` | `prod_staging` | `table` ; `incremental` pour les grosses tables de tâches Oracle |
| Intermediate | `models/intermediate/<source>/` | `prod_intermediate` | `table` ; `incremental` sur gros volume ; `ephemeral` ponctuel |
| Marts | `models/marts/<bu>/` | `prod_marts` | `table` ; `view` ponctuelle |
| Apps | `models/apps/<application>/` | `prod_app_<application>` | explicite par modèle |
| Seeds | `data/reference_data/<source>/` | `prod_reference` | — |

Une BU = un dossier de `models/marts/` ; `bi` (gouvernance du parc Power BI) et `plateforme`
(santé de la plateforme data) n'y sont pas des domaines métier. Sources :
[README](README.md#sources-de-données).

## Créer ou modifier un modèle

Ordre : staging → intermediate → marts, sans sauter de couche. **Lire la page de la couche avant
d'écrire** ([`docs/conventions/`](docs/conventions/)). SQL et entrée YAML dans la même PR.

- **Nommage** : `<prefixe>_<source ou BU>__<entite>`, entité au singulier. `stg_` / `int_` par
  source, `dim_` / `fct_` par BU, `app_` par application, `snap_` / `ref_` par source. IDs = nom
  source en staging, `<entite>_id` en marts ; booléens `is_` / `has_` ; `_at` pour un timestamp,
  `_date` pour une date.
- **Staging** : une table source = un modèle, renommage passthrough, sans dédoublonnage
  préventif. `extracted_at` obligatoire, pas de `deleted_at`.
- **Intermediate** : logique métier d'**une** source, uniquement des `ref()`. Le croisement de
  sources se fait dans les marts.
- **Description** : dans le YAML seulement, à toutes les couches, jamais dans le `config()`
  (règle `dbt check` `description_dans_config`).
- **Marts** ([`marts.md`](docs/conventions/marts.md)) : description en 4 blocs, grain
  obligatoire ; `config()` = matérialisation seulement ; étoile stricte (ni jointure fait-à-fait,
  ni snowflake, ni OBT) ; tests minimum (§ 7) ; colonnes grain-first.
- **Apps** ([`apps.md`](docs/conventions/apps.md)) : partent du staging ou de l'intermediate,
  jamais d'un mart.
- **`dbt build` exécute les règles de `checks/`**
  ([`CONVENTIONS.md`](CONVENTIONS.md#règles-vérifiées-par-dbt-check)) : une règle en `error`
  arrête le build et nomme le modèle en cause. Corriger le modèle ; n'ajouter une exception à la
  règle qu'avec l'accord de l'utilisateur.
- **Avant d'écrire un mart**, explorer l'amont dans BigQuery : `SELECT DISTINCT` pour les
  `accepted_values`, `COUNT(*)` et `MIN/MAX` pour les bornes.
- **Exposures** (`models/exposures/<bu>.yml`) : à mettre à jour dès qu'un mart lu par un rapport
  ou une application est créé ou modifié.

## Pièges BigQuery et frontières

- **Fuseau** : stocker en UTC, afficher en heure de Paris. `date(x)` sans fuseau sur un instant
  UTC donne la veille pour un événement entre 00 h et 02 h à Paris.
- **Partition** sur la date filtrée (par Power BI pour un fait, par l'incrémental en staging) ;
  **cluster** sur les FK les plus jointes, 4 au maximum. Pas de partition sur une petite dimension.
- **Le raw est fidèle à la source** : `ingestion/` ne type rien (`NUMBER` Oracle → `FLOAT64`,
  clés → `NUMERIC`, types inconnus → `STRING`). Tous les casts se font ici.
- **Un changement de type dans `prod_raw` casse un incrémental** sur toute colonne passée sans
  cast (`MERGE` : `Value of type X cannot be assigned to <col>`). `--full-refresh` ne le révèle
  pas. Après une évolution de type : build **sans** `--full-refresh`, et caster explicitement.
  `--static-analysis strict` ne compare que les colonnes dont le `data_type` est déclaré : il ne
  remplace pas le cast.

## Lint — `dbt lint`, config `.sqlfluff`

Le `.sqlfluff` fait foi. `dbt lint` ne se connecte pas à BigQuery et respecte les `-- noqa`, mais
doit résoudre les `env_var()` de `profiles.yml` (`.env` chargé). Sa parité avec SQLFluff n'est pas
totale : l'indentation (LT02) diverge. Piège : `capitalisation.functions` et
`capitalisation.types` prennent `extended_capitalisation_policy` ; avec `capitalisation_policy`,
la règle est ignorée sans message.

## Délégation aux subagents

Ce qui lit beaucoup pour conclure peu part en subagent ; la session principale garde la décision.

| Situation | Délégation |
|---|---|
| Comprendre comment marche X, recherche large | agent `Explore` |
| Un mart semble fini | agent `mart-reviewer`, **avant** de le déclarer terminé : il confronte le mart à la donnée de prod (grain, réconciliation, orphelins, montants, fuseau) |
| Changement d'un type de colonne, d'un nom de source, d'un sélecteur, d'un tag | agent `boundary-impact` |
| Revue de diff avant PR | `/code-review` |

Ne pas déléguer à un subagent : l'écriture d'un staging (la session principale la mène avec
l'utilisateur), les décisions d'architecture, les arbitrages métier.
Un constat d'agent se vérifie avant d'être appliqué. `sonnet` suffit au travail de volume
(audit d'une BU, migration en fan-out). Deux corrections ratées sur le même point : `/clear` et
reprompt plus précis.

## Tenir la doc à jour

Dans la même PR que le changement :

| Changement | Mettre à jour |
|---|---|
| Nouvelle source | tableau du README, tags dans `dbt_project.yml`, seuils dans `_<source>__sources.yml`, page `docs/architecture/` si la source a des pièges |
| Nouveau rapport Power BI, nouvelle lecture par une app | `models/exposures/<bu>.yml` |
| Nouvelle BU | `models/marts/<bu>/` et son YAML, fichier d'exposures, tags |
| Règle d'une couche | sa page dans `docs/conventions/`, et un fichier `checks/` si une requête peut la vérifier |
| Règle commune (colonnes, lint, sévérités, fuseau) | `CONVENTIONS.md` |
| Workflow, PR, checklist | `CONTRIBUTING.md` |
| Environnement, CI/CD, IAM | `docs/environnements.md` |
| Choix structurant | `docs/decisions.md` |
