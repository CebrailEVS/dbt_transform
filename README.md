# EVS Data Warehouse — projet dbt

Couche **Transform** de la plateforme data d'**EVS Professionnelle France** : modélisation
BigQuery avec dbt, déclenchée par Google Cloud Workflows après chaque extraction.

**[Documentation dbt générée](https://cebrailevs.github.io/dbt_transform/)** ·
[CONTRIBUTING](CONTRIBUTING.md) · [CONVENTIONS](CONVENTIONS.md) ·
[Environnements & CI/CD](docs/environnements.md)

---

## Démarrage

Installation du poste, clé et dataset de dev : [CONTRIBUTING § 1](CONTRIBUTING.md#1-installer-son-poste).
Python 3.11+ requis : dbt v2 est un moteur Rust distribué comme extension CPython.

---

## Stack

| Composant | Rôle |
|---|---|
| **dlt** *(repo `ingestion`)* | Extraction et chargement vers `prod_raw` |
| **BigQuery** | Lac (`prod_raw`) et entrepôt (`prod_staging` → `prod_intermediate` → `prod_marts`) |
| **dbt v2** *(ce repo)* | Transformation. Moteur Rust, adaptateur BigQuery et linter inclus |
| **Cloud Workflows + Scheduler + Cloud Run** | Orchestration de la production (repo `infra`) |
| **GitHub Actions** | CI/CD, authentification sans clé (WIF) |
| **Power BI** | Restitution |

Tout tourne dans **un seul projet GCP**, `evs-datastack-prod`. Dev, CI et prod sont séparés
par dataset et par IAM. Voir [docs/environnements.md](docs/environnements.md).

---

## Sources de données

| Source | Système | Description |
|---|---|---|
| **oracle_neshu** | Oracle ERP (NESHU) | ERP principal : clients, machines, produits, tâches |
| **oracle_lcdp** | Oracle ERP (LCDP) | ERP secondaire, même schéma qu'oracle_neshu |
| **yuman_api** | Yuman API | Interventions terrain : clients, sites, matériels, bons de travail |
| **mssql_sage** | MSSQL Sage | Comptabilité : écritures, comptes tiers, collaborateurs |
| **nesp_tech** | Nomad Repair API | Interventions techniques Nespresso, pièces détachées |
| **nesp_co** | Excel / Nespresso | Données commerciales Nespresso |
| **zoho_desk** | Zoho Desk API | Tickets support, SLA, threads |
| **apptech** | App interne (tables externes GCS) | Suivi technicien : events, pauses, curatif, astreinte |
| **gac** | SFTP CSV | Assurance flotte, sinistres véhicules |
| **powerbi_activity** | API admin Power BI | Journaux d'usage du locataire + inventaire espaces/rapports/modèles |
| **yuman_evs_sftp** | Fichier SFTP | Stock théorique Yuman |
| **oracle_neshu_gcs** | Oracle ERP (NESHU) | Stock théorique Oracle NESHU |
| **oracle_lcdp_gcs** | Oracle ERP (LCDP) | Stock théorique Oracle LCDP |
| **historic** | Archive | Analytique Sage 2024, figée |

> **Tables externes GCS** : `nesp_tech` et `apptech` sont lues via des tables externes BigQuery
> adossées à GCS (API spécifique, fichiers écrits par l'app). Toutes les autres sources sont des
> tables natives chargées par dlt.
>
> Le suffixe `_gcs` des deux sources de stock théorique est historique : elles sont chargées
> directement depuis Oracle par dlt. Le nom est gardé pour ne pas casser les `source()`.

Fraîcheur des sources : **[`docs/freshness.md`](docs/freshness.md)** (référence unique).

---

## Modèles

```
models/
├── staging/        1 modèle = 1 table source : typage, nettoyage   (par source)
├── intermediate/   logique métier, enrichissement                  (par source)
├── marts/          dimensions et faits pour Power BI              (par BU)
├── apps/           modèles de service d'une application interne   (par application)
└── exposures/      rapports et applications qui lisent les modèles
```

**8 BU** : `neshu`, `lcdp`, `technique`, `commerce`, `finance`, `services_generaux`,
`supply_chain`, `bi`. Le data engineer possède `staging/`, `intermediate/` et les snapshots ;
le data analyst contribue aux `marts/`.

---

## Commandes courantes

```bash
dbt build -s mon_modele                 # ton modèle seul (parents lus en prod : defer)
dbt build -s mon_modele+                # + tout l'aval
dbt build -s tag:oracle_neshu           # toute une source
dbt build -s +exposure:business_review  # tout ce qui alimente un rapport
dbt clone -s mon_incremental            # copier une table prod pour tester un MERGE
dbt lint models/chemin/ [--fix]         # lint SQL (règles : CONVENTIONS.md)
dbt freshness                           # fraîcheur (autorité : docs/freshness.md)
```

`dbt build` exécute les modèles et leurs tests dans l'ordre du DAG : un test en échec bloque
l'aval. Les snapshots ne se lancent jamais à la main, ils appartiennent à Cloud Workflows.

---

## CI/CD

| Événement | Ce qui se passe |
|---|---|
| PR vers `master` | lint, analyse statique, build `state:modified+` dans `dbt_ci_pr_<N>` avec defer |
| PR fermée | suppression de `dbt_ci_pr_<N>` |
| Push sur `master` | build `state:modified+` **en prod**, publication des docs, nouvelle image `dbt-runner` |

Seul ce qui est modifié est reconstruit, à chaque étape. Un push direct sur `master` part
en prod immédiatement. Détail : [docs/environnements.md](docs/environnements.md).

---

## Dépendances

Versions : `requirements-lock.txt` (dbt) et `packages.yml` (paquets dbt).

| Paquet | Rôle |
|---|---|
| `dbt` (pip) | Moteur dbt v2. Gratuit, licence propriétaire dbt Labs (`dbt-oss`, en Apache 2.0, n'a pas `dbt lint`) |
| `dbt_utils` | `unique_combination_of_columns`, `expression_is_true`, `generate_surrogate_key` |
| `dbt_expectations` | Volumes, plages de valeurs, récence |
| `dbt_orphan` | Repérage des tables orphelines, cf. [docs/maintenance.md](docs/maintenance.md) |

---

## Couche IA (Claude Code)

Versionnée et partagée. Seuls `.claude/settings.local.json` et `.claude/notes/` restent locaux.

```
CLAUDE.md         contexte projet et règles strictes
.mcp.json         serveurs MCP : BigQuery, Power BI, dbt
.claude/
├── commands/     /build-source, /new-mart, /lint-fix, /freshness
├── skills/       audit-sources, audit-docs, check-staging-relationships, profile
├── agents/       mart-reviewer
└── hooks/        garde-fou de branche avant commit/push, dbt lint et dbt parse après édition, helpers d'auth MCP
```

Les hooks git de `.pre-commit-config.yaml` rejouent les mêmes contrôles pour les humains (cf. CONTRIBUTING § 1).

---

## Dépannage

```bash
dbt debug                          # configuration et connexion
set -a && source .env && set +a    # variables non chargées (sans direnv)
scripts/pull-state.sh --force      # defer qui pointe vers un modèle absent : retélécharger le manifest
rm -rf target/                     # artefacts d'une ancienne version de dbt
```

---

## Documentation

| Sujet | Où |
|---|---|
| Environnements, identités, CI/CD, defer | [docs/environnements.md](docs/environnements.md) |
| Contribuer (workflow, PR, ajouter un modèle) | [CONTRIBUTING.md](CONTRIBUTING.md) |
| Conventions (index) et par couche | [CONVENTIONS.md](CONVENTIONS.md), [docs/conventions/](docs/conventions/) |
| Fraîcheur des sources | [docs/freshness.md](docs/freshness.md) |
| Circulation de la donnée en production | [docs/pipeline-schedule.md](docs/pipeline-schedule.md) |
| Architecture par source | [docs/architecture/](docs/architecture/) |
| Nettoyage des objets orphelins | [docs/maintenance.md](docs/maintenance.md) |
