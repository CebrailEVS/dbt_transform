# EVS Data Warehouse — projet dbt

Projet dbt de transformation de donnees pour **EVS Professionnelle France**.
Ce repo couvre la couche **Transform** du pipeline ELT : modelisation BigQuery via dbt,
declenchee automatiquement par Google Cloud Workflows apres chaque extraction.

**[Documentation dbt generee](https://cebrailevs.github.io/dbt_transform/)** | [CONTRIBUTING.md](CONTRIBUTING.md) | [CONVENTIONS.md](CONVENTIONS.md)

---

## Quick start

```bash
git clone https://github.com/CebrailEVS/dbt_transform.git
cd dbt_transform
python3 -m venv dbt_venv && source dbt_venv/bin/activate
pip install -r requirements-lock.txt
cp .env.example .env   # editer avec vos valeurs
set -a && source .env && set +a
dbt deps && dbt debug
```

> Python 3.11+ requis. dbt v2 est un moteur Rust distribue comme extension CPython :
> Python reste necessaire a l'execution.

---

## Stack technique

| Composant | Role |
|---|---|
| **dbt v2** | Transformation et modelisation (ce repo) — moteur Rust, adaptateur BigQuery et linter inclus |
| **BigQuery** | Data Lake (`prod_raw`) + Data Warehouse |
| **dlt** *(repo `ingestion`)* | Extraction et chargement vers BigQuery |
| **Cloud Workflows** | Orchestration production (EL → dbt → refresh PBI) |
| **Cloud Scheduler** | Declenchement des workflows (cron, `infra/workflows_el.tf`) |
| **Cloud Run Jobs** | Execution des jobs dlt et dbt en production |
| **Cloud Storage** | Zone d'atterrissage pour les sources qui ne visent pas BigQuery directement |
| **GitHub Actions** | CI/CD |
| **Power BI** | Visualisation et reporting |

---

## Sources de donnees

**14 sources**, 115 tables declarees.

| Source | Systeme | Description |
|---|---|---|
| **oracle_neshu** | Oracle ERP (NESHU) | ERP principal : clients, machines, produits, taches |
| **oracle_lcdp** | Oracle ERP (LCDP) | ERP secondaire, meme schema qu'oracle_neshu |
| **yuman_api** | Yuman API | Interventions terrain : clients, sites, materiels, bons de travail |
| **mssql_sage** | MSSQL Sage | Comptabilite : ecritures, comptes tiers, collaborateurs |
| **nesp_tech** | Nomad Repair API | Interventions techniques Nespresso, pieces detachees |
| **nesp_co** | Excel / Nespresso | Donnees commerciales Nespresso |
| **zoho_desk** | Zoho Desk API | Tickets support, SLA, threads |
| **apptech** | App interne (tables externes GCS) | Suivi technicien : events, pauses, curatif, astreinte |
| **gac** | SFTP CSV | Assurance flotte, sinistres vehicules |
| **powerbi_activity** | API admin Power BI | Journaux d'usage du locataire + inventaire espaces/rapports/modeles |
| **yuman_evs_sftp** | Fichier SFTP | Stock theorique Yuman |
| **oracle_neshu_gcs** | Oracle ERP (NESHU) | Stock theorique Oracle NESHU |
| **oracle_lcdp_gcs** | Oracle ERP (LCDP) | Stock theorique Oracle LCDP |
| **historic** | Archive | Analytique Sage 2024, figee |

> **Tables externes GCS** : `nesp_tech` (2 tables) et `apptech` (8 tables) sont lues par
> dbt via des tables externes BigQuery adossees a GCS — l'extraction ne peut pas viser
> BigQuery directement (API specifique, fichiers ecrits par l'app). Verifie le 2026-09-23
> via `INFORMATION_SCHEMA.TABLES`. Toutes les autres sources sont des tables natives.
> `nesp_co` en faisait partie et n'en fait plus : ses 3 tables sont chargees directement
> par dlt.

> Le suffixe `_gcs` des deux sources de stock theorique est un **heritage** : depuis le
> 2026-08-06 elles sont chargees directement depuis Oracle par les pipelines dlt
> `oracle_neshu_stock` / `oracle_lcdp_stock`, sans CSV ni table externe. Le nom est
> conserve pour ne pas casser les `source()`. Voir
> [`docs/architecture/oracle_neshu_gcs.md`](docs/architecture/oracle_neshu_gcs.md).

Fraicheur des sources : **[`docs/freshness.md`](docs/freshness.md)** (autorite unique).

---

## Architecture

```
models/
├── staging/          Nettoyage, typage, standardisation — 1 modele = 1 table source
├── intermediate/     Logique metier, enrichissement — aligne par source
└── marts/            Dimensions + faits pour Power BI — organise par BU
```

**8 BU dans `marts/`** : `neshu`, `lcdp`, `technique`, `commerce`, `finance`,
`services_generaux`, `supply_chain`, `bi`. Staging et intermediate restent organises
**par source**, les marts **par domaine metier** (refacto terminee en mai 2026).

Voir la [documentation dbt generee](https://cebrailevs.github.io/dbt_transform/) pour le
detail de chaque modele, colonne et test.

### Zones de responsabilite

- **Data Engineer** : `staging/`, `intermediate/`, snapshots — qualite des sources et logique metier
- **Data Analyst** : `marts/` — analytics et reporting

### Couche IA (Claude Code)

Versionnee et partagee par l'equipe (seuls `.claude/settings.local.json` et
`.claude/notes/` restent locaux) :

```
CLAUDE.md              Contexte projet (architecture, conventions, hard rules)
.mcp.json              Serveurs MCP : BigQuery, Power BI, dbt (chemins absolus)
.claude/
├── commands/          /build-source, /new-mart, /lint-fix, /freshness
├── skills/            audit-sources, audit-docs, check-staging-relationships, profile
├── agents/            mart-reviewer (review adversariale d'un mart avant PR)
├── hooks/             PostToolUse (dbt lint, dbt parse) + helpers d'auth MCP
└── settings.json      Config des hooks (versionnee)
```

Les memes garde-fous tournent hors IA via `.pre-commit-config.yaml` (cf. [CONTRIBUTING.md](CONTRIBUTING.md)).

---

## Installation

### Prerequis

- Python 3.11+ (via `pyenv`, `.python-version`)
- Acces BigQuery avec cle de service GCP (une pour `dev`, une pour `prod`)

### Etapes

```bash
git clone https://github.com/CebrailEVS/dbt_transform.git
cd dbt_transform

python3 -m venv dbt_venv
source dbt_venv/bin/activate          # Windows : dbt_venv\Scripts\activate
pip install -r requirements-lock.txt

cp .env.example .env                  # ajuster les valeurs

# direnv (recommande) — charge .env automatiquement a chaque cd
sudo apt install direnv               # Linux ; macOS : brew install direnv
echo 'eval "$(direnv hook bash)"' >> ~/.bashrc && source ~/.bashrc
direnv allow                          # une seule fois dans le repo

dbt deps
dbt debug
```

> **Sans direnv**, charger les variables avant chaque session :
> `set -a && source .env && set +a`

### Environnements

Un seul projet GCP, `evs-datastack-prod`. L'isolation se fait par **dataset**, portee par
l'IAM (`infra/dbt_environments.tf`) : aucune identite hors prod n'ecrit dans `prod_*`.

| Target | Dataset(s) | Identite | Usage |
|---|---|---|---|
| `dev` *(defaut)* | `dbt_<toi>` (un seul dataset, toutes couches) | SA `dbt-dev`, cle locale | Developpement, `--defer` vers la prod |
| `ci` | `dbt_ci_pr_<N>` (cree puis supprime par la CI) | SA `dbt-ci`, WIF | `pr-check` |
| `prod` | `prod_staging`, `prod_intermediate`, `prod_marts`, `prod_reference` | Cloud Run / SA `dbt-deployer` (WIF) | Cloud Workflows et job `cd` |

> Les tables de `dbt_<toi>` et `dbt_ci_pr_<N>` **expirent seules** (14 j / 3 j sans rebuild).
> Ne jamais lancer `--target prod` en local.

---

## Commandes courantes

```bash
# Build par source (run + test en ordre DAG, fail-fast)
dbt build --select tag:oracle_neshu

# Build par couche
dbt build --select tag:staging
dbt build --select tag:marts

# Un modele et ses dependances amont / aval
dbt build --select +fct_neshu__consommation
dbt build --select fct_neshu__consommation+

# Fraicheur / seeds
dbt freshness
dbt seed
```

> **Selecteurs `+`** : `+modele` inclut tous ses **parents**, `modele+` tous ses
> **enfants** (utile pour verifier qu'un changement ne casse rien en aval), `+modele+` les deux.

> `dbt build` remplace `dbt run` + `dbt test` : chaque modele est teste avant que ses
> enfants soient construits. Si un test staging echoue, les marts ne sont pas batis sur
> des donnees fausses.

> Les snapshots sont executes par Cloud Workflows — ne pas les lancer manuellement.

---

## Linting SQL (`dbt lint`)

Le linter est integre a dbt v2. Il lit la configuration `.sqlfluff` (memes codes de
regles, memes `-- noqa`) et ne se connecte pas a BigQuery. SQLFluff n'est plus installe.

```bash
dbt lint models/staging/oracle_neshu/    # analyser
dbt lint models/staging/oracle_neshu/ --fix
dbt lint --changed                       # seulement ce que le working tree a modifie
```

Toujours verifier avec `git diff` apres un `--fix`. Regles : [CONVENTIONS.md](CONVENTIONS.md).

---

## CI/CD

| Evenement | Action |
|---|---|
| Pull Request vers `master` | `dbt lint` sur les modeles modifies, puis build `state:modified+` en dev avec `--defer` vers prod |
| Merge / push sur `master` | Build `state:modified+` **directement en prod**, docs generees et publiees, image `dbt-runner` repoussee |

> Les builds sont **incrementaux par etat** (`state:modified+` contre le manifest stocke
> sur GCS), jamais des reconstructions completes.

> Les snapshots sont **toujours exclus** du CI/CD — ils appartiennent a Cloud Workflows.

> Un push direct sur `master` declenche le job `cd` : le changement part en prod
> immediatement, pas seulement au merge d'une PR.

---

## Dependances

### Python (`requirements.txt`)

| Package | Version | Role |
|---|---|---|
| `dbt` | 2.0.6 | Moteur dbt v2 (Rust) — adaptateur BigQuery et linter inclus. Gratuit, licence proprietaire dbt Labs (l'alternative Apache 2.0 `dbt-oss` n'a pas `dbt lint`) |

### dbt packages (`packages.yml`)

| Package | Version | Role |
|---|---|---|
| `dbt_utils` | 1.4.1 | `unique_combination_of_columns`, `expression_is_true`, `generate_surrogate_key` |
| `dbt_expectations` | 0.10.10 | Row count ranges, date ranges, regex, taux de NULL |
| `dbt_orphan` | v0.2.0 (git) | Detection des objets orphelins (cf. [docs/maintenance.md](docs/maintenance.md)) |

---

## Depannage

```bash
dbt debug                              # verifier la configuration
direnv allow                           # direnv bloque ou non autorise
set -a && source .env && set +a        # variables non chargees (sans direnv)
dbt build --select tag:oracle_neshu    # cibler les modeles en echec
dbt lint models/staging/               # verifier le linting SQL
dbt ls --select +mon_modele            # voir les dependances d'un modele
rm -rf target/                         # artefact d'une ancienne version de dbt
```

---

## Ressources

- [Documentation dbt](https://docs.getdbt.com/) · [adaptateur BigQuery](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup) · [`dbt lint`](https://docs.getdbt.com/reference/commands/lint)
- [CONTRIBUTING.md](CONTRIBUTING.md) — workflow Git et collaboration
- [CONVENTIONS.md](CONVENTIONS.md) — conventions de nommage et qualite
- [docs/freshness.md](docs/freshness.md) — fraicheur des sources (autorite unique)
- [docs/pipeline-schedule.md](docs/pipeline-schedule.md) — circulation de la donnee, de la source au mart
- [docs/maintenance.md](docs/maintenance.md) — nettoyage des objets orphelins BigQuery

---

Developpe et maintenu par l'equipe Data EVS.
