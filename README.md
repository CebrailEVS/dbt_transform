# EVS Data Warehouse — projet dbt

Couche **Transform** de la plateforme data d'**EVS Professionnelle France** : modélisation
BigQuery avec dbt, déclenchée par Google Cloud Workflows après chaque extraction.

**[Documentation dbt générée](https://cebrailevs.github.io/dbt_transform/)** ·
[CONTRIBUTING](CONTRIBUTING.md) · [CONVENTIONS](CONVENTIONS.md) ·
[Environnements & CI/CD](docs/environnements.md)

---

## Démarrage

```bash
git clone https://github.com/CebrailEVS/dbt_transform.git && cd dbt_transform
python3 -m venv dbt_venv && source dbt_venv/bin/activate
pip install -r requirements-lock.txt
cp .env.example .env          # dataset dbt_<toi> + chemin de ta clé dbt-dev
direnv allow                  # ou : set -a && source .env && set +a
dbt deps && dbt debug
scripts/pull-state.sh         # manifest prod, pour le --defer
```

Python 3.11+ est requis : dbt v2 est un moteur Rust distribué comme extension CPython.
Pour obtenir ta clé et ton dataset, voir [docs/environnements.md § 6](docs/environnements.md#6-ajouter-un-développeur).

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

## Modèles

```
models/
├── staging/        1 modèle = 1 table source : typage, nettoyage   (par source)
├── intermediate/   logique métier, enrichissement                  (par source)
└── marts/          dimensions et faits pour Power BI              (par BU)
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

| Paquet | Version | Rôle |
|---|---|---|
| `dbt` (pip) | 2.0.6 | Moteur dbt v2. Gratuit, licence propriétaire dbt Labs (`dbt-oss`, en Apache 2.0, n'a pas `dbt lint`) |
| `dbt_utils` | 1.4.1 | `unique_combination_of_columns`, `expression_is_true`, `generate_surrogate_key` |
| `dbt_expectations` | 0.10.10 | Row count, plages de dates, regex, taux de NULL |
| `dbt_orphan` | v0.2.0 (git) | Objets orphelins, cf. [docs/maintenance.md](docs/maintenance.md) |

---

## Couche IA (Claude Code)

Versionnée et partagée. Seuls `.claude/settings.local.json` et `.claude/notes/` restent locaux.

```
CLAUDE.md         contexte projet et règles strictes
.mcp.json         serveurs MCP : BigQuery, Power BI, dbt
.claude/
├── commands/     /build-source, /new-mart, /lint-fix, /freshness
├── skills/       audit-sources, audit-docs, check-staging-relationships
├── agents/       mart-reviewer
└── hooks/        dbt lint et dbt parse après édition, helpers d'auth MCP
```

`.pre-commit-config.yaml` rejoue les mêmes contrôles hors IA.

---

## Dépannage

```bash
dbt debug                          # configuration et connexion
set -a && source .env && set +a    # variables non chargées (sans direnv)
scripts/pull-state.sh              # defer qui pointe vers un modèle absent de la prod
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
