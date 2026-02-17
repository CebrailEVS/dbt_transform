# EVS Data Warehouse - dbt Project

Projet dbt de transformation de donnees pour **EVS Professionnelle France**.
Pipeline ELT moderne : extraction via Meltano, transformation via dbt, orchestration via Airflow, visualisation via Power BI.

**[Documentation dbt generee](https://cebrailevs.github.io/dbt_transform/)** | [CONTRIBUTING.md](CONTRIBUTING.md) | [CONVENTIONS.md](CONVENTIONS.md)

---

## Quick start

```bash
git clone https://github.com/CebrailEVS/dbt_transform.git
cd dbt_transform
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # editer avec vos valeurs
set -a && source .env && set +a
dbt deps && dbt debug
```

> Python 3.11+ requis (gere automatiquement via `pyenv` et `.python-version`)

---

## Stack technique

| Composant          | Role                                     |
|--------------------|------------------------------------------|
| **Meltano**        | Extraction et chargement vers BigQuery   |
| **BigQuery**       | Data Lake (prod_raw) + Data Warehouse    |
| **dbt**            | Transformation et modelisation           |
| **Airflow**        | Orchestration production                 |
| **GitHub Actions** | CI/CD automatisee                        |
| **SQLFluff**       | Linting et formatage SQL                 |
| **Power BI**       | Visualisation et reporting               |

---

## Sources de donnees

Le projet integre **9 sources** couvrant l'ensemble des operations EVS :

| Source | Systeme | Description | Staging |
|--------|---------|-------------|---------|
| **oracle_neshu** | Oracle ERP (NESHU) | ERP principal : clients, machines, produits, taches | 24 |
| **oracle_lcdp** | Oracle ERP (LCDP) | ERP secondaire, meme schema qu'oracle_neshu | 24 |
| **yuman** | Yuman API | Interventions terrain : clients, sites, materiels, bons de travail | 11 |
| **nesp_tech** | Nomad Repair API | Interventions techniques Nespresso, pieces detachees | 2 |
| **nesp_co** | Excel / Nespresso | Donnees commerciales Nespresso (WIP) | 3 |
| **mssql_sage** | MSSQL Sage | Comptabilite : ecritures, comptes tiers, collaborateurs | 5 |
| **gac** | SFTP CSV | Assurance flotte, sinistres vehicules | 1 |
| **yuman_gcs** | GCS JSON | Stock theorique Yuman | 1 |
| **oracle_neshu_gcs** | GCS CSV | Stock theorique Oracle | 1 |

---

## Architecture

```
models/
├── staging/          74 modeles — Nettoyage, typage, standardisation
├── intermediate/     18 modeles — Logique metier, enrichissement, aggregations
└── marts/            23 modeles — Dimensions + facts pour Power BI
```

**6 domaines metier :** Operations (oracle_neshu), Service Technique (yuman, nesp_tech), Finance (mssql_sage), Flotte (gac), Stock (yuman_gcs, oracle_neshu_gcs), Commercial Nespresso (nesp_co - WIP).

Voir la [documentation dbt generee](https://cebrailevs.github.io/dbt_transform/) pour le detail de chaque modele, colonne et test.

### Zones de responsabilite

- **Data Engineer** : `staging/`, `intermediate/` — qualite des sources et logique metier
- **Data Analyst** : `intermediate/`, `marts/` — analytics et reporting

---

## Installation

### Prerequis

- Python 3.11+ (via `pyenv`, detecte automatiquement)
- Acces BigQuery avec cle de service GCP
- Variables d'environnement configurees (voir ci-dessous)

### Etapes

```bash
# 1. Cloner le repository
git clone https://github.com/CebrailEVS/dbt_transform.git
cd dbt_transform

# 2. Environnement virtuel (recommande)
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# ou venv\Scripts\activate  # Windows

# 3. Installer les dependances (dbt, dbt-bigquery, sqlfluff)
pip install -r requirements.txt

# 4. Configurer les variables d'environnement
cp .env.example .env  # Ajuster les valeurs

# 5. Charger les variables (OBLIGATOIRE avant chaque session dbt)
set -a && source .env && set +a

# 6. Installer les packages dbt
dbt deps

# 7. Tester la configuration
dbt debug
```

### Variables d'environnement (.env)

```bash
DBT_TARGET=dev
DBT_BIGQUERY_PROJECT=evs-datastack-prod
DBT_BIGQUERY_KEYFILE=/chemin/vers/votre/cle.json
DBT_BIGQUERY_DATASET_DEV=dev
DBT_BIGQUERY_DATASET_PROD=prod
```

### Environnements

| Environnement | Utilisateur | Schemas | Usage |
|---------------|-------------|---------|-------|
| `dev` | Data Engineer + Data Analyst | `dev_staging`, `dev_intermediate`, `dev_marts` | Developpement et tests |
| `prod` | Airflow | `prod_staging`, `prod_intermediate`, `prod_marts` | Production automatisee |

> Les deux profils lisent depuis `prod_raw` (memes sources). Seuls les schemas de destination changent.

---

## Commandes courantes

```bash
# Charger les variables (toujours en premier)
set -a && source .env && set +a

# Build par source (run + test en ordre DAG, fail-fast)
dbt build --select tag:oracle_neshu
dbt build --select tag:yuman

# Build par couche
dbt build --select tag:staging
dbt build --select tag:marts

# Build un modele et ses dependances amont
dbt build --select +fct_oracle_neshu__conso_business_review

# Freshness / Seeds / Snapshots
dbt source freshness
dbt seed
dbt snapshot

# Documentation (naviguer les modeles, colonnes, tests)
dbt docs generate && dbt docs serve
```

> `dbt build` remplace `dbt run` + `dbt test` : il execute et teste chaque modele avant de passer aux modeles enfants. Si un test staging echoue, les marts ne sont pas construits sur des donnees incorrectes.

---

## Linting SQL (SQLFluff)

Le projet utilise [SQLFluff](https://sqlfluff.com/) pour garantir un style SQL homogene. Configuration dans `.sqlfluff`.

```bash
# Analyser un fichier ou repertoire
sqlfluff lint models/staging/oracle_neshu/
sqlfluff lint models/

# Corriger automatiquement
sqlfluff fix models/staging/oracle_neshu/
```

Toujours verifier avec `git diff` apres un `sqlfluff fix`. Voir [CONVENTIONS.md](CONVENTIONS.md) pour les regles appliquees.

---

## CI/CD

| Evenement | Action |
|-----------|--------|
| Pull Request vers `master` | `dbt build --exclude resource_type:snapshot` sur dev (slim CI si manifest disponible) |
| Merge dans `master` | Build complet en prod + generation docs + deploiement GitHub Pages |

> Les snapshots sont exclus du CI pour eviter des captures SCD parasites. Ils sont geres uniquement par Airflow en production.

---

## Dependances

### Python (`requirements.txt`)

| Package | Version | Role |
|---------|---------|------|
| `dbt-core` | 1.11.3 | Framework de transformation |
| `dbt-bigquery` | 1.11.0 | Adaptateur BigQuery |
| `sqlfluff` | 4.0.0 | Linter SQL |
| `sqlfluff-templater-dbt` | 4.0.0 | Support Jinja/dbt pour SQLFluff |

### dbt packages (`packages.yml`)

| Package | Version | Role |
|---------|---------|------|
| `dbt-utils` | 1.1.1 | Tests avances (unique_combination, expression_is_true) |
| `dbt_expectations` | 0.10.10 | Tests de qualite (row count, date range, distributions) |

---

## Depannage

```bash
dbt debug                              # Verifier la configuration
set -a && source .env && set +a        # Variables non chargees
git rebase --abort                     # Annuler un rebase en echec
dbt build --select tag:oracle_neshu    # Cibler les modeles en echec
sqlfluff lint models/staging/          # Verifier le linting SQL
dbt ls --select +mon_modele            # Voir les dependances d'un modele
```

---

## Ressources

- [Documentation dbt](https://docs.getdbt.com/)
- [dbt BigQuery Adapter](https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile)
- [SQLFluff](https://docs.sqlfluff.com/)
- [dbt_expectations](https://github.com/metaplane/dbt-expectations)
- [CONTRIBUTING.md](CONTRIBUTING.md) — Workflow Git et collaboration
- [CONVENTIONS.md](CONVENTIONS.md) — Conventions de nommage et qualite

---

Developpe et maintenu par l'equipe Data EVS.
