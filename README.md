# EVS Data Warehouse - dbt Project

Projet dbt de transformation de donnees pour **EVS Professionnelle France**.
Pipeline ELT moderne : extraction via Meltano, transformation via dbt, orchestration via Airflow, visualisation via Power BI.

**[Documentation dbt generee](https://cebrailevs.github.io/dbt_transform/)**

---

## Stack technique

| Composant          | Role                                     |
|--------------------|------------------------------------------|
| **Meltano**        | Extraction et chargement vers BigQuery   |
| **BigQuery**       | Data Lake (prod_raw) + Data Warehouse    |
| **dbt**            | Transformation et modelisation           |
| **Airflow**        | Orchestration production                 |
| **GitHub Actions** | CI/CD automatisee                        |
| **Power BI**       | Visualisation et reporting               |

---

## Sources de donnees

Le projet integre **9 sources** couvrant l'ensemble des operations EVS :

| Source | Systeme | Description | Nb staging models |
|--------|---------|-------------|-------------------|
| **oracle_neshu** | Oracle ERP (NESHU) | ERP principal : clients, machines, produits, taches (appro, conso, chargement, livraison, telemetrie) | 24 |
| **oracle_lcdp** | Oracle ERP (LCDP) | ERP secondaire : meme schema qu'oracle_neshu, base de donnees differente | 24 |
| **yuman** | Yuman API | Plateforme de gestion des interventions terrain : clients, sites, materiels, bons de travail | 11 |
| **nesp_tech** | Nomad Repair API | Interventions techniques Nespresso : reparations, pieces detachees, delais SLA | 2 |
| **nesp_co** | Excel / Nespresso | Donnees commerciales Nespresso : activites, opportunites, base clients (WIP - staging uniquement) | 3 |
| **mssql_sage** | MSSQL Sage | Comptabilite : ecritures comptables/analytiques, comptes tiers, collaborateurs | 5 |
| **gac** | SFTP CSV | Assurance flotte : suivi des sinistres vehicules | 1 |
| **yuman_gcs** | GCS / SFTP JSON | Stock theorique Yuman (extraction externe) | 1 |
| **oracle_neshu_gcs** | GCS / CSV | Stock theorique Oracle (extraction nightly) | 1 |

---

## Architecture des modeles

```
models/
├── staging/                  Nettoyage, typage, standardisation des sources brutes
│   ├── oracle_neshu/         (24 modeles)
│   ├── oracle_lcdp/          (24 modeles)
│   ├── yuman/                (11 modeles)
│   ├── mssql_sage/           (5 modeles)
│   ├── nesp_co/              (3 modeles) [WIP]
│   ├── nesp_tech/            (2 modeles)
│   ├── gac/                  (1 modele)
│   ├── yuman_gcs/            (1 modele)
│   └── oracle_neshu_gcs/     (1 modele)
│
├── intermediate/             Logique metier, enrichissement, aggregations par type de tache
│   ├── oracle_neshu/         (14 modeles : appro, chargement, telemetrie, pointage, etc.)
│   ├── yuman/                (1 modele : demands + workorders enrichis)
│   ├── nesp_tech/            (2 modeles : delais + facturation interventions)
│   └── mssql_sage/           (1 modele : P&L par BU)
│
└── marts/                    Tables finales dimensions + facts pour Power BI
    ├── oracle_neshu/         (9 modeles : dims company/device/product + facts conso/appro)
    ├── yuman/                (6 modeles : dims clients/sites/materials + facts pricing/delais)
    ├── mssql_sage/           (1 modele : KPIs P&L par BU)
    ├── nesp_tech/            (1 modele : pricing pieces detachees)
    ├── gac/                  (1 modele : sinistres)
    ├── oracle_neshu_gcs/     (1 modele : stock produits)
    └── yuman_gcs/            (1 modele : stock articles)
```

---

## Domaines metier et modeles marts

Les rapports Power BI sont organises par domaine metier. Voici la correspondance entre domaines et modeles dbt :

### Operations & Approvisionnement
*Source principale : oracle_neshu*

| Modele | Description |
|--------|-------------|
| `dim_oracle_neshu__company` | Dimension clients enrichie (region, secteur, statut, KA, modele economique) via 23 familles de labels |
| `dim_oracle_neshu__device` | Dimension machines (marque, gamme, categorie, statut actif) |
| `dim_oracle_neshu__product` | Dimension produits (famille, groupe, marque, proprietaire, BIO) |
| `dim_oracle_neshu__contract` | Dimension contrats (engagement, nombre collaborateurs, dates) |
| `dim_oracle_neshu__vehicule_roadman` | Dimension vehicules et roadmen (code GEA) |
| `fct_oracle_neshu__conso_business_review` | Consommation unifiee : telemetrie + chargement + livraison |
| `fct_oracle_neshu__pa_business_review` | Passages approvisionneurs (missions prevues/faites) |
| `fct_oracle_neshu__appro` | Detail passages avec metriques temps (duree, rang journalier, pointage) |
| `fct_oracle_neshu__chargement_par_quinzaine` | Chargements agreges par quinzaine |
| `fct_oracle_neshu__chargement_vs_conso` | Reconciliation chargement vs consommation par machine |

### Service Technique & Interventions Terrain
*Sources : yuman, nesp_tech*

| Modele | Description |
|--------|-------------|
| `dim_yuman__clients` | Dimension clients Yuman (code, categorie, partenaire) |
| `dim_yuman__sites` | Dimension sites avec agence |
| `dim_yuman__materials` | Dimension materiels avec categorie |
| `dim_yuman__materials_clients` | Vue denormalisee client + site + materiel |
| `fct_yuman__workorder_pricing` | Tarification interventions (lookup type x machine x zone x partenaire) |
| `fct_yuman__workorder_delais_neshu` | Suivi SLA interventions avec metriques delais |
| `fct_nesp_tech__pieces_detachees_pricing` | Facturation pieces detachees par intervention |

### Maintenance Equipements
*Sources : oracle_neshu + yuman (croisement)*

| Modele | Description |
|--------|-------------|
| `fct_oracle_neshu__machines_maintenance_tracking` | Suivi maintenance preventive : retard, delais, derniere intervention |

### Finance & Comptabilite
*Source : mssql_sage*

| Modele | Description |
|--------|-------------|
| `fct_mssql_sage__pnl_bu_kpis` | KPIs P&L par BU : CA, marge brute/nette, masse salariale (mensuel, YTD, N-1, evolution %) |

### Flotte & Assurance
*Source : gac*

| Modele | Description |
|--------|-------------|
| `fct_gac__sinistres_sg` | Suivi sinistres vehicules : couts (assureur, auto-assurance, franchise, client) |

### Inventaire & Stock
*Sources : yuman_gcs, oracle_neshu_gcs*

| Modele | Description |
|--------|-------------|
| `fct_yuman_gcs__stock_articles` | Stock theorique articles Yuman |
| `fct_oracle_neshu_gcs__stock_products` | Stock quotidien produits Oracle (ruptures, prix achat, derniere date inventaire) |

### Commercial Nespresso [WIP]
*Source : nesp_co (staging uniquement, marts a venir)*

| Modele staging | Description |
|----------------|-------------|
| `stg_nesp_co__activite` | Activites commerciales (appels, RDV) |
| `stg_nesp_co__opportunite` | Pipeline opportunites |
| `stg_nesp_co__client` | Base clients (volumes capsules, scores NES) |

---

## Donnees de reference (seeds)

```
data/reference_data/
├── general/
│   └── ref_general__feries_metropole.csv       Calendrier jours feries
├── yuman/                                       14 fichiers
│   ├── ref_yuman__tarification_clean.csv        Matrice tarifs (type x machine x zone)
│   ├── ref_yuman__technicien_clean.csv          Referentiel techniciens
│   ├── ref_yuman__tech_agence.csv               Mapping technicien -> agence
│   ├── ref_yuman__cp_metropole.csv              Code postal -> metropole
│   └── ...                                      Machine, marque, type inter (nettoyage)
├── nesp_tech/                                   6 fichiers
│   ├── ref_nesp_tech__articles_prix.csv         Prix unitaires pieces
│   ├── ref_nesp_tech__key_facturation.csv       Cles de facturation
│   └── ...                                      Machines, type inter, zones montagne
├── oracle_neshu/                                3 fichiers
│   ├── ref_oracle_neshu__roadman_gea.csv        Mapping roadman -> code GEA
│   └── ref_oracle_neshu__valo_*.csv             Valorisation parc machines
└── mssql_sage/                                  2 fichiers
    ├── ref_mssql_sage__code_analytique_bu.csv   Code analytique -> BU
    └── ref_mssql_sage__code_comptable_bu.csv    Compte comptable -> categorie P&L
```

---

## Snapshots (historisation)

Suivi des changements via strategie `check` :

| Snapshot | Colonnes suivies | Usage |
|----------|-----------------|-------|
| `snap_oracle_neshu__company` | proadman, modele_economique, is_active | Historique changements clients |
| `snap_oracle_neshu__device` | Configuration machines | Historique equipements |
| `snap_oracle_neshu__valo_parc_machines` | Valorisation parc | Evolution valeur parc |

---

## Conventions de nommage

### Modeles
```
stg_<source>__<entite>              Staging (ex: stg_oracle_neshu__company)
int_<source>__<entite>              Intermediate (ex: int_oracle_neshu__appro_tasks)
dim_<source>__<entite>              Dimension mart (ex: dim_oracle_neshu__company)
fct_<source>__<entite>              Fact mart (ex: fct_oracle_neshu__conso_business_review)
snap_<source>__<entite>             Snapshot (ex: snap_oracle_neshu__company)
ref_<source>__<entite>              Seed (ex: ref_oracle_neshu__roadman_gea)
```

### Colonnes standardisees
```
id<entite>                          Cle primaire (ex: idcompany, idtask)
id<entite_fk>                       Cle etrangere (ex: idcompany_peer, iddevice)
created_at                          Date de creation
updated_at                          Date de modification (COALESCE avec creation_date)
extracted_at                        Timestamp d'extraction Meltano (_sdc_extracted_at)
deleted_at                          Soft delete (_sdc_deleted_at)
code_status_record                  Statut enregistrement (1=actif, 0/-1=inactif)
```

### Branches Git
```
feature/<domaine>-<description>     Ex: feature/modelisation-oracle-lcdp
fix/<description>                   Ex: fix/telemetry-join-orphelins
docs/<description>                  Ex: docs/update-readme
```

### Commits (Conventional Commits)
```
feat: add dim_oracle_neshu__product model
fix: correct join logic in staging task
docs: update model documentation
```

---

## Freshness monitoring

| Tier | Tables | Seuils | Exemple |
|------|--------|--------|---------|
| **Fact tables** | Taches, produits, ressources | warn 26h / error 36h | evs_task, lcdp_task |
| **Dimensions operationnelles** | Clients, machines, contrats | warn 26h / error 36h | evs_company, evs_device |
| **Referentiels stables** | Types, labels, familles | Freshness desactivee | evs_task_type, evs_label |

Champ timestamp : `_sdc_extracted_at` (convention Meltano/Singer)

---

## Installation et configuration

### Prerequis

- Python 3.10+
- Acces BigQuery avec permissions appropriees
- Cle de service GCP configuree

### Installation

```bash
# Cloner le repository
git clone https://github.com/CebrailEVS/dbt_transform.git
cd dbt_transform

# Environnement virtuel
python3 -m venv venv
source venv/bin/activate

# Installer dbt
pip install dbt-bigquery

# Configurer les variables d'environnement
cp .env.example .env  # Ajuster les valeurs

# Charger les variables (OBLIGATOIRE avant chaque session dbt)
set -a && source .env && set +a

# Installer les dependances dbt
dbt deps

# Tester la configuration
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

---

## Environnements

| Environnement | Utilisateur | Dataset BigQuery | Usage |
|---------------|-------------|------------------|-------|
| `dev` | Data Engineer / Analyst | `dev` | Developpement et tests |
| `prod` | Airflow | `prod` | Production automatisee |

---

## Commandes courantes

```bash
# Charger les variables (toujours en premier)
set -a && source .env && set +a

# Executer tous les modeles
dbt run

# Executer par source
dbt run --select tag:oracle_neshu
dbt run --select tag:oracle_lcdp
dbt run --select tag:yuman

# Executer par couche
dbt run --select tag:staging
dbt run --select tag:intermediate
dbt run --select tag:marts

# Executer un modele et ses dependances amont
dbt run --select +fct_oracle_neshu__conso_business_review

# Tests
dbt test
dbt test --select tag:oracle_neshu

# Freshness
dbt source freshness

# Seeds
dbt seed

# Snapshots
dbt snapshot

# Documentation
dbt docs generate && dbt docs serve
```

---

## CI/CD

| Evenement | Action |
|-----------|--------|
| Push vers `master` | Deploiement automatique en production |
| Push vers `feature/**` | Tests de validation en dev |
| Pull Request | Tests complets avant merge |

---

## Depannage

```bash
# Verifier la configuration
dbt debug

# Variables non chargees
set -a && source .env && set +a

# Conflit Git
git rebase --abort  # Annuler si necessaire

# Voir les tests en echec
dbt test --select tag:oracle_neshu  # Cibler par source
```

---

## Dependances

- `dbt-utils 1.1.1` : Tests avances (unique_combination_of_columns, accepted_range, expression_is_true)

---

Developpe et maintenu par l'equipe Data EVS.
