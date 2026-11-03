# EVS Data Warehouse — projet dbt

Couche **Transform** de la plateforme data d'**EVS Professionnelle France** : modélisation
BigQuery avec dbt, lancée par Cloud Workflows après chaque extraction.

**[Documentation dbt générée](https://cebrailevs.github.io/dbt_transform/)** : modèles, colonnes,
lignage, tests.

---

## Démarrage

Installation du poste, clé et dataset de dev : [CONTRIBUTING § 1](CONTRIBUTING.md#1-installer-son-poste).

---

## Stack

```
sources → dlt (repo ingestion) → BigQuery prod_raw → dbt (ce repo) → prod_* → Power BI
                         Cloud Workflows orchestre l'extraction puis dbt (repo infra)
```

| Composant | Rôle |
|---|---|
| **dlt** *(repo `ingestion`)* | Extraction et chargement vers `prod_raw`, sans typage métier |
| **BigQuery** | Lac (`prod_raw`) et entrepôt (`prod_staging` → `prod_intermediate` → `prod_marts`) |
| **dbt v2** *(ce repo)* | Transformation, tests, documentation, lint |
| **Cloud Workflows, Scheduler, Cloud Run** *(repo `infra`)* | Orchestration de la production |
| **GitHub Actions** | CI/CD, sans clé stockée (WIF) |
| **Power BI** | Restitution |

Versions : `requirements-lock.txt` (dbt) et `packages.yml` (paquets dbt). Choix structurants et
pistes écartées : [docs/decisions.md](docs/decisions.md).

---

## Sources de données

Une ligne par source déclarée dans dbt. Tables, colonnes et tests : la doc dbt générée.

| Source dbt | Système | Contenu | Particularités |
|---|---|---|---|
| `oracle_neshu` | ERP Oracle NESHU | clients, machines, produits, tâches | [oracle_neshu.md](docs/architecture/oracle_neshu.md) |
| `oracle_lcdp` | ERP Oracle LCDP | même schéma qu'`oracle_neshu` | [oracle_neshu.md](docs/architecture/oracle_neshu.md) |
| `oracle_nayka` | ERP Oracle du partenaire Nayka | même schéma qu'`oracle_neshu` | [oracle_neshu.md](docs/architecture/oracle_neshu.md) |
| `oracle_neshu_gcs`, `oracle_lcdp_gcs` | ERP Oracle NESHU et LCDP | photos du stock théorique | [oracle_neshu_gcs.md](docs/architecture/oracle_neshu_gcs.md) |
| `yuman_api` | API Yuman | interventions terrain : clients, sites, matériels, bons | [yuman.md](docs/architecture/yuman.md) |
| `yuman_evs_sftp` | fichier SFTP Yuman | stock théorique Yuman | [yuman_evs_sftp.md](docs/architecture/yuman_evs_sftp.md) |
| `mssql_sage` | Sage (SQL Server) | comptabilité et gestion commerciale | [mssql_sage.md](docs/architecture/mssql_sage.md) |
| `nesp_tech` | rapports du prestataire technique Nespresso | interventions, pièces détachées | [nesp_tech.md](docs/architecture/nesp_tech.md) |
| `nesp_co` | fichiers Nespresso | données commerciales | [nesp_co.md](docs/architecture/nesp_co.md) |
| `zoho_desk` | API Zoho Desk | tickets de support, SLA | [zoho_desk.md](docs/architecture/zoho_desk.md) |
| `gac` | fichiers SFTP | flotte de véhicules, sinistres | [gac.md](docs/architecture/gac.md) |
| `powerbi_activity` | API d'administration Power BI | usage et inventaire du parc Power BI | [powerbi_activity.md](docs/architecture/powerbi_activity.md) |
| `apptech` | application interne Suivi Tech | retraitements d'interventions techniques | [apptech.md](docs/architecture/apptech.md) |
| `dbt_artifacts` | runner dbt de prod | résultats des exécutions de prod | — |
| `historic` | archive | analytique Sage figée | — |

Le suffixe `_gcs` est historique : ces sources sont chargées depuis Oracle par dlt. Ne pas renommer
une source, chaque `source()` casserait.
Fraîcheur : [docs/freshness.md](docs/freshness.md).

---

## Modèles

```
models/
├── staging/        1 modèle = 1 table source : typage, nettoyage   (par source)
├── intermediate/   logique métier dans une source                  (par source)
├── marts/          dimensions et faits pour Power BI              (par BU)
├── apps/           modèles de service d'une application interne   (par application)
└── exposures/      rapports et applications qui lisent les modèles
snapshots/          historique SCD2, construit en prod seulement
data/reference_data/ seeds (référentiels CSV)
```

Une BU = un dossier de `models/marts/`. Qui possède quoi : [CONTRIBUTING § 6](CONTRIBUTING.md#6-rôles).

---

## Commandes courantes

```bash
dbt build -s mon_modele                 # ton modèle seul (parents lus en prod : defer)
dbt build -s mon_modele+                # + tout l'aval
dbt build -s tag:oracle_neshu           # toute une source
dbt build -s +exposure:<nom>            # tout ce qui alimente un rapport
dbt ls -s source:oracle_neshu+          # ce qu'une source reconstruit
dbt clone -s mon_incremental            # copier une table prod pour tester un MERGE
dbt lint models/chemin/ [--fix]         # lint SQL (règles : CONVENTIONS.md)
dbt source freshness                    # fraîcheur des sources (méthode : docs/freshness.md)
```

`dbt build` exécute les modèles et leurs tests dans l'ordre du DAG : un test en échec bloque
l'aval. Les snapshots ne se lancent jamais à la main.

CI/CD : une PR est construite dans son propre dataset ; un push sur `master`, même direct, part en
prod. Détail : [docs/environnements.md § 4](docs/environnements.md#4-les-quatre-flux).

---

## Dépannage

```bash
dbt debug                                     # configuration et connexion
set -a && source .env && set +a               # variables non chargées (sans direnv)
scripts/pull-state.sh --force                 # defer vers un modèle absent : retélécharger le manifest prod
dbt run-operation vider_dataset_dev           # résultats dev incohérents : lister ce qui serait supprimé
dbt run-operation vider_dataset_dev --args '{confirmer: true}'   # puis vider ton dataset
rm -rf target/                                # artefacts d'une ancienne version de dbt
```

---

## Documentation

| Sujet | Où |
|---|---|
| Contribuer : poste, cycle git, PR, ajouter un modèle | [CONTRIBUTING.md](CONTRIBUTING.md) |
| Conventions communes, puis une page par couche | [CONVENTIONS.md](CONVENTIONS.md), [docs/conventions/](docs/conventions/) |
| Environnements, identités, CI/CD, retour arrière | [docs/environnements.md](docs/environnements.md) |
| Circulation de la donnée en production | [docs/pipeline-schedule.md](docs/pipeline-schedule.md) |
| Fraîcheur des sources | [docs/freshness.md](docs/freshness.md) |
| Particularités et pièges de chaque source | [docs/architecture/](docs/architecture/) |
| Décisions d'architecture | [docs/decisions.md](docs/decisions.md) |
| Nettoyage des tables orphelines | [docs/maintenance.md](docs/maintenance.md) |
| Consignes à l'agent IA (Claude Code) | [CLAUDE.md](CLAUDE.md) ; `.claude/` versionné, sauf `settings.local.json` et `notes/` |
