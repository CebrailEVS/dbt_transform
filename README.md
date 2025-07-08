# 🧱 EVS Data Transformation (dbt)

Ce dépôt contient les transformations de données dbt (Data Build Tool) utilisées par **EVS Professionnelle France** dans une stack ELT moderne orchestrée via **Airflow**.  
Les données sont extraites et chargées via **Meltano**, transformées avec **dbt** dans **BigQuery**, et visualisées dans **Power BI**.

---

## 🚀 Stack Data Modernisée

| Composant       | Rôle                                |
|------------------|--------------------------------------|
| Meltano          | Extraction et chargement vers BigQuery |
| BigQuery         | Lake + Warehouse principal            |
| dbt              | Transformation et modélisation        |
| Airflow (HTTPS)  | Orchestration prod automatique        |
| GitHub Actions   | Intégration continue (CI/CD dbt)      |
| PostgreSQL local | Stockage métadonnées Meltano/Airflow |
| Power BI         | Visualisation                        |

---

## 🏗️ Architecture

```
transform/
├── models/
│   ├── staging/          # Modèles de staging (nettoyage, standardisation)
│   │   ├── oracle_neshu/
│   │   └── yuman/
│   └── marts/            # Modèles finaux orientés métier
│       ├── oracle_neshu/
│       └── yuman/
├── .env                  # Variables d'environnement
├── dbt_project.yml       # Configuration principale du projet
└── profiles.yml          # Configuration des connexions
```

## 🔧 Prérequis

- Python 3.10.13 (géré via pyenv)
- DBT installé via pipx
- Accès à BigQuery avec les bonnes permissions
- Clé de service GCP configurée

## ⚙️ Installation & Configuration

### 1. Cloner le repository
```bash
git clone https://github.com/CebrailEVS/dbt_transform.git
cd dbt_transform
```

### 2. Configuration Python et DBT

**Option A : Avec venv (recommandé pour débuter)**
```bash
# Créer un environnement virtuel
python3 -m venv venv

# Activer l'environnement
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows

# Installer DBT
pip install dbt-bigquery
```

**Option B : Avec pyenv + pipx (configuration avancée)**
```bash
# Installer la version Python requise
pyenv install 3.10.13
pyenv local 3.10.13

# Installer DBT via pipx
pipx install dbt-bigquery
```

### 3. Configuration des variables d'environnement
Ajuster les valeurs dans le fichier `.env` selon votre environnement :

```bash
# Environment target
DBT_TARGET=dev  # dev, dev_analyst, ou prod

# BigQuery Configuration
DBT_BIGQUERY_PROJECT=meltano-test-data
DBT_BIGQUERY_KEYFILE=/chemin/vers/votre/cle.json

# Datasets centralisés
DBT_BIGQUERY_DATASET_DEV=dev
DBT_BIGQUERY_DATASET_PROD=prod
```

### 4. Charger les variables d'environnement
**IMPORTANT** : Avant chaque session DBT, exporter les variables :

```bash
# Charger les variables d'environnement
set -a && source .env && set +a

# Ou créer un alias dans votre .bashrc/.zshrc :
alias dbt-env="set -a && source .env && set +a"
```

### 5. Test de la configuration
```bash
dbt debug
```

## 🚀 Utilisation

### Workflow quotidien

**1. Charger les variables d'environnement (obligatoire à chaque session)**
```bash
set -a && source .env && set +a
```

### Commandes principales

```bash
# Installer les dépendances
dbt deps

# Tester les modèles
dbt test

# Compiler les modèles (sans exécution)
dbt compile

# Exécuter tous les modèles
dbt run

# Exécuter un modèle spécifique
dbt run --select stg_yuman__clients

# Exécuter tous les modèles d'une source
dbt run --select staging.yuman

# Générer la documentation
dbt docs generate
dbt docs serve
```

### Développement par couches

1. **Staging** : Nettoyage et standardisation des données brutes
2. **Marts** : Modèles finaux orientés métier, prêts pour l'analyse

## 🌍 Gestion des environnements

### 🔐 Environnements dbt
La variable `DBT_TARGET` (définie dans `.env`) contrôle l'environnement actif.

| Environnement     | Utilisé pour         | Dataset BigQuery      | Accès          |
|-------------------|----------------------|------------------------|----------------|
| `dev`             | Développement local  | `dev`                  | Lead + Analyst |
| `dev_analyst`     | Test analyste isolé  | `dev_analyst`          | Analyste seul  |
| `prod`            | Orchestration Airflow| `prod`                 | Lead seulement |

Le fichier `.env` contient :
```env
DBT_TARGET=dev
DBT_BIGQUERY_PROJECT=meltano-test-data
DBT_BIGQUERY_KEYFILE=/opt/credentials/gcp-meltano-key.json
DBT_BIGQUERY_DATASET_DEV=dev
DBT_BIGQUERY_DATASET_PROD=prod
```

### Workflow de déploiement

1. **Développement local** : 
   - Lead : Travail en environnement `dev`
   - Analyst : Travail en environnement `dev_analyst`
2. **Tests et validation** : Tests locaux puis push sur branche feature
3. **Pull Request** : Création d'une PR vers `master`
4. **CI/CD** : Tests automatiques via GitHub Actions
5. **Review** : Validation par le lead data uniquement
6. **Déploiement** : Merge vers `master` → Airflow déploie automatiquement en `prod`

**Important** : Seul le lead data a accès à la production. Airflow se charge du déploiement continu en production.

## 🔄 Workflow Git

### Pour le Data Analyst

```bash
# 1. TOUJOURS charger les variables d'environnement en premier
set -a && source .env && set +a

# 2. Créer une branche pour vos développements
git checkout -b feature/nouveau-modele

# 3. Développer et tester localement (en dev_analyst)
dbt run --select votre_modele
dbt test --select votre_modele

# 4. Commit et push
git add .
git commit -m "Ajout du modèle xxx"
git push origin feature/nouveau-modele

# 5. Créer une Pull Request vers master sur GitHub
```

### Bonnes pratiques

- Une branche par fonctionnalité/modèle
- Tests locaux avant push
- Messages de commit descriptifs
- Documentation des modèles dans les fichiers `.yml`

## 🧪 Tests et Qualité

Les tests DBT sont définis dans les fichiers `*_models.yml` et incluent :
- Tests d'unicité
- Tests de non-nullité
- Tests de référence
- Tests personnalisés

```bash
# Exécuter tous les tests
dbt test

# Tests sur un modèle spécifique
dbt test --select stg_yuman__clients
```

## 📖 Documentation

La documentation est générée automatiquement par DBT :

```bash
dbt docs generate
dbt docs serve  # Ouvre la doc dans le navigateur
```

## 🤝 Contribution

1. Forker le projet ou créer une branche
2. Développer en suivant nos conventions de nommage
3. Tester localement
4. Créer une Pull Request avec description détaillée
5. Attendre la review et validation

## 🔗 Liens utiles

- [Documentation DBT](https://docs.getdbt.com/)
- [DBT BigQuery Adapter](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup)
- [Repository GitHub](https://github.com/CebrailEVS/dbt_transform)