# 🧱 EVS Data Transformation (dbt)

Transformations de données dbt pour **EVS Professionnelle France** dans une stack ELT moderne orchestrée via Airflow.

## 📋 Vue d'ensemble

### Stack Technique

| Composant       | Rôle                                    |
|-----------------|----------------------------------------|
| **Meltano**     | Extraction et chargement vers BigQuery |
| **BigQuery**    | Data Lake + Data Warehouse             |
| **dbt**         | Transformation et modélisation         |
| **Airflow**     | Orchestration production               |
| **GitHub Actions** | CI/CD automatisée                   |
| **Power BI**    | Visualisation et reporting             |

### Architecture des Modèles

```
models/
├── staging/          # 🔄 Nettoyage et standardisation (zone partagée)
│   ├── oracle_neshu/
│   └── yuman/
├── intermediate/     # 👤 Logique métier complexe (Data Engineer)
│   └── oracle_neshu/
└── marts/           # 📊 Modèles finaux orientés business
    ├── oracle_neshu/ # 👤 Business logic (Data Engineer)
    └── yuman/        # 📊 Analytics (Data Analyst)
```

## 🚀 Installation & Configuration

### Prérequis

- Python 3.10.13
- Accès BigQuery avec permissions appropriées
- Clé de service GCP configurée

### Installation

```bash
# 1. Cloner le repository
git clone https://github.com/CebrailEVS/dbt_transform.git
cd dbt_transform

# 2. Environnement virtuel (recommandé)
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# ou venv\Scripts\activate  # Windows

# 3. Installer dbt
pip install dbt-bigquery

# 4. Configurer les variables d'environnement
cp .env.example .env  # Ajuster les valeurs selon votre environnement

# 5. Charger les variables (OBLIGATOIRE avant chaque session dbt)
set -a && source .env && set +a

# 6. Tester la configuration
dbt debug
```

### Configuration des Variables

Fichier `.env` :
```bash
# Environment cible
DBT_TARGET=dev  # dev (Data Engineer) | dev_analyst (Data Analyst) | prod

# Configuration BigQuery
DBT_BIGQUERY_PROJECT=evs-datastack-prod
DBT_BIGQUERY_KEYFILE=/chemin/vers/votre/cle.json

# Datasets
DBT_BIGQUERY_DATASET_DEV=dev
DBT_BIGQUERY_DATASET_PROD=prod
```

## 🌍 Gestion des Environnements

### Répartition des Environnements

| Environnement | Utilisateur    | Dataset BigQuery | Usage                    |
|---------------|----------------|------------------|--------------------------|
| `dev`         | Data Engineer  | `dev`           | Développement principal  |
| `dev_analyst` | Data Analyst   | `dev_analyst`   | Développement analytique |
| `prod`        | Airflow        | `prod`          | Production automatisée   |

### Zones de Responsabilité

- **🔄 Zone Partagée** (`staging/`) : Coordination requise entre équipes
- **👤 Zone Data Engineer** (`intermediate/`, `marts/oracle_neshu/`) : Business logic
- **📊 Zone Data Analyst** (`marts/yuman/`) : Analytics et reporting

## 💻 Utilisation Quotidienne

### Commandes Essentielles

```bash
# ⚠️ TOUJOURS commencer par charger les variables
set -a && source .env && set +a

# Installation des dépendances
dbt deps

# Exécution des modèles
dbt run                              # Tous les modèles
dbt run --select stg_yuman__clients  # Modèle spécifique
dbt run --select staging.yuman      # Tous les modèles d'une source

# Tests de qualité
dbt test                             # Tous les tests
dbt test --select stg_yuman__clients # Tests d'un modèle

# Documentation
dbt docs generate && dbt docs serve
```

### Alias Utile

Ajoutez à votre `.bashrc` ou `.zshrc` :
```bash
alias dbt-env="set -a && source .env && set +a"
```

## 🤝 Workflow Collaboratif

### Démarrage de Journée

```bash
# 1. Récupérer les dernières modifications
git checkout master && git pull origin master

# 2. Travailler sur votre branche
git checkout feature/votre-branche
# ou créer une nouvelle branche
git checkout -b feature/nouvelle-fonctionnalite

# 3. Si votre branche est ancienne, synchroniser
git rebase master
```

### Cycle de Développement

```bash
# 1. Charger l'environnement
set -a && source .env && set +a

# 2. Développer et tester
dbt run --target votre_environnement
dbt test --target votre_environnement

# 3. Commits réguliers
git add .
git commit -m "feat: add dim_oracle_neshu__product model"

# 4. Push quand la fonctionnalité est prête
git push origin feature/votre-branche
```

### Processus de Pull Request

1. **Synchronisation finale avec master**
   ```bash
   git checkout master && git pull origin master
   git checkout feature/votre-branche && git rebase master
   ```

2. **Tests complets**
   ```bash
   dbt run --target votre_environnement
   dbt test --target votre_environnement
   ```

3. **Push et création de la PR**
   ```bash
   git push origin feature/votre-branche --force-with-lease
   ```

4. **Review** : Le Data Engineer review les PR du Data Analyst et vice versa

### Conventions de Nommage

```bash
# Branches
feature/modelisation-oracle_neshu
feature/analytics-yuman-workorders
feature/fix-telemetry-join

# Commits
feat: add new dimension model
fix: correct join logic in staging
docs: update model documentation
```

## 🧪 Tests et Qualité

Les tests dbt incluent :
- Tests d'unicité et de non-nullité
- Tests de référence entre modèles
- Tests personnalisés métier

Configuration dans les fichiers `*_models.yml` :
```yaml
models:
  - name: stg_yuman__clients
    tests:
      - unique:
          column_name: client_id
      - not_null:
          column_name: client_id
```

## 🚦 Règles de Collaboration

### ✅ Bonnes Pratiques

- Toujours partir de `master` à jour
- Tester localement avant de push
- Communiquer avant de modifier les zones partagées
- Utiliser des messages de commit descriptifs
- Créer des PR pour toute modification

### ❌ À Éviter

- Push direct sur `master`
- Modifier `staging/` sans coordination
- Merger ses propres PR sans review
- Ignorer les tests qui échouent

### Communication Requise

Prévenez-vous mutuellement avant de modifier :
- `dbt_project.yml`
- Fichiers dans `staging/` (sources communes)
- `*_sources.yml` (définitions des sources)
- Macros partagées

## 🔄 CI/CD Automatique

Le workflow GitHub Actions se déclenche automatiquement :

- **Push vers `master`** → Déploiement automatique en production
- **Push vers `feature/**`** → Tests de validation en dev
- **Pull Request** → Tests complets avant merge

## 📖 Documentation

La documentation est générée automatiquement et inclut :
- Lineage des modèles
- Description des colonnes
- Tests appliqués
- Métriques de qualité

Accès via `dbt docs serve` après `dbt docs generate`.

## 🆘 Dépannage

### Problèmes Courants

```bash
# Variables d'environnement non chargées
dbt debug  # Vérifier la configuration

# Conflits Git complexes
git rebase --abort  # Annuler et demander de l'aide

# Tests qui échouent
dbt test --select fail  # Voir uniquement les tests en échec
```

### Support

En cas de problème :
1. Utiliser `git status` pour comprendre l'état actuel
2. Faire `git rebase --abort` si nécessaire
3. Demander de l'aide avant de forcer des opérations

## 🔗 Ressources

- [Documentation dbt](https://docs.getdbt.com/)
- [dbt BigQuery Adapter](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup)
- [Repository GitHub](https://github.com/CebrailEVS/dbt_transform)

---

**Développé avec ❤️ par l'équipe Data EVS**