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
DBT_BIGQUERY_PROJECT=evs-datastack-prod
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

## 🔄🤝 Workflow Git

# 🔄 Guide de Workflow Collaboratif DBT

Ce guide décrit notre processus de collaboration sur le projet DBT avec des environnements séparés et une CI/CD automatisée.

## 📋 Configuration des Environnements

### Répartition des datasets
- **Data Engineer** : `dev` (développement) + `prod` (production)
- **Data Analyst** : `dev_analyst` (développement personnel)

### Structure des zones de travail
```
models/
├── staging/
│   ├── oracle_neshu/    # 🔄 Zone partagée - coordination requise
│   └── yuman/           # 🔄 Zone partagée - coordination requise
├── intermediate/
│   └── oracle_neshu/    # 👤 Zone Data Engineer
└── marts/
    ├── oracle_neshu/    # 👤 Zone Data Engineer (business logic)
    └── yuman/           # 📊 Zone Data Analyst (analytics)
```

## 🚀 Workflow Quotidien

### 1. Démarrage de journée (OBLIGATOIRE)
```bash
# Toujours commencer par récupérer les dernières modifications
git checkout master
git pull origin master

# Puis aller sur votre branche ou en créer une nouvelle
git checkout feature/votre-branche
# OU créer une nouvelle
git checkout -b feature/nouvelle-fonctionnalite
```

### 2. Si votre branche existe depuis plusieurs jours
```bash
# Récupérer les modifications de master dans votre branche
git checkout feature/votre-branche
git rebase master  # Applique les nouveautés de master sur votre branche
```

**En cas de conflit pendant le rebase :**
```bash
# Résoudre les conflits dans les fichiers
git add .
git rebase --continue
```

## 💻 Pendant le Développement

### 3. Cycle de travail
```bash
# Développer vos modèles...
# Tester localement
dbt run --target votre_environnement
dbt test --target votre_environnement

# Commits fréquents (pas besoin de push à chaque fois)
git add .
git commit -m "feat: add dim_oracle_neshu__product model"
git commit -m "fix: correct join logic in telemetry_tasks"
```

### 4. Push seulement quand c'est validé
```bash
# Une fois vos tests passés et votre fonctionnalité terminée
git push origin feature/votre-branche
```

## 🔀 Processus de Pull Request

### 5. Avant de créer la PR (IMPORTANT)
```bash
# Synchroniser avec master une dernière fois
git checkout master
git pull origin master
git checkout feature/votre-branche
git rebase master

# Tests finaux
dbt run --target votre_environnement
dbt test --target votre_environnement

# Push final (avec --force-with-lease si rebase)
git push origin feature/votre-branche --force-with-lease
```

### 6. Création et validation des PR

#### Pour le Data Analyst :
1. Travailler sur le dataset `dev_analyst`
2. Tester les modèles : `dbt run --target dev_analyst`
3. Une fois validé, push de la branche et création d'une PR sur GitHub
4. Assigner le Data Engineer comme reviewer

#### Pour le Data Engineer :
1. Review de la PR sur GitHub
2. Tests de la branche si nécessaire :
   ```bash
   git fetch origin
   git checkout feature/branche-analyst
   dbt run --target dev  # Test sur le dataset dev
   ```
3. Approbation et merge de la PR

## 📐 Conventions de Nommage

### Branches
```bash
# Exemples de noms de branches valides
feature/modelisation-oracle_neshu
feature/analytics-yuman-workorders
feature/fix-telemetry-join
feature/add-product-dimensions
```

## 🚦 Règles de Collaboration

### Communication requise avant modification
Prévenez-vous mutuellement avant de modifier :
- `dbt_project.yml`
- Fichiers dans `staging/` (sources communes)
- `*_sources.yml` (définitions des sources)
- Macros partagées

### Processus de résolution de conflit
```bash
# Si conflit pendant un rebase
git status  # Voir les fichiers en conflit
# Éditer manuellement les fichiers
git add .
git rebase --continue

# Si conflit trop complexe, annuler et demander de l'aide
git rebase --abort
```

## ✅ Checklist Quotidienne

### Début de journée
- [ ] `git checkout master && git pull origin master`
- [ ] `git checkout ma-branche` (ou créer nouvelle branche)
- [ ] `git rebase master` (si branche existante)

### Pendant le travail
- [ ] Développement sur ma zone attribuée
- [ ] Tests locaux : `dbt run --target mon_env && dbt test --target mon_env`
- [ ] Commits réguliers avec messages clairs

### Fin de fonctionnalité
- [ ] Rebase final avec master
- [ ] Tests complets validés
- [ ] Push de la branche
- [ ] Création de la PR
- [ ] Review et merge

## 🔧 Scripts Utiles

### Test rapide de votre environnement
```bash
# Vérifier que tout fonctionne
dbt debug --target votre_environnement
dbt compile --target votre_environnement
```

## 🚨 Règles Importantes

### ❌ Ne JAMAIS faire
- Push direct sur `master`
- Modifier les fichiers de staging sans coordination
- Travailler directement sur `master`
- Merger ses propres PR sans review

### ✅ Toujours faire
- Partir de `master` à jour pour créer une nouvelle branche
- Tester localement avant de push
- Créer des PR pour toute modification
- Communiquer avant les gros changements

## 🔄 CI/CD Automatique

Le workflow GitHub Actions se déclenche automatiquement :
- Sur push vers `master` → Déploiement en `prod`
- Sur push vers `feature/**` → Tests en `dev`
- Sur Pull Request vers `master` → Tests de validation

## 📞 Support

En cas de problème ou de conflit complexe, n'hésitez pas à :
1. Faire `git rebase --abort` pour annuler une opération problématique
2. Demander de l'aide avant de forcer quoi que ce soit
3. Utiliser `git status` pour comprendre l'état actuel

---

*Ce workflow garantit une collaboration fluide et évite les conflits entre environnements !* 🎯

## 🔗 Liens utiles

- [Documentation DBT](https://docs.getdbt.com/)
- [DBT BigQuery Adapter](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup)
- [Repository GitHub](https://github.com/CebrailEVS/dbt_transform)