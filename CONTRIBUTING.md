# Contributing - EVS dbt Project

Guide de collaboration pour le projet dbt EVS. A lire avant toute contribution.

---

## Workflow Git

### Branches

| Branche | Usage |
|---------|-------|
| `master` | Production. Deploiement automatique via CI/CD. |
| `feature/<source>/<description>` | Nouvelle fonctionnalite ou ajout de modeles. |
| `fix/<source>/<description>` | Correction de bug. |

Exemples :
- `feature/oracle_neshu/add-valorisation-parc`
- `fix/yuman/workorder-deduplication`

### Workflow quotidien

```bash
# 1. Toujours partir de master a jour
git checkout master && git pull

# 2. Creer une branche
git checkout -b feature/oracle_neshu/add-kpi-livraison

# 3. Charger les variables d'environnement
set -a && source .env && set +a

# 4. Developper et tester (build = run + test en ordre DAG)
dbt build --select tag:oracle_neshu

# 5. Linter le SQL
sqlfluff lint models/staging/oracle_neshu/
sqlfluff lint models/intermediate/oracle_neshu/

# 6. Commiter
git add models/staging/oracle_neshu/...
git commit -m "feat(oracle_neshu): add KPI livraison model"

# 7. Pousser et creer une PR
git push -u origin feature/oracle_neshu/add-kpi-livraison
```

### Commits

Format : `type(scope): description courte`

| Type | Usage |
|------|-------|
| `feat` | Nouveau modele ou fonctionnalite |
| `fix` | Correction de bug |
| `refactor` | Restructuration sans changement fonctionnel |
| `test` | Ajout ou modification de tests |
| `docs` | Documentation uniquement |
| `chore` | Maintenance (deps, config, seeds) |

Exemples :
- `feat(oracle_neshu): add int_oracle_neshu__pointage model`
- `fix(yuman): deduplicate workorder_demands on id`
- `test(mssql_sage): add row count expectations on f_ecriturea`
- `docs: update README with new sources`

---

## Pull Requests

### Avant de creer une PR

1. **Tester localement** : `dbt build` sur les modeles concernes
2. **Linter** : `sqlfluff lint` sans violations bloquantes
3. **Verifier les dependances** : `dbt build --select +mon_modele` (amont complet)
4. **Pas de secrets** : ne jamais commiter `.env`, cles GCP, credentials

### Contenu de la PR

- Titre clair avec le scope : `feat(yuman): add workorder pricing mart`
- Description : quoi, pourquoi, et comment tester
- Lister les modeles ajoutes ou modifies

### Apres merge

```bash
git checkout master && git pull
git branch -d feature/oracle_neshu/add-kpi-livraison  # supprimer la branche locale
```

---

## Ajouter un nouveau modele

### Staging

1. Creer le fichier SQL dans `models/staging/<source>/`
2. Nommer : `stg_<source>__<table>.sql`
3. Ajouter le modele dans `_<source>__models.yml` (description + tests)
4. Verifier que la source existe dans `_<source>__sources.yml`
5. Ajouter les tags dans `dbt_project.yml` si nouvelle source

### Intermediate

1. Creer dans `models/intermediate/<source>/`
2. Nommer : `int_<source>__<description>.sql`
3. Utiliser uniquement des `ref()` vers staging ou d'autres intermediate
4. Documenter dans le YAML de la source correspondante

### Marts

1. Creer dans `models/marts/<source>/`
2. Nommer : `dim_<source>__<entite>.sql` ou `fct_<source>__<metrique>.sql`
3. C'est la couche exposee a Power BI — veiller a la clarte des colonnes

---

## Ajouter une nouvelle source

1. Creer le dossier `models/staging/<nouvelle_source>/`
2. Creer `_<source>__sources.yml` avec freshness et colonnes cles
3. Creer `_<source>__models.yml` avec descriptions et tests
4. Ajouter les tags dans `dbt_project.yml`
5. Creer les modeles staging, puis intermediate, puis marts selon le besoin
6. Mettre a jour le tableau des sources dans `README.md`

---

## Checklist avant merge

- [ ] `dbt build` passe sans erreur sur les modeles concernes (PASS ou WARN accepte, pas d'ERROR)
- [ ] `sqlfluff lint` sans violations sur les fichiers modifies
- [ ] Modeles documentes dans les fichiers YAML
- [ ] Pas de secrets dans le commit
- [ ] PR avec description claire

---

## Environnement de developpement

| Env | Schemas | Utilisateur | Usage |
|-----|---------|-------------|-------|
| `dev` | `dev_staging`, `dev_intermediate`, `dev_marts` | Data Engineer + Data Analyst | Developpement et tests |
| `prod` | `prod_staging`, `prod_intermediate`, `prod_marts` | Airflow | Production automatisee |

Les deux environnements lisent depuis `prod_raw` (meme source). Seuls les schemas de destination changent. Configurer `DBT_TARGET=dev` dans `.env`.

---

## Roles et responsabilites

| Action | Data Engineer | Data Analyst |
|--------|:---:|:---:|
| Modifier `staging/` | Oui | Non |
| Modifier `intermediate/` | Oui | Sur validation DE |
| Creer/modifier `marts/` | Oui | Oui |
| Ajouter des seeds (CSV) | Oui | Oui |
| Modifier `snapshots/` | Oui | Non |
| Modifier `dbt_project.yml` | Oui | Non |
| Pousser directement sur `master` | Non | Non |
| Creer une PR | Oui | Oui |
| Reviewer une PR | Oui | - |

> Toute contribution passe par une PR. Le Data Engineer review et merge.

---

## Explorer le projet existant

Avant de creer de nouveaux modeles, prendre le temps de comprendre ce qui existe :

```bash
# Naviguer visuellement tous les modeles, colonnes et tests
dbt docs generate && dbt docs serve

# Lister les modeles d'un domaine
dbt ls --select tag:oracle_neshu

# Voir les dependances amont d'un modele
dbt ls --select +fct_oracle_neshu__conso_business_review

# Voir les dependances aval d'un modele
dbt ls --select fct_oracle_neshu__conso_business_review+
```

La documentation generee est aussi disponible en ligne : [https://cebrailevs.github.io/dbt_transform/](https://cebrailevs.github.io/dbt_transform/)

---

## Besoin d'aide ?

- `dbt debug` pour verifier la configuration
- `dbt docs generate && dbt docs serve` pour naviguer les modeles
- Consulter [CONVENTIONS.md](CONVENTIONS.md) pour les regles de nommage et qualite
