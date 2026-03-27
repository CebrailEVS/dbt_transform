# Contributing - EVS dbt Project

Guide de collaboration pour le projet dbt EVS. A lire avant toute contribution.

---

## Installation de l'environnement local

### Premier démarrage

```bash
# 1. Cloner le repo
git clone https://github.com/CebrailEVS/dbt_transform.git
cd dbt_transform

# 2. Créer un environnement virtuel Python
python3 -m venv dbt_venv
source dbt_venv/bin/activate  # Windows : dbt_venv\Scripts\activate

# 3. Installer les dépendances exactes
pip install -r requirements-lock.txt

# 4. Copier et remplir le fichier d'environnement
cp .env.example .env  # puis remplir les variables avec le Data Engineer

# 5. Installer direnv (charge .env automatiquement a chaque cd dans le repo)
sudo apt install direnv                  # Linux
# brew install direnv                    # macOS
echo 'eval "$(direnv hook bash)"' >> ~/.bashrc && source ~/.bashrc
direnv allow                             # a faire une seule fois dans le repo

# 6. Vérifier que tout fonctionne
dbt debug --target dev
```

> `direnv allow` est a executer une seule fois par machine/repo. Apres ca, les variables `.env` sont chargees automatiquement des que tu entres dans le dossier.
> **Sans direnv**, charge les variables manuellement avant chaque session : `set -a && source .env && set +a`

### Comprendre les fichiers de dépendances

| Fichier | Rôle | Modifié par |
|---------|------|-------------|
| `requirements.txt` | Dépendances directes avec versions fixes (`dbt-bigquery==1.11.1`) | Data Engineer uniquement |
| `requirements-lock.txt` | Toutes les dépendances (y compris transitives) figées exactement | Généré automatiquement |

**Règle simple :**
- Tu installes toujours depuis `requirements-lock.txt` → environnement identique pour toute l'équipe
- Tu ne modifies jamais ces fichiers toi-même → c'est le rôle du Data Engineer

### Mettre à jour ses dépendances

Si le Data Engineer a mis à jour les versions (dbt, sqlfluff...) :

```bash
git pull
pip install -r requirements-lock.txt
```

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

# 3. Developper et tester (build = run + test en ordre DAG)
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

### Mettre a jour sa branche avant une PR

Si `master` a avance depuis la creation de ta branche, il faut rebaser avant d'ouvrir (ou de mettre a jour) ta PR. Cela evite les "Update branch" sur GitHub et garde un historique lineaire.

```bash
# 1. Mettre master a jour en local
git checkout master
git pull origin master

# 2. Rebaser ta branche sur master
git checkout ma_branche
git rebase master

# 3. Pousser (force necessaire apres un rebase)
git push --force-with-lease
```

> `--force-with-lease` est plus sur que `--force` : il echoue si quelqu'un d'autre a pousse sur ta branche entre-temps.

En cas de conflit pendant le rebase :
```bash
# Resoudre le conflit dans le fichier concerne, puis :
git add fichier_en_conflit
git rebase --continue
```

---

## Pull Requests

### Avant de creer une PR

1. **Tester localement** : `dbt build` sur les modeles concernes
2. **Linter** : `sqlfluff lint` sans violations bloquantes
3. **Verifier les dependances** : `dbt build --select +mon_modele` (amont complet)
4. **Verifier les warnings dbt** : `dbt parse` ne doit produire aucun `[WARNING]`
5. **Pas de secrets** : ne jamais commiter `.env`, cles GCP, credentials

> `dbt parse` valide la coherence de tous les fichiers YAML sans toucher BigQuery.
> Les warnings apparaissent typiquement lors d'un copier-coller d'un bloc YAML avec
> une syntaxe obsolete, un `ref()` mal ecrit, ou une colonne documentee qui n'existe
> plus dans le modele. La CI les detecte automatiquement et les signale sur la PR —
> mieux vaut les corriger avant.
>
> ```bash
> dbt parse --target dev
> ```

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

## Ajouter un seed

1. Placer le CSV dans `data/reference_data/<source>/` en respectant le nommage `ref_<source>__<entite>.csv`
2. Sauvegarder en **UTF-8 sans BOM** (pas depuis Excel directement — utiliser LibreOffice ou un editeur de texte)
3. Ajouter une entree dans `data/schema.yml` avec :
   - `description` du seed
   - `config: column_types:` pour **toutes les colonnes** (voir types autorises dans CONVENTIONS.md)
   - `columns:` avec description et tests pour chaque colonne
4. **Verifier les types en regardant les donnees reelles**, pas seulement le nom de colonne :
   ```bash
   head -3 data/reference_data/<source>/ref_<source>__<entite>.csv
   ```
5. Tester : `dbt build -s ref_<source>__<entite>`
6. Verifier que les modeles downstream compilent toujours : `dbt build -s ref_<source>__<entite>+`

> Ne pas ajouter de `column_types` dans `dbt_project.yml` — tout se declare dans `data/schema.yml`.

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
- [ ] `dbt parse` sans `[WARNING]` — verifier les fichiers YAML modifies
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
