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

### Rester a jour avec le remote

VS Code fetche automatiquement le remote toutes les 3 minutes (`git.autofetch: true` dans `.vscode/settings.json`).
Surveille l'indicateur en bas a gauche de la fenetre, a cote du nom de branche :

- Pas de chiffre → tu es a jour
- `↓2` → 2 commits a recuperer : lance `git pull`
- `↑1` → 1 commit local a pousser

Clique sur l'icone pour synchroniser directement depuis VS Code.

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

#### Modele mono-source (cas standard)

1. Creer dans `models/marts/<source>/`
2. Nommer : `dim_<source>__<entite>.sql` ou `fct_<source>__<metrique>.sql`
3. C'est la couche exposee a Power BI — veiller a la clarte des colonnes

#### Modele cross-source (plusieurs sources de donnees)

Un modele est cross-source s'il consomme des `ref()` provenant de sources differentes
(ex : oracle_neshu + yuman, nesp_co + nesp_tech).

1. Creer dans `models/marts/technique/` (ou le dossier BU correspondant, pas dans un dossier source)
2. Nommer avec le prefixe BU : `fct_technique__<metrique>.sql`
3. Ajouter la section **Sources** dans la description YAML du modele :
   ```yaml
   description: |
     ...
     **Sources :** nesp_tech (interventions) · yuman (workorders NESHU)
   ```
4. Pour les tests `relationships` sur des FK nullable (LEFT JOIN), ajouter un filtre :
   ```yaml
   - relationships:
       arguments:
         to: ref('stg_yuman__materials')
         field: material_id
       config:
         where: "material_id is not null"
   ```
5. Ne pas creer d'intermediate dedie si le modele n'a qu'un seul consommateur — absorber la logique directement dans le mart

> **Scheduling :** les modeles cross-source ne sont pas construits par le pipeline de leur source.
> Ils sont construits par le workflow `transform-technique-daily` (03:00 quotidien).
> Voir `docs/scheduling_and_tagging_decisions.md` pour le detail.

### Exposures (rapport Power BI)

Les exposures declarent quels rapports Power BI consomment quels modeles dbt. Cela permet de :

- **Visualiser la lignee complete** dans `dbt docs` — de la source raw jusqu'au rapport BI
- **Mesurer l'impact d'un changement** : si on modifie `fct_oracle_neshu__appro`, on voit immediatement quels rapports sont affectes
- **Cibler un rebuild** : `dbt build -s +exposure:reporting_appro` reconstruit uniquement les modeles qui alimentent ce rapport
- **Documenter qui est responsable** de chaque rapport (owner) et son niveau de maturite

Quand un rapport Power BI est cree ou modifie, mettre a jour le fichier exposure correspondant dans `models/exposures/`.

- Un fichier par BU/partenaire : `neshu.yml`, `lcdp.yml`, etc.
- Si le rapport est pour une nouvelle BU, creer un nouveau fichier dans `models/exposures/`
- Chaque exposure doit lister tous les modeles dbt (`ref()`) et sources externes (`source()`) consommes par le rapport

```yaml
- name: nom_du_rapport          # slug CLI : dbt build -s +exposure:nom_du_rapport
  label: "Nom Lisible"          # affiche dans dbt docs
  type: dashboard
  maturity: high | medium | low
  description: >
    Description metier du rapport.
  owner:
    name: Prenom Nom
    email: prenom.nom@evs-pro.com
  depends_on:
    - ref('fct_source__modele')
    - ref('dim_source__dimension')
```

> Si le rapport consomme une table alimentee par un pipeline externe (Cloud Run / Cloud Workflows),
> utiliser `source('<source_externe>', '<table>')` et verifier que la source est declaree
> dans le fichier `_<source>__marts_sources.yml` correspondant.

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
- [ ] Si le modele est consomme par un rapport Power BI : exposure mise a jour dans `models/exposures/`
- [ ] Pas de secrets dans le commit
- [ ] PR avec description claire

---

## Environnement de developpement

### Les deux environnements

Le projet dispose de deux environnements BigQuery dans le **meme projet GCP** :

| Environnement | Schemas | Qui ecrit dedans | Frequence de mise a jour |
|---------------|---------|------------------|--------------------------|
| `prod` | `prod_staging`, `prod_intermediate`, `prod_marts` | Cloud Workflows (automatique) | Chaque nuit en production |
| `dev` | `dev_staging`, `dev_intermediate`, `dev_marts` | Toi (en local) + CI sur les PRs | Uniquement quand tu lances un build |

> Les deux environnements lisent depuis la **meme source** : `prod_raw`. Seuls les schemas de destination changent.

---

### Pourquoi les tables `dev_marts` peuvent sembler "vieilles" ?

C'est normal, et c'est voulu.

`prod_marts` est reconstruit chaque nuit automatiquement par Cloud Workflows.
`dev_marts`, lui, n'est reconstruit que dans deux cas :
- quand la CI tourne sur une **Pull Request** (uniquement les modeles modifies)
- quand **tu lances toi-meme** un `dbt build` en local

Si personne n'a ouvert de PR ni lance de build depuis plusieurs jours, les tables `dev_*` contiennent les donnees telles qu'elles etaient au dernier build. Elles ne se rafraichissent pas automatiquement.

---

### Quelle table utiliser selon ce que tu fais ?

| Ce que tu veux faire | Ou regarder |
|----------------------|-------------|
| Faire une analyse, un rapport, explorer les donnees | `prod_marts` — toujours frais, reconstruit chaque nuit |
| Tester un modele que tu es en train de developper | `dev_marts` — mais tu dois d'abord lancer un build (voir ci-dessous) |
| Verifier que ta PR ne casse rien | La CI s'en charge automatiquement quand tu ouvres la PR |

> **Regle simple :** si tu ne developpes pas un modele dbt, tu n'as pas besoin de `dev_*`. Utilise `prod_marts`.

---

### Comment rafraichir `dev_marts` quand tu developpes ?

Tu n'as **pas besoin** de tout reconstruire. Lance uniquement le modele sur lequel tu travailles et ses dependances amont :

```bash
# Construire ton modele et tout ce dont il depend (staging + intermediate + mart)
dbt build -s +nom_de_ton_modele --target dev

# Exemple : tu travailles sur fct_yuman__interventions
dbt build -s +fct_yuman__interventions --target dev
```

Le `+` devant le nom du modele signifie "et tous ses parents". Sans ca, dbt ne reconstruirait que le modele final, sans s'assurer que les tables en amont sont a jour dans ton `dev_*`.

Apres ca, ta table `dev_marts.fct_yuman__interventions` est fraiche et reflete exactement ton code local.

---

### Configurer son environnement local

Verifier que `DBT_TARGET=dev` est bien defini dans ton `.env` avant de lancer quoi que ce soit :

```bash
# Dans .env
DBT_TARGET=dev
```

Cela garantit que tous tes builds locaux ecrivent dans `dev_*` et jamais dans `prod_*`.

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
- Documentation en ligne (modeles, lineage, tests) : [https://cebrailevs.github.io/dbt_transform/](https://cebrailevs.github.io/dbt_transform/)
- Consulter [CONVENTIONS.md](CONVENTIONS.md) pour les regles de nommage et qualite
