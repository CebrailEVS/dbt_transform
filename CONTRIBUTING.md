# Contribuer au projet dbt EVS

Guide pratique pour développer, tester et livrer un modèle. Règles de modélisation :
[CONVENTIONS.md](CONVENTIONS.md). Dev, CI et prod : [docs/environnements.md](docs/environnements.md).

---

## 1. Installer son poste

Python 3.11 ou plus récent : dbt v2 est un moteur Rust distribué comme extension CPython.

```bash
git clone https://github.com/CebrailEVS/dbt_transform.git && cd dbt_transform
python3 -m venv dbt_venv && source dbt_venv/bin/activate
pip install -r requirements-lock.txt

cp .env.example .env          # renseigner son dataset de dev et le chemin de sa clé
direnv allow                  # charge .env à chaque cd ; sans direnv : set -a && source .env && set +a

dbt deps && dbt debug

pipx install pre-commit && pre-commit install   # hooks git
scripts/pull-state.sh         # premier manifest prod ; ensuite, les hooks le tiennent à jour
```

- **Dataset et clé** : le data engineer crée ton dataset `dbt_<toi>` et te remet ta clé
  ([docs/environnements.md § 6](docs/environnements.md#6-travailler-à-plusieurs)). La clé reste
  hors du repo.
- **Hooks git** (`.pre-commit-config.yaml`, identiques pour toute l'équipe) : `dbt lint` au commit
  et `dbt parse` au push, qui refusent l'opération en cas d'échec ; mise à jour du manifest prod au
  `git pull` et au changement de branche, qui n'avertit qu'en cas d'échec.
- **Dépendances** : toujours installer depuis `requirements-lock.txt`. Seul le data engineer
  modifie `requirements*.txt` ; après un `git pull` qui les change, relancer le `pip install`.

---

## 2. Développer un modèle

Tu écris dans **ton** dataset `dbt_<toi>`. Tout ce que tu ne construis pas est lu en prod
(**defer**) : tu ne reconstruis jamais la chaîne amont.

```bash
dbt build -s mon_modele         # ton modèle et ses tests
dbt build -s mon_modele+        # + tout l'aval, pour vérifier que rien ne casse
```

- **Incrémental** : `dbt clone -s mon_incremental`, puis `dbt run -s mon_incremental`. C'est alors
  le vrai `MERGE` qui s'exécute, pas une reconstruction complète.
- **Recette contre la prod** : `--favor-state`, et le même nombre de lignes en dev et en prod avant
  d'interpréter un écart ([docs/environnements.md § 3](docs/environnements.md#3-le-defer)).
- **Analyser ou explorer** : `prod_marts`, jamais ton dataset de dev.

---

## 3. Git et pull request

### Branches et commits

| Branche | Usage |
|---|---|
| `master` | Production. Tout push qui touche le code déclenche un déploiement. |
| `feature/<description>` | Nouveau modèle ou fonctionnalité |
| `fix/<description>` | Correction |

Format de commit : `type(scope): description`, avec `feat`, `fix`, `refactor`, `test`, `docs`
ou `chore`. Exemple : `feat(neshu): add fct_neshu__passage_appro`.

### Cycle

```bash
git fetch origin master
git checkout -b feature/kpi-livraison-neshu origin/master   # toujours depuis master à jour
dbt run-operation vider_dataset_dev                              # aucun objet récent d'un autre chantier ?
dbt run-operation vider_dataset_dev --args '{confirmer: true}'   # repartir d'un dataset vide
# ... développer, dbt build -s ..., dbt lint ...
git commit -m "feat(neshu): add kpi livraison"
git push -u origin feature/kpi-livraison-neshu      # puis ouvrir la PR sur GitHub
```

Un seul chantier construit en dev à la fois. Vider le dataset au début de chaque branche évite
qu'une table d'un ancien chantier soit lue à la place de la prod
([docs/environnements.md § 1](docs/environnements.md#un-chantier-à-la-fois-dataset-vidé-en-début-de-branche)).
Si `master` a avancé : `git rebase origin/master` puis `git push --force-with-lease`.

### Sur la PR

La CI lint les modèles modifiés et compile tout le projet en analyse statique stricte, puis construit les modèles modifiés et leur aval dans
`dbt_ci_pr_<N>`, que tu peux ouvrir dans BigQuery. Au merge, le même périmètre est reconstruit en
prod. Détail : [docs/environnements.md § 4](docs/environnements.md#4-les-quatre-flux).

Après le merge d'un collègue : `git pull` sur `master` (le manifest suit), puis
`git rebase origin/master` sur ta branche.

### Après le merge

```bash
git checkout master && git pull && git branch -d feature/kpi-livraison-neshu
```

---

## 4. Ajouter…

Chaque page de couche porte les règles complètes ; ici, seulement les étapes.

### Un modèle de staging (data engineer)
1. `models/staging/<source>/stg_<source>__<table>.sql`, avec `description` dans le `config()`.
2. Entrée dans `_<source>__models.yml` : description et tests.
3. Table déclarée dans `_<source>__sources.yml`.

Règles : [staging.md](docs/conventions/staging.md).

### Un modèle intermediate
1. `models/intermediate/<source>/int_<source>__<entite>.sql`, uniquement des `ref()` d'une source.
2. Entrée dans `_<source>__intermediate_models.yml`.

Règles : [intermediate.md](docs/conventions/intermediate.md).

### Un mart
1. `models/marts/<bu>/dim_<bu>__<entite>.sql` ou `fct_<bu>__<entite>.sql`.
2. Entrée dans `_<bu>__marts_models.yml`, description en 4 blocs et tests minimum.
3. Revue par l'agent `mart-reviewer` avant de le déclarer terminé.

Règles : [marts.md](docs/conventions/marts.md).

### Un modèle pour une application
Uniquement pour une application interne qui lit ses données telles quelles, pas pour Power BI.
Règles et prérequis d'infra : [apps.md](docs/conventions/apps.md).

### Une exposure
Dès qu'un rapport Power BI ou une application lit un modèle, le déclarer dans
`models/exposures/<bu>.yml` :

```yaml
- name: nom_du_rapport            # dbt build -s +exposure:nom_du_rapport
  label: "Nom lisible"
  type: dashboard
  maturity: high
  owner:
    name: Data analyst            # un rôle, jamais le nom ni l'e-mail d'une personne
  depends_on:
    - ref('fct_<bu>__<entite>')
```

### Un seed
1. CSV dans `data/reference_data/<source>/ref_<source>__<entite>.csv`, en **UTF-8 sans BOM** :
   ne pas l'enregistrer depuis Excel.
2. Documentation et `column_types` dans `_<source>__seeds.yml`, dans le même dossier.
3. `dbt build -s ref_<source>__<entite>+`

Règles : [seeds-snapshots.md](docs/conventions/seeds-snapshots.md).

### Une source
1. `models/staging/<source>/` avec `_<source>__sources.yml` (fraîcheur comprise) et `_<source>__models.yml`.
2. Tags dans `dbt_project.yml`.
3. Une ligne dans le tableau des sources du [README](README.md#sources-de-données), et une page
   dans `docs/architecture/` si la source a des pièges.

---

## 5. Checklist avant merge

- [ ] `dbt build -s mon_modele+` passe (PASS ou WARN, aucune ERROR)
- [ ] `dbt lint` propre sur les fichiers modifiés
- [ ] `dbt parse` sans `[WARNING]`
- [ ] Modèles documentés en YAML (marts : 4 blocs + grain)
- [ ] Exposure à jour si un rapport ou une application lit le modèle
- [ ] Doc à jour si une règle change (page de la couche, `CONVENTIONS.md` ou `docs/decisions.md`)
- [ ] Aucun secret committé (`.env`, clés)
- [ ] PR décrite : quoi, pourquoi, comment vérifier

---

## 6. Rôles

| Action | Data engineer | Data analyst |
|---|:---:|:---:|
| `staging/`, snapshots, `dbt_project.yml` | Oui | Non |
| `intermediate/` | Oui | Sur validation |
| `marts/`, seeds, exposures | Oui | Oui |
| Ouvrir une PR | Oui | Oui |
| Relire et merger | Oui | — |

Tout changement de code passe par une PR. Les PR du data analyst sont relues et mergées par le data
engineer. Celles du data engineer n'ont pas de second relecteur humain : elles passent par
`/code-review` et, pour un mart, par l'agent `mart-reviewer`. Seule la documentation pure peut être
poussée directement sur `master`, par le data engineer.

---

## Aide

- `dbt ls -s +mon_modele` / `dbt ls -s mon_modele+` : dépendances amont et aval
- [Documentation générée](https://cebrailevs.github.io/dbt_transform/) : modèles, lignage, tests
- Dépannage courant : [README](README.md#dépannage)
