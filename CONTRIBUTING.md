# Contribuer au projet dbt EVS

Guide pratique pour développer, tester et livrer un modèle. Les règles de nommage et de
qualité sont dans [CONVENTIONS.md](CONVENTIONS.md). L'architecture dev / CI / prod est
décrite dans [docs/environnements.md](docs/environnements.md).

---

## 1. Installer son poste

```bash
git clone https://github.com/CebrailEVS/dbt_transform.git && cd dbt_transform
python3 -m venv dbt_venv && source dbt_venv/bin/activate
pip install -r requirements-lock.txt

cp .env.example .env          # renseigner DBT_BIGQUERY_DATASET_DEV et la clé (voir ci-dessous)
direnv allow                  # charge .env à chaque cd ; sans direnv : set -a && source .env && set +a

dbt deps && dbt debug
scripts/pull-state.sh         # manifest prod pour le defer

pipx install pre-commit && pre-commit install && pre-commit install --hook-type pre-push
```

- **Dataset et clé** : le data engineer crée ton dataset `dbt_<toi>` et te remet la clé
  `dbt-dev` (procédure : [docs/environnements.md § 6](docs/environnements.md#6-ajouter-un-développeur)).
  La clé reste hors du repo.
- **pre-commit** : `dbt lint` s'exécute au commit et `dbt parse` au push. Ce sont les mêmes
  contrôles que la CI, rejoués en local.
- **Dépendances** : toujours installer depuis `requirements-lock.txt`. Seul le data engineer
  modifie `requirements*.txt`. Après un `git pull` qui les modifie :
  `pip install -r requirements-lock.txt`.

---

## 2. Développer un modèle

Tu écris dans **ton** dataset `dbt_<toi>`. Tout ce que tu ne construis pas est lu en prod
(**defer**), donc tu ne reconstruis jamais la chaîne amont.

```bash
scripts/pull-state.sh           # après chaque merge sur master
dbt build -s mon_modele         # ton modèle et ses tests
dbt build -s mon_modele+        # + tout l'aval, pour vérifier que rien ne casse
```

**Modèle incrémental** : clone d'abord la table prod, puis lance le build. C'est alors le
vrai `MERGE` qui s'exécute, pas une reconstruction complète.

```bash
dbt clone -s stg_oracle_lcdp__task
dbt run   -s stg_oracle_lcdp__task
```

**Comparer avec la prod** avant de livrer :

```sql
select 'dev' as env, count(*) from `evs-datastack-prod.dbt_<toi>.mon_modele`
union all
select 'prod', count(*) from `evs-datastack-prod.prod_marts.mon_modele`
```

| Tu veux… | Regarde |
|---|---|
| analyser, explorer, faire un rapport | `prod_marts` |
| tester ce que tu développes | `dbt_<toi>` |
| vérifier ta PR | `dbt_ci_pr_<N>`, construit par la CI |

Les tables de `dbt_<toi>` expirent après 14 jours sans rebuild : pas de ménage à faire.

---

## 3. Git et pull request

### Branches et commits

| Branche | Usage |
|---|---|
| `master` | Production. Tout push déclenche un déploiement. |
| `feature/<scope>/<description>` | Nouveau modèle ou fonctionnalité |
| `fix/<scope>/<description>` | Correction |

Format de commit : `type(scope): description`, avec `feat`, `fix`, `refactor`, `test`, `docs`
ou `chore`. Exemple : `feat(neshu): add fct_neshu__passage_appro`.

### Cycle

```bash
git checkout master && git pull
git checkout -b feature/neshu/kpi-livraison
# ... développer, dbt build -s ..., dbt lint ...
git commit -m "feat(neshu): add kpi livraison"
git push -u origin feature/neshu/kpi-livraison     # puis ouvrir la PR sur GitHub
```

Si `master` a avancé : `git rebase origin/master` puis `git push --force-with-lease`.

### Ce que fait la CI sur ta PR

Elle lance le lint des modèles modifiés, `dbt parse`, puis l'analyse statique stricte. Elle
construit ensuite les modèles modifiés et leur aval dans `dbt_ci_pr_<N>`. Tu peux ouvrir ce
dataset dans BigQuery pour contrôler le résultat. Il est supprimé à la fermeture de la PR.

Au merge, seuls les modèles modifiés et leur aval sont reconstruits en prod.

### Après le merge

```bash
git checkout master && git pull && git branch -d feature/neshu/kpi-livraison
scripts/pull-state.sh
```

---

## 4. Ajouter…

### Un modèle de staging (data engineer)
1. `models/staging/<source>/stg_<source>__<table>.sql`, avec `description` dans le `config()`.
2. Entrée dans `_<source>__models.yml` : description et tests.
3. Source déclarée dans `_<source>__sources.yml`.

Règles complètes : [docs/conventions/staging.md](docs/conventions/staging.md).

### Un modèle intermediate
1. `models/intermediate/<source>/int_<source>__<description>.sql`, uniquement des `ref()`.
2. Documentation dans le YAML de la source.

Il reste **aligné sur une source** : le croisement de sources se fait dans les marts.
Règles : [docs/conventions/intermediate.md](docs/conventions/intermediate.md).

### Un mart
1. `models/marts/<bu>/dim_<bu>__<entite>.sql` ou `fct_<bu>__<entite>.sql`.
2. Entrée dans `_<bu>__marts_models.yml`, avec une description en 4 blocs
   (`[QUOI MÉTIER]`, `[COMMENT CONSTRUITE]`, `[GRAIN]`, `[NOTES]`) et les tests minimum.
3. Schéma en étoile strict : un fait référence des dimensions, jamais un autre fait.

Règles complètes et exemples : [docs/conventions/marts.md](docs/conventions/marts.md).

### Une exposure (rapport Power BI)
Dès qu'un rapport consomme un mart, le déclarer dans `models/exposures/<bu>.yml` :

```yaml
- name: nom_du_rapport            # dbt build -s +exposure:nom_du_rapport
  label: "Nom lisible"
  type: dashboard
  maturity: high
  owner:
    name: Prénom Nom
    email: prenom.nom@evs-pro.com
  depends_on:
    - ref('fct_<bu>__<entite>')
```

### Un seed
1. CSV dans `data/reference_data/<source>/ref_<source>__<entite>.csv`, en **UTF-8 sans BOM**.
   Ne pas l'enregistrer depuis Excel.
2. Documentation et `column_types` (toutes les colonnes) dans `_<source>__seeds.yml`, dans le
   même dossier.
3. `dbt build -s ref_<source>__<entite>+`

Types et pièges : [docs/conventions/seeds-snapshots.md](docs/conventions/seeds-snapshots.md).

### Une source
1. `models/staging/<source>/` avec `_<source>__sources.yml` (freshness) et `_<source>__models.yml`.
2. Tags dans `dbt_project.yml`.
3. Ligne dans le tableau des sources du [README](README.md).

---

## 5. Checklist avant merge

- [ ] `dbt build -s mon_modele+` passe (PASS ou WARN, aucune ERROR)
- [ ] `dbt lint` propre sur les fichiers modifiés
- [ ] `dbt parse` sans `[WARNING]`
- [ ] Modèles documentés en YAML (marts : 4 blocs + grain)
- [ ] Exposure à jour si un rapport Power BI consomme le modèle
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

Tout changement de code passe par une PR. Seule la documentation pure peut être poussée
directement sur `master`, par le data engineer.

---

## Aide

- `dbt debug` pour vérifier la configuration
- `dbt ls -s +mon_modele` / `dbt ls -s mon_modele+` pour voir les dépendances amont et aval
- [Documentation générée](https://cebrailevs.github.io/dbt_transform/) : modèles, lignage, tests
