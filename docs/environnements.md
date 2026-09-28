# Environnements, CI/CD et defer

Référence unique sur **où** dbt écrit, **avec quelle identité**, et **comment** un changement
va de ton poste à la prod. Infra correspondante : `infra/dbt_environments.tf`.

---

## 1. Un projet GCP, un dataset par usage

Tout vit dans `evs-datastack-prod`. L'isolation se fait par **dataset**, et elle est portée
par l'**IAM** : aucune identité hors prod ne peut écrire dans `prod_*`.

| Usage | Dataset | Écrit par | Durée de vie des tables |
|---|---|---|---|
| Lac | `prod_raw`, `historic` | pipelines dlt (repo `ingestion`) | permanente |
| Prod | `prod_staging`, `prod_intermediate`, `prod_marts`, `prod_reference` | Cloud Run (nuit), job `cd` (merge) | permanente |
| Snapshots | `snapshots` | Cloud Workflows uniquement | permanente |
| Dev | `dbt_<toi>` — **toutes les couches dans un seul dataset** | toi, en local | 14 jours sans rebuild |
| CI | `dbt_ci_pr_<N>` | job `pr-check` | supprimé à la fermeture de la PR (filet : 3 jours) |
| Ingestion de test | `dev_raw`, `dev_raw_staging` | `dlt --target dev` | permanente (dlt garde son état dans `_dlt_version`) |

L'expiration porte sur les **tables**, pas sur le dataset. Chaque rebuild remet le compteur à
zéro.

La macro `generate_schema_name` fait le routage :
- en **prod** : `prod_<couche>`, le comportement dbt par défaut ;
- **ailleurs** : tout dans le dataset du target. Les préfixes `stg_` / `int_` / `dim_` / `fct_` / `ref_`
  évitent les collisions de noms.

Hors prod, un dataset `prod*` est refusé dès la compilation.

## 2. Les identités

| Identité | Utilisée par | Authentification | Écrit | Lit |
|---|---|---|---|---|
| `meltano-runner` | Cloud Run (jobs dlt et dbt nocturnes) | clé montée depuis Secret Manager | `prod_raw`, `prod_*`, `snapshots` | tout |
| `dbt-deployer` | job `cd`, sur `master` **uniquement** | WIF | `prod_staging` / `intermediate` / `marts` / `reference`, bucket d'état, Artifact Registry | tout |
| `dbt-ci` | job `pr-check` | WIF | `dbt_ci_pr_<N>` (il les crée et en est owner) | `prod_*` |
| `dbt-dev` | développeurs, en local | clé JSON hors repo (`chmod 600`) | `dbt_<dev>`, `dev_raw*` | `prod_*` |

**WIF** (Workload Identity Federation) : GitHub Actions présente un jeton signé
(« repo `dbt_transform`, branche `master` ») et GCP lui prête une identité pour la durée
du run. **Aucune clé n'est stockée dans GitHub.**

## 3. Le defer

`--defer` fait pointer chaque `ref()` vers un modèle **non construit dans ce run** vers sa
version prod. Tu ne construis que ce que tu modifies ; les parents sont lus en prod, à jour.

dbt sait où se trouve chaque modèle en prod grâce au **`manifest.json` de prod** :

```
merge → cd produit manifest.json → gs://evs-datastack-dbt-state/dbt-state/  (versionné, 10 versions)
                                     ├─► pr-check     (state:modified + defer)
                                     ├─► toi          (scripts/pull-state.sh + defer)
                                     └─► cd suivant   (state:modified)
```

`state:modified+` compare le code au manifest et ne sélectionne que les nœuds modifiés,
plus leur aval.

## 4. Les quatre flux

### Production planifiée
```
Cloud Scheduler → Cloud Workflows → Cloud Run dlt        → prod_raw
                                  → Cloud Run dbt-runner → dbt build source:<X>+ → prod_*
```
Le job `dbt-runner` tourne sur l'image `:latest`, résolue à chaque exécution. Le détail est
dans [`pipeline-schedule.md`](pipeline-schedule.md).

### Développement local
```bash
scripts/pull-state.sh                  # manifest prod → state/ (après chaque merge)
dbt build -s mon_modele                # écrit dbt_<toi>.mon_modele, parents lus en prod
dbt build -s mon_modele+               # + tout l'aval
dbt clone -s mon_incremental           # copie de la table prod (instantanée, gratuite)
dbt run   -s mon_incremental           # → exécute le vrai MERGE incrémental
```

### Pull request — `pr-check`
1. Authentification WIF avec `dbt-ci`, puis création de `dbt_ci_pr_<N>`.
2. `dbt lint` (modèles modifiés), `dbt parse`, puis `dbt compile --static-analysis strict`
   sur le projet entier. Ce dernier contrôle est bloquant.
3. `dbt build --select state:modified+ --defer`, snapshots exclus.
4. À la fermeture de la PR, `cleanup-ci-dataset` supprime `dbt_ci_pr_<N>`.

### Merge sur `master` — `cd`
1. Authentification WIF avec `dbt-deployer`.
2. `dbt build --select state:modified+` **directement en prod**, snapshots exclus.
3. `dbt docs generate`, dépôt du nouveau manifest, publication des docs sur GitHub Pages.
4. Build et push de `dbt-runner:latest`, repris par la production planifiée à sa prochaine
   exécution.

> Tout push sur `master` déclenche `cd`, **y compris un push direct sans PR**.
> Les `cd` passent **un par un**, dans l'ordre des push (verrou `concurrency`). Si
> plusieurs merges s'empilent, GitHub ne garde que le dernier en attente : il se
> compare au dernier manifest déposé, donc il reconstruit aussi les changements
> des runs annulés.
> Dans les logs, `dbt docs generate` affiche une ligne « Succeeded model » par nœud : il lit
> seulement les métadonnées, il ne reconstruit rien.

## 5. Garde-fous

- **IAM** : `dbt-dev` et `dbt-ci` ne peuvent pas écrire dans `prod_*` ni dans `snapshots`.
- **Macro** : un dataset `prod*` hors prod provoque une erreur de compilation.
- **WIF** : seul `master` obtient `dbt-deployer`. Une PR obtient `dbt-ci`.
- **Snapshots** : toujours exclus de la CI et de la CD.
- **Manifest versionné** : on peut revenir à un état antérieur.

## 6. Travailler à plusieurs

Chaque développeur a **son dataset** (`dbt_<nom>`) et lit **la même prod** par le defer : deux
personnes peuvent modifier le même modèle en même temps sans se gêner. Chaque PR a son propre
dataset de CI, et les déploiements passent un par un.

### Arrivée d'un développeur

**Côté data engineer**, environ 15 minutes :

1. **Identité.** Aujourd'hui, un seul SA `dbt-dev` écrit dans tous les datasets de dev. Dès le
   deuxième développeur, passer à **un SA par personne** (`dbt-dev-<nom>`), avec un droit
   d'écriture sur son seul dataset. C'est ce qui rend l'audit lisible et évite qu'une personne
   écrase le dataset d'une autre. Dans `infra/dbt_environments.tf`, `local.dbt_developers` doit
   alors créer le SA **et** le dataset de chaque entrée ; le SA actuel migre par un bloc `moved {}`.
2. **Dataset.** Ajouter le nom à `local.dbt_developers`, puis `terraform plan` et
   `terraform apply`. Cela crée `dbt_<nom>`, dont les tables expirent après 14 jours.
3. **Clé.** Générer la clé de son SA :
   ```bash
   gcloud iam service-accounts keys create /opt/credentials/gcp-dbt-dev-<nom>-key.json \
     --iam-account=dbt-dev-<nom>@evs-datastack-prod.iam.gserviceaccount.com
   ```
   La lui transmettre par un **canal sécurisé**, jamais par mail ou messagerie en clair. Côté
   poste : `chmod 600`, hors de tout repo.
4. **GitHub.** Accès en écriture au repo. La protection de `master` impose déjà la PR.
5. **DSI.** Le prévenir de l'ouverture d'un accès GCP.

**Côté nouveau développeur**, environ 20 minutes : suivre [CONTRIBUTING § 1](../CONTRIBUTING.md#1-installer-son-poste),
avec `DBT_BIGQUERY_DATASET_DEV=dbt_<nom>` et le chemin de sa clé dans `.env`. Vérifier avec
`dbt debug`, puis `scripts/pull-state.sh`.

### Au quotidien

- Tu développes dans ton dataset ; tes collègues dans le leur.
- Après un merge d'un collègue : `git pull`, `git rebase origin/master` sur ta branche, puis
  `scripts/pull-state.sh`. Un conflit git n'apparaît que si vous avez touché le **même fichier**.
- Chaque PR est relue par une autre personne avant le merge.
- Sans rebase, rien ne casse en prod : la CI de ta PR reconstruit aussi les modèles de ton
  collègue, dans leur version antérieure et dans ton dataset de PR. Le résultat est juste moins
  lisible.

### Départ d'un développeur

1. Supprimer sa clé, puis son SA.
2. Retirer son nom de `local.dbt_developers`, puis `terraform apply`. Terraform refuse de
   supprimer un dataset qui contient des tables : le vider avant.
3. Retirer son accès GitHub.

## 7. Où c'est défini

| Sujet | Fichier |
|---|---|
| Identités, datasets, bucket d'état | `infra/dbt_environments.tf` |
| Ouverture WIF au repo dbt | `infra/github_actions_wif.tf` |
| Targets `dev` / `ci` / `prod` | `profiles.yml` |
| Routage des datasets | `macros/generate_schema_name.sql` |
| CI/CD | `.github/workflows/dbt-ci.yml` |
| Variables locales | `.env.example` |
| Liste des développeurs | `infra/dbt_environments.tf` (`local.dbt_developers`) |
| Récupération du manifest | `scripts/pull-state.sh` |
