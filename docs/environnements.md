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
> Dans les logs, `dbt docs generate` affiche une ligne « Succeeded model » par nœud : il lit
> seulement les métadonnées, il ne reconstruit rien.

## 5. Garde-fous

- **IAM** : `dbt-dev` et `dbt-ci` ne peuvent pas écrire dans `prod_*` ni dans `snapshots`.
- **Macro** : un dataset `prod*` hors prod provoque une erreur de compilation.
- **WIF** : seul `master` obtient `dbt-deployer`. Une PR obtient `dbt-ci`.
- **Snapshots** : toujours exclus de la CI et de la CD.
- **Manifest versionné** : on peut revenir à un état antérieur.

## 6. Ajouter un développeur

1. Ajouter son nom à `local.dbt_developers` dans `infra/dbt_environments.tf`, puis
   `terraform plan` et `terraform apply`. Cela crée `dbt_<nom>`.
2. Générer sa clé :
   ```bash
   gcloud iam service-accounts keys create /opt/credentials/gcp-dbt-dev-prod-key.json \
     --iam-account=dbt-dev@evs-datastack-prod.iam.gserviceaccount.com
   chmod 600 /opt/credentials/gcp-dbt-dev-prod-key.json
   ```
3. Dans son `.env` (modèle : `.env.example`), renseigner
   `DBT_BIGQUERY_DATASET_DEV=dbt_<nom>`.

## 7. Où c'est défini

| Sujet | Fichier |
|---|---|
| Identités, datasets, bucket d'état | `infra/dbt_environments.tf` |
| Ouverture WIF au repo dbt | `infra/github_actions_wif.tf` |
| Targets `dev` / `ci` / `prod` | `profiles.yml` |
| Routage des datasets | `macros/generate_schema_name.sql` |
| CI/CD | `.github/workflows/dbt-ci.yml` |
| Variables locales | `.env.example` |
| Récupération du manifest | `scripts/pull-state.sh` |
