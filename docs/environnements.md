# Environnements, CI/CD et defer

Référence unique sur **où** dbt écrit, **avec quelle identité**, et **comment** un changement
va de ton poste à la prod. Infra correspondante : `infra/dbt_environments.tf`.

---

## 1. Un projet GCP, un dataset par usage

Tout vit dans `evs-datastack-prod`. L'isolation se fait par **dataset**, et elle est portée
par l'**IAM** : les identités dbt hors prod (`dbt-dev`, `dbt-ci`) ne peuvent pas écrire dans `prod_*`.

| Usage | Dataset | Écrit par | Durée de vie des tables |
|---|---|---|---|
| Lac | `prod_raw`, `historic` | pipelines dlt (repo `ingestion`) | permanente |
| Prod | `prod_staging`, `prod_intermediate`, `prod_marts`, `prod_reference` | runs planifiés (Cloud Run), job `cd` (merge) | permanente |
| Snapshots | `snapshots` | Cloud Workflows uniquement | permanente |
| Applications | `prod_app_<application>` | runs planifiés (Cloud Run), job `cd` (merge) ; lu par le seul compte de l'application | permanente |
| Dev | `dbt_<toi>` — **toutes les couches dans un seul dataset** | toi, en local | vidé au début de chaque branche ; filet : expiration des tables |
| CI | `dbt_ci_pr_<N>` | job `pr-check` | supprimé à la fermeture de la PR ; filet : expiration des tables |
| Ingestion de test | `dev_raw`, `dev_raw_staging` | `dlt --target dev` | permanente (dlt garde son état dans `_dlt_version`) |

L'expiration porte sur les **tables**, pas sur le dataset, et court depuis leur création (délais :
`infra/dbt_environments.tf` pour le dev, `dbt-ci.yml` pour la CI). Un modèle `table` est recréé à
chaque build, donc son expiration est repoussée ; un incrémental, non : le `MERGE` ne recrée pas la
table, qui expire au terme du délai compté depuis son clone ou son full-refresh.

### Vider son dataset au début de chaque branche

```bash
dbt run-operation vider_dataset_dev                             # liste ce qui serait supprimé
dbt run-operation vider_dataset_dev --args '{confirmer: true}'  # vide ton dataset
```

Une table restée d'un ancien chantier est lue par le defer à la place de la prod (§ 3), et une
table de quelques jours suffit à fausser une recette. L'expiration n'est qu'un filet.

La macro lit le dataset du target `dev` ; elle refuse tout autre target et tout dataset hors
`dbt_<nom>`. Elle supprime les tables et les vues, jamais le dataset, qui appartient à Terraform.
En reprenant une autre branche en cours, reconstruire ses modèles : leur version dev est partie
avec le reste. Ne pas vider pendant qu'un autre chantier travaille dans le même dataset (deux
sessions en parallèle) : ses tables partiraient en cours de route.

La macro `generate_schema_name` fait le routage :
- en **prod** : `prod_<couche>`, le comportement dbt par défaut ;
- **ailleurs** : tout dans le dataset du target. Les préfixes `stg_` / `int_` / `dim_` / `fct_` / `ref_`
  évitent les collisions de noms.

Hors prod, un dataset `prod*` est refusé dès la compilation.

## 2. Les identités

| Identité | Utilisée par | Authentification | Écrit | Lit |
|---|---|---|---|---|
| `meltano-runner` (nom historique) | Cloud Run (jobs dlt et dbt planifiés) | clé montée depuis Secret Manager | `prod_raw`, `prod_*`, `snapshots` | tout |
| `dbt-deployer` | job `cd`, sur `master` **uniquement** | WIF | les datasets `prod_*` construits par dbt, dont `prod_app_*` ; bucket d'état, Artifact Registry | tout |
| `dbt-ci` | job `pr-check` | WIF | `dbt_ci_pr_<N>` (il les crée et en est owner) | `prod_*` |
| `dbt-dev` | développeurs, en local | clé JSON hors repo (`chmod 600`) | `dbt_<dev>`, `dev_raw*` | `prod_*` |

**WIF** (Workload Identity Federation) : GitHub Actions présente un jeton signé
(« repo `dbt_transform`, branche `master` ») et GCP lui prête une identité pour la durée
du run. **Aucune clé n'est stockée dans GitHub.**

## 3. Le defer

`--defer` fait pointer chaque `ref()` vers un modèle **non construit dans ce run** vers sa
version prod, **sauf si ce modèle existe déjà dans ton dataset dev** : dbt prend alors ta
version dev, même ancienne. Tu ne construis que ce que tu modifies ; les parents absents de
ton dataset sont lus en prod, à jour.

### `--favor-state` : forcer les parents en prod

`--favor-state` supprime cette exception : tout modèle non construit dans le run est lu en
prod, même s'il existe en dev.

| Situation | Option |
|---|---|
| Tu développes une chaîne en plusieurs commandes (`stg_x`, puis `int_x` qui doit lire **ton** `stg_x`) | `--defer` seul (défaut) |
| **Recette dev ↔ prod** : tu modifies un modèle et compares ses chiffres à la prod | `--favor-state` |
| Ton dataset contient des tables d'un autre chantier, sans rapport | `--favor-state` |

Ne pas l'activer par défaut : dans le premier cas, il ferait lire le `stg_x` de prod et
masquerait ta modification, sans erreur.

**Piège** : une table restée dans ton dataset depuis quelques jours fausse une recette sans aucun
message. Le vidage en début de branche (§ 1) évite le cas
le plus courant. Réflexe avant d'interpréter un écart :
**vérifier que dev et prod ont le même nombre de lignes**.

Construire toute la chaîne modifiée en **une seule commande** (`-s stg_x+`) évite aussi le
problème : ce qui est construit dans le run est toujours lu en dev.

dbt sait où se trouve chaque modèle en prod grâce au **`manifest.json` de prod** :

```
merge → cd produit manifest.json → gs://evs-datastack-dbt-state/dbt-state/  (versionné)
                                     ├─► pr-check     (state:modified + defer)
                                     ├─► toi          (hook git à chaque git pull + defer)
                                     └─► cd suivant   (state:modified)
```

`state:modified+` compare le code au manifest et ne sélectionne que les nœuds modifiés,
plus leur aval.

**Mise à jour du manifest local** : automatique. Les hooks git `post-merge` (`git pull`) et
`post-checkout` (changement de branche) lancent `scripts/pull-state.sh`, qui ne télécharge que si
la prod a changé. Seul cas manuel : un collègue a mergé et tu es resté sur ta branche
sans pull ni checkout.

```bash
scripts/pull-state.sh          # met à jour si besoin
scripts/pull-state.sh --force  # retélécharge quoi qu'il arrive
```

## 4. Les quatre flux

### Production planifiée
```
Cloud Scheduler → Cloud Workflows → Cloud Run dlt        → prod_raw
                                  → Cloud Run dbt-runner → dbt build source:<X>+ → prod_*
```
Le job `dbt-runner` tourne sur l'image `:latest`, résolue à chaque exécution. Les paquets dbt y
sont installés à la construction (`package-lock.yml`) : une exécution n'a besoin d'aucun accès à
hub.getdbt.com ni à GitHub. Cadence et sélection : [`pipeline-schedule.md`](pipeline-schedule.md).

**Trace et alerte.** À la sortie de chaque exécution, même en échec, `publish_run.py` dépose une
enveloppe (contexte, codes retour, `run_results.json`, `sources.json`) que lit la source
`dbt_artifacts`, base des marts `plateforme`. Chaque échec, de build ou de fraîcheur au-delà
d'`error_after`, produit aussi une ligne JSON `severity: ERROR`, que l'alerte de Cloud Monitoring
(`infra/monitoring.tf`) envoie par mail : Cloud Logging ne tire une sévérité que du JSON, jamais
du texte brut, même écrit sur stderr.

### Développement local
```bash
git pull                               # le hook git met aussi state/manifest.json à jour
dbt build -s mon_modele                # écrit dbt_<toi>.mon_modele, parents lus en prod
dbt build -s mon_modele+               # + tout l'aval
dbt clone -s mon_incremental           # copie de la table prod (instantanée, gratuite)
dbt run   -s mon_incremental           # → exécute le vrai MERGE incrémental
```

### Pull request — `pr-check`
1. Authentification WIF avec `dbt-ci`, puis création de `dbt_ci_pr_<N>`.
2. `dbt lint` (modèles modifiés), `dbt parse`, puis `dbt compile --static-analysis strict`
   sur le projet entier. Ce dernier contrôle est bloquant.
3. Clone depuis la prod des incrémentaux modifiés (`dbt clone --full-refresh`) : le build exécute
   leur vrai `MERGE`, comme le fera la prod au merge. En échec, un avertissement s'affiche sur la PR
   et ils sont reconstruits entièrement.
4. `dbt build --select state:modified+ --defer --favor-state`, snapshots exclus : tout ce qui n'est
   pas construit dans le run est lu en prod, même s'il reste dans le dataset d'un push précédent.
5. Un run à la fois par PR : un nouveau push annule le run en cours. À la fermeture de la PR,
   `cleanup-ci-dataset` annule un run encore en cours, puis supprime `dbt_ci_pr_<N>`.

### Merge sur `master` — `cd`
1. Authentification WIF avec `dbt-deployer`.
2. `dbt build --select state:modified+` **directement en prod**, snapshots exclus.
3. `dbt docs generate` et dépôt du nouveau manifest ; le job `deploy-docs` publie ensuite la doc
   sur GitHub Pages.
4. Build et push de `dbt-runner:latest`, repris par la production planifiée à sa prochaine
   exécution. La même image est aussi poussée sous `dbt-runner:<sha du commit>`.

> Tout push sur `master` déclenche `cd`, **y compris un push direct sans PR**.
> Les `cd` passent **un par un**, dans l'ordre des push (verrou `concurrency`). Si
> plusieurs merges s'empilent, GitHub ne garde que le dernier en attente : il se
> compare au dernier manifest déposé, donc il reconstruit aussi les changements
> des runs annulés.

**Revenir en arrière.** Le chemin normal est un `git revert` du commit fautif, en PR : le `cd`
reconstruit et redéploie. En urgence, avant la prochaine exécution planifiée, re-pointer `latest`
sur la version précédente :
```bash
gcloud artifacts docker tags add \
  europe-west1-docker.pkg.dev/evs-datastack-prod/data-pipelines/dbt-runner:<sha> \
  europe-west1-docker.pkg.dev/evs-datastack-prod/data-pipelines/dbt-runner:latest
```
Cela ne remet ni `prod_*` ni le manifest dans leur état antérieur, et le prochain `cd` repousse
`latest` : c'est un sursis, le `git revert` reste nécessaire.

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

**Côté data engineer** :

1. **Identité.** Un compte de service par développeur (`dbt-dev-<nom>`), en écriture sur son seul
   dataset : l'audit reste lisible, et personne n'écrase le dataset d'un autre. Si `dbt-dev` est
   encore partagé, le scinder d'abord.
2. **Dataset.** Ajouter le nom à `local.dbt_developers` (`infra/dbt_environments.tf`), puis
   `terraform plan` et `terraform apply`, qui créent `dbt_<nom>`.
3. **Clé.** Générer une clé pour son compte de service (`gcloud iam service-accounts keys create`)
   et la lui transmettre par un **canal sécurisé**, jamais par mail ou messagerie en clair. Côté
   poste : `chmod 600`, hors de tout repo.
4. **GitHub.** Accès en écriture au repo. La protection de `master` impose déjà la PR.
5. **DSI.** Le prévenir de l'ouverture d'un accès GCP.

**Côté nouveau développeur** : suivre [CONTRIBUTING § 1](../CONTRIBUTING.md#1-installer-son-poste),
avec `DBT_BIGQUERY_DATASET_DEV=dbt_<nom>` et le chemin de sa clé dans `.env`. Vérifier avec
`dbt debug`, `pre-commit install`, puis `scripts/pull-state.sh` pour le premier manifest.

### Au quotidien

- Tu développes dans ton dataset ; tes collègues dans le leur. Le vidage en début de branche
  (§ 1) ne touche que le tien.
- Après un merge d'un collègue : `git pull` (le manifest suit), puis `git rebase origin/master`
  sur ta branche. Un conflit git n'apparaît que si vous avez touché le **même fichier**.
- Relecture des PR : [CONTRIBUTING § 6](../CONTRIBUTING.md#6-rôles).
- Sans rebase, rien ne casse en prod : la CI de ta PR reconstruit aussi les modèles de ton
  collègue, dans leur version antérieure et dans ton dataset de PR. Le résultat est juste moins
  lisible.

### Départ d'un développeur

1. Lui faire vider son dataset (`dbt run-operation vider_dataset_dev --args '{confirmer: true}'`) :
   Terraform refuse de supprimer un dataset qui contient des tables.
2. Supprimer sa clé, puis son SA.
3. Retirer son nom de `local.dbt_developers`, puis `terraform apply`.
4. Retirer son accès GitHub.

## 7. Où c'est défini

| Sujet | Fichier |
|---|---|
| Identités, datasets, bucket d'état | `infra/dbt_environments.tf` |
| Ouverture WIF au repo dbt | `infra/github_actions_wif.tf` |
| Targets `dev` / `ci` / `prod` | `profiles.yml` |
| Routage des datasets | `macros/generate_schema_name.sql` |
| Vidage du dataset de dev | `macros/vider_dataset_dev.sql` |
| CI/CD | `.github/workflows/dbt-ci.yml` |
| Variables locales | `.env.example` |
| Liste des développeurs | `infra/dbt_environments.tf` (`local.dbt_developers`) |
| Récupération du manifest | `scripts/pull-state.sh`, lancé par les hooks git (`.pre-commit-config.yaml`) |
| Alerte sur les échecs de prod | `infra/monitoring.tf`, `publish_run.py` |
