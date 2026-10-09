# Couche `apps` — modèles de service d'une application

Première application : **Cockpit Supply** (`models/apps/cockpit_supply/`).

## 1. Rôle

Une application interne (et non un rapport Power BI) lit ses données **telles quelles**, au grain
de ses écrans, au lieu de charger des marts entiers et de tout recalculer de son côté. La couche
`apps` porte ces modèles :

- **taillés pour une seule application** : grain et colonnes dictés par ses écrans ;
- **hors schéma en étoile** : tables larges, dénormalisées, sous-ensembles ou agrégats ; les règles
  de [marts.md](marts.md) (star schema strict, pas d'OBT) **ne s'appliquent pas ici** ;
- **un dataset par application** : `prod_app_<application>` (ex. `prod_app_cockpit_supply`), lu par
  le seul compte de service de l'application (moindre privilège). Le compte `powerbi` n'y a pas accès.

Ce n'est pas un raccourci pour éviter un mart : une donnée utile à plusieurs consommateurs (BI et
application) se modélise d'abord en mart, et le modèle `apps` s'appuie dessus (`ref()`).

## 2. Nommage

| Élément | Règle | Exemple |
|---|---|---|
| Dossier | `models/apps/<application>/` | `models/apps/cockpit_supply/` |
| Modèle | `app_<nom court de l'application>__<bu>_<entité>` (préfixe obligatoire : tout cohabite dans un seul dataset en dev) | `app_cockpit__neshu_stock_photo` |
| YAML | `_<application>__app_models.yml` | `_cockpit_supply__app_models.yml` |
| Dataset | `+schema: app_<application>` dans `dbt_project.yml` → `prod_app_<application>` | `prod_app_cockpit_supply` |

**Un modèle par BU** quand les sources diffèrent (Neshu, Cafés du Phare, TechCare) : les runs
intraday Oracle Neshu et LCDP peuvent partir à la même minute, et un modèle croisant les deux
serait reconstruit par deux workflows en même temps. Une synthèse inter-BU se fait en **vue**.

## 3. Matérialisation

Toujours **explicite** dans `{{ config() }}` (le défaut du projet est `view`) :

| Cas | Matérialisation |
|---|---|
| Sous-ensemble ou agrégat d'un mart, petit à moyen volume | `table` (partition sur la date filtrée par l'app, cluster sur ses clés de filtre) |
| Dérivé des tâches Oracle à gros volume | `incremental` (`merge`) : reconstruire ces tables en entier à chaque run intraday coûte cher |
| Synthèse inter-BU | `view` |

Pas de run dédié ni de selector : les modèles descendent de `source:<src>+` et sont reconstruits
par les workflows existants ; le tag `apps` / `<application>` sert aux builds manuels.

## 4. Documentation et tests

- Description YAML en 4 blocs `[QUOI MÉTIER]` / `[COMMENT CONSTRUITE]` / `[GRAIN]` / `[NOTES]`, comme
  les marts ; `[NOTES]` dit quels écrans le lisent (sans compte de routes ni date de relevé) et
  rappelle « réservé à l'application, pas de rapport Power BI dessus ».
- Grain testé : `dbt_utils.unique_combination_of_columns` (error) ; `not_null` sur ses colonnes.
- **Exposure obligatoire** `type: application` dans `models/exposures/<application>.yml`, listant
  tout ce que l'application lit (couche `apps` **et** marts / intermédiaires encore lus en direct) :
  `dbt ls -s +exposure:<application>` donne l'impact d'un changement.

## 5. Infra (repo `infra/`)

Avant le premier merge d'une nouvelle application :
1. dataset `prod_app_<application>` dans `bigquery.tf` ;
2. ajout à `dbt_read_datasets` (cible du `--defer` en dev/CI) et `dbt_prod_write_datasets`
   (job `cd`) dans `dbt_environments.tf` — sans ce dernier, le `cd` échoue au merge (et ce serait
   masqué par les droits larges de `meltano-runner` sur les runs planifiés) ;
3. `roles/bigquery.dataViewer` du compte de service de l'application sur ce dataset.

## 6. Checklist PR

- [ ] modèle dans `models/apps/<application>/`, préfixe `app_<nom court>__`, matérialisation explicite
- [ ] YAML 4 blocs + test du grain
- [ ] exposure de l'application à jour
- [ ] dataset et droits présents dans `infra/` (appliqués avant le merge)
- [ ] `dbt lint` vert, build dev vert
