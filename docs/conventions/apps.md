# Couche `apps` — modèles de service d'une application

Règles transversales : [CONVENTIONS.md](../../CONVENTIONS.md).

## 1. Rôle

Une application interne (et non un rapport Power BI) lit ses données **telles quelles**, au grain
de ses écrans, au lieu de charger des marts entiers et de tout recalculer de son côté. La couche
`apps` porte ces modèles :

- **taillés pour une seule application** : grain et colonnes dictés par ses écrans ;
- **hors schéma en étoile** : tables larges, dénormalisées, sous-ensembles ou agrégats ; les règles
  de [marts.md](marts.md) (star schema strict, pas d'OBT) **ne s'appliquent pas ici** ;
- **un dataset par application** : `prod_app_<application>` (ex. `prod_app_cockpit_supply`), lu par
  le seul compte de service de l'application (moindre privilège). Le compte `powerbi` n'y a pas accès.

**Indépendance vis-à-vis des marts** : un modèle `apps` part du staging ou de l'intermediate de sa
source, jamais d'une dim ni d'un fait. La BI et l'application sont deux usages distincts : une colonne
demandée par l'app ne s'ajoute pas à une dim, et un changement de dim ne modifie pas l'app. Le prix est
une logique parfois dupliquée (pivot de labels, chaîne de prévision…), assumé : une évolution de règle se
reporte dans les deux.

Où mettre une logique lue par plusieurs modèles `apps` :
- **une seule source** → un `int_<source>__*` ;
- **plusieurs sources** (interdit en intermediate) → un modèle `apps` d'étape, non lu par l'application
  et dit comme tel dans `[NOTES]`.

Contrôle : `grep -l "ref('dim_\|ref('fct_" models/apps/*/*.sql` ne doit rien renvoyer.

## 2. Nommage

| Élément | Règle | Exemple |
|---|---|---|
| Dossier | `models/apps/<application>/` | `models/apps/cockpit_supply/` |
| Modèle | `app_<nom court de l'application>__<bu>_<entité>` (préfixe obligatoire : tout cohabite dans un seul dataset en dev) | `app_cockpit__neshu_stock_photo` |
| YAML | `_<application>__app_models.yml` | `_cockpit_supply__app_models.yml` |
| Dataset | `+schema: app_<application>` dans `dbt_project.yml` → `prod_app_<application>` | `prod_app_cockpit_supply` |

**Un modèle par BU** quand les sources diffèrent : les workflows de deux sources peuvent partir en
même temps, et un modèle qui les croise serait reconstruit deux fois en parallèle. Une synthèse
inter-BU se fait en **vue**.

## 3. Matérialisation

Toujours **explicite** dans `{{ config() }}` (le défaut du projet est `view`) :

| Cas | Matérialisation |
|---|---|
| Cas général : sous-ensemble ou agrégat du staging ou de l'intermediate | `table` (partition sur la date filtrée par l'app, cluster sur ses clés de filtre) |
| Dérivé des tâches Oracle à gros volume | `incremental` (`merge`) : le reconstruire en entier à chaque run intraday coûterait cher |
| Synthèse inter-BU | `view` |

Pas de run dédié ni de selector : les modèles descendent de `source:<src>+` et sont reconstruits
par les workflows existants ; le tag `apps` / `<application>` sert aux builds manuels.

Heures : même contrat que les marts ([CONVENTIONS.md § Fuseau horaire](../../CONVENTIONS.md#fuseau-horaire)).

## 4. Documentation et tests

- Description YAML en 4 blocs, comme les marts ([marts.md § 2](marts.md#2-description-en-4-blocs)) ;
  `[NOTES]` dit quels écrans le lisent et rappelle « réservé à l'application, pas de rapport
  Power BI dessus ».
- Grain testé : `dbt_utils.unique_combination_of_columns` (error) ; `not_null` sur ses colonnes.
- **Exposure obligatoire** `type: application` dans `models/exposures/<application>.yml`, listant
  tout ce que l'application lit (couche `apps` **et** marts / intermédiaires encore lus en direct) :
  `dbt ls -s +exposure:<application>` donne l'impact d'un changement.

## 5. Infra (repo `infra/`)

Avant le premier merge d'une nouvelle application, dans `infra/` :
1. le dataset `prod_app_<application>` ;
2. ce dataset dans les datasets lus par dev et CI (cible du `--defer`) et dans ceux écrits par le
   job `cd` (`dbt_environments.tf`). Sans ce dernier, le `cd` échoue au merge, alors que les runs
   planifiés passent : leur identité a des droits plus larges ;
3. la lecture du dataset pour le compte de service de l'application.

## 6. Checklist PR

- [ ] modèle dans `models/apps/<application>/`, préfixe `app_<nom court>__`, matérialisation explicite
- [ ] YAML 4 blocs + test du grain
- [ ] exposure de l'application à jour
- [ ] dataset et droits présents dans `infra/` (appliqués avant le merge)
- [ ] `dbt lint` vert, build dev vert
