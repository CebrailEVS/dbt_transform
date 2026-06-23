# Passages appro — voie rapide (fast-lane)

> Pourquoi les passages appro ont une mécanique à part, et comment l'opérer.
> Marts concernés : `fct_neshu__passage_appro`, `fct_lcdp__passage_appro`.

## Le besoin

Le monitoring des passages appro (Roadman) doit être rafraîchi **plusieurs fois par
jour**, alors que le reste des marts d'une BU se reconstruit **1×/nuit** (à la fin de
l'EL `source:oracle_<bu>+`). On ne veut pas relancer tout le DAG Oracle à chaque refresh.

Historiquement c'était un pipeline Python séparé écrivant en direct dans `prod_marts`
(hors dbt). Il a été **consolidé dans dbt** : une seule définition métier, testée et
documentée, rafraîchie par une « voie rapide ».

## Le principe : two-speed

- **Voie nocturne** (inchangée) : EL complet `source:oracle_<bu>+` → reconstruit toute la BU 1×/nuit.
- **Voie rapide** (intra-day) : ne rebuild **que le sous-graphe ancêtre** du mart passages appro,
  pas toute la source. C'est ce qu'isole le selector dbt `passage_appro_fastlane_<bu>`.

```
Cloud Scheduler (jours ouvrés)            workflow pipeline-passages-appro-<bu>
  neshu : 0 8,11,15                         ┌─────────────────────────────────────────┐
  lcdp  : 0 8,11,15,18      ──────────────► │ 1) EL en PARALLÈLE :                     │
                                            │    • tap transactionnel (incr.)         │  TASK, TASK_HAS_RESOURCES,
                                            │      tap-oracle-<bu>-appro-fastlane      │  LABEL_HAS_TASK  → prod_raw
                                            │    • tap référentiels (full table)      │  COMPANY, DEVICE, LOCATION, CONTACT,
                                            │      tap-oracle-<bu>-appro-refs          │  TASK_STATUS, RESOURCES, LABEL
                                            │ 2) dbt build                            │
                                            │    --selector passage_appro_fastlane_<bu>│  → fct_<bu>__passage_appro
                                            │ 3) refresh Power BI                     │
                                            └─────────────────────────────────────────┘
```

dbt attend la fin des **deux** branches EL avant de builder (barrière).

## Pourquoi DEUX taps EL en parallèle (le détail important)

La voie rapide est un **build partiel** : elle rafraîchit les **faits** (table `TASK`,
fréquente) mais pas, par défaut, les **tables de référence** (company, device, location…
rafraîchies seulement la nuit).

Conséquence sans le tap réf : une entité créée dans la journée (ex. une nouvelle
`location`) est référencée par une tâche fraîche mais **absente du référentiel stale** →
les tests `relationships` du staging échouent (orphelin) et **bloquent le run**.

Le **tap réf** (`*-appro-refs`, FULL_TABLE) rafraîchit en parallèle les références que le
sous-graphe reconstruit (`COMPANY, DEVICE, LOCATION, TASK_STATUS, RESOURCES, LABEL`).
Résultat : dims fraîches dès l'intra-day, tests `relationships` qui restent **stricts
(`error`)** sans flapping, et un nouveau client/machine/site visible le jour même.

> **Garder une réf fraîche = tap réf (`prod_raw`) + reconstruction du `stg_*` (selector).**
> Le tap seul ne suffit pas : le test `relationships` compare deux modèles *staging*.
> Ex. `contact` (champ d'affichage utile, non joint par la chaîne appro) est ajouté au tap
> réf (`CONTACT`) **et** au selector (`stg_oracle_<bu>__contact`) → il est frais ET son test
> reste strict en voie rapide. Pour une réf de bord qu'on ne tient pas à tester en rapide,
> ne rien faire : `cautious` (cf. § Stratégie de tests) la diffère au nocturne.

## Isolation par BU

Chaque workflow utilise **son** selector (`passage_appro_fastlane_neshu` /
`_lcdp`) → un run ne reconstruit que sa BU. Le selector combiné `passage_appro_fastlane`
existe encore mais **uniquement pour un full refresh manuel des deux** :
`dbt build --selector passage_appro_fastlane`.

## Stratégie de tests : `--indirect-selection cautious`

La voie rapide est un **build partiel** → certains tests `relationships` croisent sa
frontière : FK du mart vers les dims (le mart ne joint pas les dims, star schema), ou
cross-refs vers des `stg_*` non reconstruits ici. En `eager` (défaut dbt) ces tests
tourneraient contre des données stale et **flapperaient**.

Le build rapide tourne donc avec **`dbt build --selector … --indirect-selection cautious`**
(dans `entrypoint.sh`, branche selector) : un test n'est exécuté que si **tous** ses
modèles parents sont dans la sélection.

- Tests **entièrement dans** le sous-graphe (task→location/company/device/task_status,
  PK, tests propres du mart) → exécutés, **stricts (`error`)**.
- Tests **de bord** (FK mart→dim, task→task_type, company→company_type…) → **ignorés en
  voie rapide**, exécutés au **nocturne** (`--select source:X+`, eager) où tout est cohérent.

Principe : on ne relâche aucune sévérité et on ne court pas après chaque réf de bord —
le partiel ne teste que son périmètre, l'intégrité complète reste garantie 1×/jour au nocturne.

## Opérer

- **Déploiement**
  - Taps : commit `meltano.yml` dans `elt_oracle_<bu>` → la CI rebuild l'image `elt-oracle-<bu>`.
  - Selector : `selectors.yml` est embarqué dans l'image `dbt-runner` (rebuild via la CI transform).
  - Workflow : `terraform apply` (le module lit le YAML de `workflows/`).
- **State Meltano** : le tap transactionnel est incrémental (state propre par BU) ; le tap
  réf est FULL_TABLE (full reload à chaque run, **pas de state à seeder**). Au tout premier
  run d'un nouveau tap transactionnel, seeder son state depuis celui du tap nocturne pour
  éviter un backfill complet.
- **Lancer manuellement** : `gcloud workflows execute pipeline-passages-appro-<bu> --location europe-west1`.

## Dépannage

- **Échec `relationships` sur une réf du cœur en voie rapide** (company/device/location/
  task_status/resources/label) : le tap réf n'a pas tourné/abouti → vérifier la branche
  `branch_refs` du workflow et le tap `*-appro-refs`.
- **Échec sur une réf de bord en voie rapide** : ne devrait plus arriver — `cautious` les
  diffère au nocturne. Si ça arrive, c'est que la réf est dans le sous-graphe : soit la
  laisser (tap réf + selector, cf. contact), soit la sortir du selector pour la renvoyer au nocturne.
- **Échec de bord au nocturne** : vrai problème d'intégrité source à investiguer (pas un
  artefact de la voie rapide).
