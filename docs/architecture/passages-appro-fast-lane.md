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
                                            │    • tap référentiels (full table)      │  COMPANY, DEVICE, LOCATION,
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

## Isolation par BU

Chaque workflow utilise **son** selector (`passage_appro_fastlane_neshu` /
`_lcdp`) → un run ne reconstruit que sa BU. Le selector combiné `passage_appro_fastlane`
existe encore mais **uniquement pour un full refresh manuel des deux** :
`dbt build --selector passage_appro_fastlane`.

## Limite connue (eventual consistency)

Quelques FK pointent vers des références **hors** du sous-graphe appro (ex.
`task → task_type`, `task → contact`, `company → company_type`). Leur modèle staging
n'est pas reconstruit par la voie rapide, donc leur test `relationships` peut rarement
flapper sur un enregistrement créé le jour même. Ces réfs changent très rarement → laissé
en `error`, à traiter au cas par cas (cf. § Dépannage).

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

- **Échec test `relationships` sur une réf du cœur** (company/device/location/task_status/
  resources/label) : ne devrait plus arriver (tap réf). Vérifier que le tap réf tourne bien
  dans la branche `branch_refs` du workflow.
- **Échec sur une réf de bord** (task_type, contact, company_type…) : passer ce test précis
  en `severity: warn` (la réf n'est pas reconstruite par la voie rapide ; la rebuild
  entièrement reviendrait à refaire le nocturne).
