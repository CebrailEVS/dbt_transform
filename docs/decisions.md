# Décisions d'architecture

Les choix structurants du projet, leur raison et ce qui a été écarté. Une entrée évite de
reproposer une piste déjà étudiée. Revenir sur une décision : réécrire l'entrée dans la même PR
que le changement, avec la nouvelle raison.

---

## dbt v2 plutôt que `dbt-oss`

- **Décision** : paquet `dbt` (moteur v2, licence propriétaire gratuite de dbt Labs).
- **Pourquoi** : il intègre `dbt lint`, que la CI et les hooks git exécutent.
- **Écarté** : `dbt-oss` (Apache 2.0), qui n'a pas `dbt lint`.
- **Conséquence** : Python ≥ 3.11 à l'exécution ; toujours appeler `./dbt_venv/bin/dbt`.

## Un projet GCP, un dataset par usage

- **Décision** : dev, CI et prod vivent dans le même projet ; l'isolation se fait par dataset, et
  l'IAM en est la frontière.
- **Pourquoi** : le defer lit la prod sans copier de donnée, et il n'y a qu'un projet à maintenir.
- **Écarté** : un projet GCP de dev séparé, trop lourd à maintenir pour l'équipe.
- **Détail** : [`environnements.md`](environnements.md).

## Chaque extraction enchaîne sa transformation

- **Décision** : chaque workflow d'extraction lance `dbt build --select source:<source>+`. Pas de
  workflow de transformation global.
- **Pourquoi** : l'ordre chargement → transformation est garanti par construction, et le lignage
  dbt décide de ce qui se reconstruit : ajouter un mart ne touche pas l'orchestration.
- **Coût assumé** : un mart multi-sources se reconstruit une fois par source et peut exposer un
  état mixte ; pas de filet nocturne.
- **Écarté** : des workflows par BU sur cron fixe (couplage par l'horaire, doubles builds) ; une
  voie rapide intraday dans un workflow dédié (un second YAML à maintenir pour un gain limité).
- **Détail** : [`pipeline-schedule.md`](pipeline-schedule.md).

## Le raw reste fidèle à la source

- **Décision** : l'extraction ne fait aucun typage métier ; tous les casts se font en staging.
- **Pourquoi** : l'extraction reste générique, et le type se décide une fois, à côté de son usage.
- **Conséquence** : un changement de type dans le raw casse le `MERGE` d'un incrémental sur toute
  colonne passée sans cast ([`staging.md` § 4](conventions/staging.md#4-pattern-sql)).

## Staging 1 pour 1, sans dédoublonnage préventif

- **Décision** : un staging nettoie et type une table source, sans jointure ni logique métier. Un
  doublon déclenche l'échec du test `unique` ; on dédoublonne alors en connaissant la cause.
- **Pourquoi** : un `qualify row_number()` préventif garde une ligne en silence et rend le test
  vert, donc masque l'anomalie de la source.
- **Détail** : [`staging.md`](conventions/staging.md).

## Marts par BU, schéma en étoile strict

- **Décision** : les marts sont rangés par BU, en faits et dimensions reliés par `<entite>_id`.
- **Pourquoi** : un mart sert un usage métier qui croise souvent plusieurs sources ; l'étoile laisse
  Power BI agréger sans double comptage.
- **Écarté** : un rangement par source, qui cassait dès qu'un mart croisait deux sources ; une
  table par rapport, tout déjà joint (OBT).
- **Détail** : [`marts.md`](conventions/marts.md).

## Les applications ne lisent pas les marts

- **Décision** : un modèle `apps` part du staging ou de l'intermediate de sa source, jamais d'une
  dimension ni d'un fait.
- **Pourquoi** : la BI et l'application sont deux usages : une colonne demandée par l'application
  n'entre pas dans une dimension, et un changement de dimension ne modifie pas l'application.
- **Coût assumé** : une logique parfois dupliquée, à reporter des deux côtés.
- **Détail** : [`apps.md`](conventions/apps.md).

## Snapshots construits en prod seulement

- **Décision** : seul un workflow Cloud Workflows dédié lance `dbt snapshot` ; la CI, la CD et le
  dev les excluent.
- **Pourquoi** : un historique SCD2 ne se reconstruit pas. Un build ailleurs, un changement de
  stratégie ou un renommage le perdrait.
- **Détail** : [`seeds-snapshots.md`](conventions/seeds-snapshots.md#snapshots).

## Développement : defer par défaut, dataset vidé à chaque branche

- **Décision** : `--defer` seul par défaut, `--favor-state` pour une recette ; le développeur vide
  son dataset au début de chaque branche (`vider_dataset_dev`).
- **Pourquoi** : `--favor-state` par défaut ferait lire en prod un parent que l'on vient de
  construire dans une commande précédente, et masquerait la modification. Une table dev restée
  d'un ancien chantier est lue à la place de la prod : le vidage supprime ce cas.
- **Écarté** : supprimer le dataset au merge (il appartient à une personne, pas à une PR).
- **Détail** : [`environnements.md` § 1 et § 3](environnements.md).

## Conventions vérifiées par `dbt check`, en dev et en CI

- **Décision** : les règles d'architecture qu'une requête peut vérifier (une app qui lit un mart,
  un intermediate qui croise des sources, un fait qui lit un fait, les tests d'unicité, les
  descriptions) sont des fichiers de `checks/`, exécutés par `dbt build` en dev et en CI. La prod
  les saute (`--skip-checks`).
- **Pourquoi** : une règle écrite dépend de la vigilance d'un relecteur, humain ou agent ; une
  règle exécutée arrête l'erreur au moment où elle est commise. En prod, la PR les a déjà
  vérifiées, et une convention ne doit pas arrêter la production.
- **Écarté** : dbt-project-evaluator et dbt-bouncer, outils tiers en plus ; la relecture des
  conventions par l'agent `mart-reviewer`, recentré sur la donnée.
- **Détail** : [`CONVENTIONS.md`](../CONVENTIONS.md#règles-vérifiées-par-dbt-check).

## Une CI à l'échelle de l'équipe

- **Décision** : la CI construit `state:modified+` dans un dataset propre à la PR, après avoir cloné
  les incrémentaux modifiés ; le merge reconstruit le même périmètre en prod.
- **Pourquoi** : elle teste le vrai `MERGE` et tout l'aval d'un changement, pour un coût faible.
- **Écarté** : déploiement blue/green, write-audit-publish et diff de données : hors de proportion
  pour une petite équipe.
- **Détail** : [`environnements.md` § 4](environnements.md#4-les-quatre-flux).
