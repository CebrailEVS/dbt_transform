# Rythme des pipelines — principes

> Ce document explique **comment** la donnée circule de la source au mart, et
> **pourquoi** l'orchestration est faite ainsi.
>
> Il ne contient **aucun horaire, aucun décompte, aucun cron**. Ces valeurs
> changent, et une copie diverge toujours de l'original. La cadence réelle vit
>
> dans `infra/workflows_el.tf` — s'y reporter, ou lire l'état réel (y compris les
> schedulers intraday et ceux en pause) :
>
> ```bash
> gcloud scheduler jobs list --location=europe-west1 \
>   --format='table(name.basename(),schedule,state)'
> ```

---

## 1. Le principe : chaque EL enchaîne sa propre transformation

Il n'y a **pas** de workflow « transform » global. Chaque pipeline d'extraction dont la
source est déclarée dans dbt fait, dans un seul workflow :

```
extract → load BigQuery → dbt build --select source:<source>+ → [refresh Power BI]
```

Conséquence directe : **l'ordre entre le chargement et la transformation est
garanti par construction**, sans qu'aucun cron n'ait à être accordé avec un
autre. C'est la principale raison de ce choix.

Le sous-graphe à reconstruire n'est pas maintenu à la main : `source:<source>+`
laisse le **lignage dbt** décider de ce qui descend de cette source. Ajouter un
mart ne demande donc aucune modification d'orchestration.

**Deux exceptions à connaître :**
- Les **snapshots** ont leur propre workflow (`dbt snapshot`, jamais `dbt build`).
  Ils sont exclus de tous les autres builds.
- La chaîne **`apptech`** n'est branchée sur aucun cron, volontairement : son
  build est déclenché par l'app Suivi Tech elle-même (événementiel).
- Les workflows d'**export** et ceux dont la donnée n'entre pas dans dbt
  (ex. `pipeline-oracle-nayka`, `pipeline-yuman-lcdp`) n'ont pas d'étape dbt.

## 2. Trois régimes de cadence

| Régime | Ce qu'il sert | Comment il est exprimé |
|---|---|---|
| **Nocturne** | remise à niveau complète d'une source | un scheduler par pipeline EL |
| **Intraday** | besoins suivis en journée (passages appro, tournées roadmen) | un **second scheduler** pointant le **même** workflow |
| **Hebdomadaire** | sources à faible rotation, et les purges | un scheduler à cadence hebdo |

L'intraday ne duplique pas le YAML : deux schedulers, une seule recette. Dupliquer
condamnerait à reporter chaque correction deux fois, et un jour elles
divergeraient.

**L'intraday reconstruit le sous-graphe entier de la source**, pas un
sous-ensemble ciblé : un second workflow plus étroit ne vaut pas sa maintenance
pour un mainteneur unique. Si le besoin revient, passer un sélecteur différent
dans le `body` du scheduler intraday (`params:`), pas dupliquer le YAML.

> **Un build partiel casse les tests `relationships`** : une entité créée dans la
> journée, référencée par une tâche fraîche, manque au référentiel resté sur la
> nuit. Parades : rafraîchir aussi les référentiels joints, ou différer ces tests
> au build complet (`--indirect-selection cautious`, géré par `entrypoint.sh` en
> mode sélecteur nommé).

## 3. Fan-out : ce que ce design coûte, assumé

Un mart se reconstruit **dès qu'une seule** de ses sources atterrit. On n'attend
pas que toutes soient fraîches : la fraîcheur est *eventual*.

- **Un mart multi-sources est reconstruit une fois par source.** Redondant, mais
  il est toujours aussi frais que sa source la plus récente.
- **Un mart multi-sources expose un état mixte** : une source à jour, une autre
  d'hier. C'est acceptable tant que le grain du mart ne suppose pas la
  simultanéité — à vérifier au cas par cas quand on écrit le mart.
- **Pas de filet nocturne.** Si un EL échoue ou ne tourne pas ce jour-là, ses
  marts restent sur la dernière donnée chargée jusqu'au prochain run de la
  source. Aucun rattrapage automatique.
- **Le fan-out traverse les refs mart→mart**, y compris entre BU. Un sélecteur
  de source touche donc parfois des marts d'une autre BU que la sienne.

Pour savoir ce qu'une source déclenche réellement, ne pas lire une matrice :
la demander à dbt.

```bash
dbt ls --select "source:<source>+" --resource-type model
```

## 4. Fraîcheur

Chaque pipeline lance `dbt source freshness` sur sa propre source **avant** le
build. Le contrôle est **non bloquant** : la donnée déjà chargée doit continuer
à se transformer. En cas de dépassement du seuil, une ligne part sur **stderr**,
que Cloud Run route en `severity=ERROR` et que l'alerte existante récupère.

Les seuils par source, et la distinction entre les deux méthodes de mesure,
vivent dans [`freshness.md`](freshness.md) — ne pas les recopier ici.

## 5. Où trouver quoi

| Question | Où est la réponse |
|---|---|
| À quelle heure tourne un pipeline ? | `infra/workflows_el.tf` |
| Qu'est-ce qu'un pipeline fait, étape par étape ? | `infra/workflows/<nom>.yaml` |
| Qu'est-ce qu'une source déclenche ? | `dbt ls --select "source:X+"` |
| Quels seuils de fraîcheur ? | [`freshness.md`](freshness.md) |
| Quel rapport BI consomme quel mart ? | `models/exposures/<bu>.yml` |
| Pourquoi l'orchestration est-elle faite ainsi ? | `infra/CLAUDE.md` § Charte |
| Les particularités d'une source | [`architecture/`](architecture/) |
