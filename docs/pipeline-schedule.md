# Circulation de la donnée en production — principes

**Comment** la donnée va de la source au mart, et **pourquoi** l'orchestration est faite ainsi.
Aucun horaire ici : la cadence vit dans `infra/workflows_el.tf`, et l'état réel des schedulers
(pauses comprises) se lit avec :

```bash
gcloud scheduler jobs list --location=europe-west1 --format='table(name.basename(),schedule,state)'
```

---

## 1. Le principe : chaque extraction enchaîne sa propre transformation

Il n'y a **pas** de workflow de transformation global. Chaque pipeline d'extraction dont la
source est déclarée dans dbt fait, dans un seul workflow :

```
extract → load BigQuery → fraîcheur de la source → dbt build --select source:<source>+ → [refresh Power BI]
```

- **L'ordre entre chargement et transformation est garanti par construction**, sans qu'aucun cron
  n'ait à être accordé avec un autre. C'est la principale raison de ce choix.
- **Le sous-graphe à reconstruire n'est pas maintenu à la main** : `source:<source>+` laisse le
  lignage dbt décider. Ajouter un mart ne demande aucune modification d'orchestration.

**Exceptions :**
- les **snapshots** ont leur propre workflow (`dbt snapshot`, jamais `dbt build`) et sont exclus
  de tous les autres builds ;
- la BU **`plateforme`** a un workflow dbt seul : sa source (`dbt_artifacts`) est écrite par le
  runner lui-même, sans extraction ;
- la chaîne **`apptech`** n'est branchée sur aucun cron, par choix
  ([architecture/apptech.md](architecture/apptech.md)) ;
- les workflows d'**export**, et ceux dont la donnée n'entre pas dans dbt, n'ont pas d'étape dbt.

## 2. Trois régimes de cadence

| Régime | Ce qu'il sert | Comment il est exprimé |
|---|---|---|
| **Quotidien** | remise à niveau complète d'une source | un scheduler par pipeline |
| **Intraday** | besoins suivis en journée (passages appro, tournées) | un **second scheduler** sur le **même** workflow |
| **Hebdomadaire** | sources à faible rotation, purges | un scheduler à cadence hebdomadaire |

L'intraday ne duplique pas le YAML du workflow : deux schedulers, une seule recette. Dupliquer
obligerait à reporter chaque correction deux fois, et un jour les deux divergeraient. L'intraday
reconstruit donc le sous-graphe entier de la source, pas un sous-ensemble ciblé.

> **Un build partiel peut faire échouer un `relationships`** : une entité créée dans la journée,
> référencée par une donnée fraîche, peut manquer à un référentiel qui n'a pas été rechargé. C'est
> l'une des raisons du `warn` sur ces tests ([CONVENTIONS.md § Tests](../CONVENTIONS.md#tests)).

## 3. Fan-out : ce que ce design coûte, assumé

Un mart se reconstruit **dès qu'une seule** de ses sources atterrit. On n'attend pas que toutes
soient fraîches : la fraîcheur est *eventual*.

- **Un mart multi-sources est reconstruit une fois par source.** Redondant, mais il est toujours
  aussi frais que sa source la plus récente.
- **Un mart multi-sources expose un état mixte** : une source à jour, une autre de la veille.
  Acceptable tant que le grain du mart ne suppose pas la simultanéité : à vérifier en écrivant le
  mart.
- **Pas de filet nocturne.** Si une extraction échoue ou ne tourne pas, ses marts restent sur la
  dernière donnée chargée jusqu'au prochain run de la source. Aucun rattrapage automatique.
- **Le fan-out traverse les `ref()` entre marts**, y compris entre BU : une source touche parfois
  des marts d'une autre BU que la sienne.

Pour savoir ce qu'une source déclenche, ne pas tenir de matrice : la demander à dbt.

```bash
dbt ls --select "source:<source>+" --resource-type model
```

## 4. Où trouver quoi

| Question | Où est la réponse |
|---|---|
| À quelle heure tourne un pipeline ? | `infra/workflows_el.tf`, `infra/workflows_dbt.tf` |
| Que fait un pipeline, étape par étape ? | `infra/workflows/<nom>.yaml` |
| Comment le runner enchaîne fraîcheur et build ? | `entrypoint.sh` |
| Qu'est-ce qu'une source déclenche ? | `dbt ls --select "source:X+"` |
| Fraîcheur : méthodes, gravité | [`freshness.md`](freshness.md) |
| Pourquoi l'orchestration est-elle faite ainsi ? | `infra/CLAUDE.md` § Charte, [`decisions.md`](decisions.md) |
