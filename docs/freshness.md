# Fraîcheur des sources

Comment le projet surveille l'âge des données : méthodes, paliers de seuils, gravité. Les seuils
appliqués vivent dans les YAML, chacun avec un commentaire qui le justifie :
`_<source>__sources.yml` (méthode A) ou `_<source>__models.yml` (méthode B).

---

## Paliers

Un seuil se fixe sur le **pire cas normal** : l'écart maximal entre deux chargements réussis, plus
une marge. `warn` signale un retard ; `error` signale un chargement manquant.

| Palier | Pour | warn / error |
|---|---|---|
| Critique | source quotidienne dont la BI dépend chaque matin | 26 h / 36 h |
| Standard | source quotidienne | 26 h / 48 h |
| Relâché | rotation lente, ou retard sans enjeu | 7 j / 14 j, ou `warn` seul |

Une source hebdomadaire ou livrée à la main prend sa cadence plus une marge. Une source **non
rétroactive** (un fichier ou une période manqués ne reviennent jamais) justifie un seuil serré,
même si elle n'est pas critique pour la BI : le test est alors la seule détection d'une perte.

---

## Méthode A — fraîcheur native, au niveau source

Quand le raw expose un **TIMESTAMP** exploitable (`_extracted_at`, posé par dlt).

```yaml
sources:
  - name: <source>
    config:
      loaded_at_field: _extracted_at
      freshness:
        warn_after: {count: 26, period: hour}
        error_after: {count: 48, period: hour}
    tables:
      - name: <référentiel_immuable>
        config:
          freshness: null
```

- Ne pas répéter le `freshness` par table quand il est identique au défaut de la source.
- **Une colonne DATE est refusée** (`loaded_at_field should have a timestamp type`). Parades : une
  expression (`loaded_at_field: timestamp(<colonne_date>)`) ou `loaded_at_query`. Une DATE vaut
  minuit : mesurer le pire cas depuis minuit, pas depuis l'heure du run.
- **`error_after: null` ne retire pas un seuil hérité de la source** : il est ignoré, et
  `{count: null}` est refusé à l'exécution. Pour une table qui doit s'écarter du défaut, écrire
  son propre bloc `freshness` complet.
- `dbt source freshness` n'exécute que la méthode A.

## Méthode B — test de récence sur le staging

Quand le raw n'a pas de timestamp exploitable, ou quand la fraîcheur se juge sur une date métier.
Le staging a déjà casté : on surveille là.

```yaml
models:
  - name: stg_<source>__<table>
    tests:
      - dbt_expectations.expect_row_values_to_have_recent_data:
          arguments:
            column_name: extracted_at
            datepart: hour        # ou day
            interval: 26
          config:
            severity: warn
```

Ce test tourne avec `dbt build` ou `dbt test`, comme les autres.

---

## Gravité en production

Chaque workflow lance `dbt source freshness` sur sa source avant le build.

- **Méthode A, non bloquante** : un dépassement d'`error_after` n'arrête pas le build, la donnée
  déjà chargée doit continuer à se transformer. `publish_run.py` écrit en fin de job une ligne
  `[FRESHNESS-ERROR]` en sévérité ERROR, que l'alerte envoie par mail
  ([environnements.md § 4](environnements.md#4-les-quatre-flux)).
- **Méthode B en `error`, bloquante** : le test fait échouer `dbt build`, donc le job, et l'aval
  n'est pas construit. C'est le mode le plus strict.

---

## Cas particuliers

- **Table en `merge`** : son `_extracted_at` n'avance que sur les lignes modifiées. Son âge mesure
  l'activité de la source, pas la santé du chargement : seuil large. Une table en `replace`
  rechargée à chaque run est un bon signal de santé, même si son contenu ne change pas.
- **`freshness: null`** : réservé aux référentiels vraiment immuables (types, catégories, unités).
  Toute table qui porte des événements mérite un seuil, quitte à le mettre très large.
- **Pipeline en pause** (`paused = true` dans `infra/workflows_el.tf`) : sa source n'est **pas
  surveillée**, puisque le contrôle ne tourne qu'avec son workflow.
- **Purge qui tourne hors des jours de chargement** (cas d'`oracle_nayka`) : son workflow ne
  contrôle pas la fraîcheur (`DBT_SOURCE_SELECTOR` vide), sinon il alerterait à chaque passage sur
  des tables simplement pas encore rechargées.
- **`_dlt_loads`** (chargements dlt complets) n'a pas d'`_extracted_at` : `loaded_at_field:
  inserted_at`. Elle couvre tous les pipelines de `prod_raw` et reste fraîche tant que l'un charge.

## Non couvert, assumé

- **`apptech`** : saisies humaines, sans cron et par choix ([architecture/apptech.md](architecture/apptech.md)).
  Un seuil horaire n'a pas de sens ; un arrêt total de l'application ne serait pas détecté par dbt.
- **`dbt_artifacts`** : écrite par le runner lui-même ; ses échecs sont alertés directement.
- **`historic`** : archive figée.
