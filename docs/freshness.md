# Source freshness — référence

Référence unique du monitoring de fraîcheur : méthode, seuils et leur justification par source.

---

## État par source

| Source | Niveau | Méthode | Champ | Seuils (warn / error) |
|---|---|---|---|---|
| `oracle_neshu` | Critique | A · source | `_extracted_at` | 26h / 36h |
| `oracle_lcdp` | Critique | A · source | `_extracted_at` | 26h / 36h |
| `yuman_api` | Standard | A · source | `_extracted_at` | 26h / 48h |
| `mssql_sage` | Standard | A · source | `_extracted_at` | 26h / 48h |
| `powerbi_activity` | Standard | A · source | `_extracted_at` | 26h / 48h |
| `gac` | Relâché | A · source | `_extracted_at` | 7j / 14j |
| `yuman_evs_sftp` | Quotidien 7j/7 | A · source | `timestamp(export_date)` | 36h / 48h |
| `nesp_co.base_client` | Manuel | A · table | `_extracted_at` | 60j / 90j |
| `nesp_co` activite + opportunite | Standard | B · staging | `extracted_at` | 2j / — |
| `nesp_tech` | Hebdomadaire | B · staging | `date_heure_fin`, `date_intervention` | 8j / 14j |
| `oracle_neshu_gcs` | Standard | B · staging | `extracted_at` | 26h / 48h |
| `oracle_lcdp_gcs` | Standard | B · staging | `extracted_at` | 26h / 48h |
| `zoho_desk` | Relâché | B · staging | `created_time` | 7j / — |
| `apptech` | — | **aucune** | — | — |
| `historic` | — | aucune (archive figée) | — | — |

Une source dont le pipeline est en pause (`paused = true` dans `infra/workflows_el.tf`, cas de
`zoho_desk`) n'est **pas surveillée** : son test ne tourne qu'avec son workflow.

**`freshness: null`** : réservé aux référentiels vraiment immuables (`*_type`, `label`,
`label_family`, `product_unit`, `*_categories`). Toute table portant des événements mérite un
seuil, quitte à le mettre très large.

> Écart connu : sont aussi en `null` des tables qui évoluent (`lcdp_contract`, `*_string`,
> `lcdp_v_label_*`, `yuman_evs_products`, `yuman_evs_products_storehouses`). À corriger dans
> les `_<source>__sources.yml`.

---

## Méthode A — freshness native, au niveau source

Quand le raw expose un **TIMESTAMP** exploitable.

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

Ne pas répéter le `freshness` par table quand il est identique au défaut de la source.

**Une colonne DATE est refusée** (`dbt9002 : loaded_at_field should have a timestamp type`).
Deux parades : une expression (`loaded_at_field: timestamp(export_date)`, cf. `yuman_evs_sftp`)
ou `loaded_at_query`.

## Méthode B — test de récence sur le staging

Quand le raw n'a pas de timestamp exploitable, ou quand la fraîcheur se juge sur une colonne du
staging (date métier). Le staging a déjà casté, on surveille là.

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

`oracle_*_gcs` et `nesp_co` (activite, opportunite) sont en méthode B alors que leur raw porte un
`_extracted_at` TIMESTAMP : la méthode A s'y appliquerait.

**Différence de gravité** : un test en `severity: error` fait échouer `dbt build`, donc le job
Cloud Run, et la chaîne s'arrête. Un `error_after` de méthode A est **non bloquant** :
`entrypoint.sh` l'écrit sur stderr et poursuit. La méthode B en `error` est donc la plus stricte.

---

## Pourquoi ces seuils — les cas non évidents

### `yuman_evs_sftp` — 36h / 48h
`export_date` est une DATE (donc minuit) qui reflète la date de **modification du fichier** sur
le SFTP, pas celle du run. Pire cas normal : juste avant le run du lendemain, soit ~30h30. 36h
laisse la marge, 48h ne se déclenche que si une journée entière manque.

**C'est la seule détection d'un jour perdu** : la source n'est pas rétroactive, un fichier
manqué ne revient pas.

### `powerbi_activity` — 26h / 48h
Seuil serré sur une source non critique pour la BI, délibérément : **l'API admin ne conserve
que 27 jours glissants** et `prod_raw` est la seule archive. Le pipeline est quotidien ; une
perte au-delà de 27 jours est irréversible.

### `oracle_*_gcs` — 26h / 48h
Pipelines quotidiens de fin de soirée, LCDP après NESHU : l'écart entre deux chargements est de
l'ordre de 24h.

### `nesp_co` activite / opportunite et `zoho_desk` — warn seul
Seules sources sans seuil d'erreur : un retard y est signalé sans jamais bloquer la chaîne.

---

## Non couvert — assumé

- **`apptech`** : tables externes GCS écrites par l'application, sans cron ; le build est
  déclenché par l'application (ne jamais la rattacher à un pipeline planifié, cf.
  [`docs/apptech/ingestion.md`](apptech/ingestion.md)). Un seuil horaire n'a pas de sens ; un
  arrêt total de l'application ne serait pas détecté par dbt.
- **`historic`** : archive figée, la fraîcheur n'a pas d'objet.
