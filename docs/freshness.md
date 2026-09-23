# Source freshness — référence

> Vérifié le 2026-09-22 : 61 tables en `PASS`, 0 warn, 0 error.

Référence unique du monitoring de fraîcheur. **14 sources, 115 tables déclarées.**

---

## État par source

| Source | Tables | Tier | Méthode | Champ | Seuils |
|---|---|---|---|---|---|
| `oracle_neshu` | 20 / 30 | Critique | A · source | `_extracted_at` | 26h / 36h |
| `oracle_lcdp` | 17 / 28 | Critique | A · source | `_extracted_at` | 26h / 36h |
| `yuman_api` | 8 / 13 | Standard | A · source | `_extracted_at` | 26h / 48h |
| `mssql_sage` | 8 / 8 | Standard | A · source | `_extracted_at` | 26h / 48h |
| `powerbi_activity` | 4 / 4 | Standard | A · source | `_extracted_at` | 26h / 48h |
| `gac` | 2 / 2 | Relaxe | A · source | `_extracted_at` | 7j / 14j |
| `yuman_evs_sftp` | 1 / 1 | Quotidien 7j/7 | A · source | `timestamp(export_date)` | 36h / 48h |
| `nesp_co.base_client` | 1 / 3 | Manuel | A · table | `_extracted_at` | 60j / 90j |
| `nesp_co` activite + opportunite | 2 | Standard | B · staging | `extracted_at` | 2j warn |
| `nesp_tech` | 2 | Hebdomadaire | B · staging | `date_heure_fin`, `date_intervention` | 8j / 14j |
| `oracle_neshu_gcs` | 1 | Standard | B · staging | `extracted_at` | 26h / 48h |
| `oracle_lcdp_gcs` | 1 | Standard | B · staging | `extracted_at` | 26h / 48h |
| `zoho_desk` | 1 modèle | Relaxe | B · staging | `created_time` | 7j warn |
| `apptech` | 0 / 8 | — | **aucune** | — | — |
| `historic` | 0 / 1 | — | aucune (archive 2024) | — | — |

**26 tables en `freshness: null`** (10 `oracle_neshu`, 11 `oracle_lcdp`, 5 `yuman_api`) :
des référentiels immuables — `*_type`, `label`, `label_family`, `product_unit`,
`*_categories`.

> **Principe directeur** : `freshness: null` ne se justifie que sur un référentiel
> vraiment immuable. Toute source portant des événements mérite un seuil, quitte
> à le mettre très large.

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

Ne jamais répéter le `freshness` par table quand il est identique au défaut source.

**Une colonne DATE est refusée** (`dbt9002 : loaded_at_field should have a timestamp
type`) — vérifié le 2026-09-22. Deux parades : une expression
(`loaded_at_field: timestamp(export_date)`, cf. `yuman_evs_sftp`) ou
`loaded_at_query`.

## Méthode B — test de récence sur le staging

Quand le raw ne livre qu'un STRING. Le staging a déjà casté, on monitore là.

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

**Différence de gravité à connaître** : un test en `severity: error` fait échouer
`dbt build`, donc le job Cloud Run — la chaîne s'arrête. Un `error_after` de
méthode A est au contraire **non bloquant** : `entrypoint.sh` l'enveloppe
volontairement et se contente d'écrire sur stderr. La méthode B est donc la plus
stricte des deux.

---

## Pourquoi ces seuils — les cas non évidents

### `yuman_evs_sftp` — 36h / 48h
`export_date` est un snapshot DATE (donc minuit) et reflète la date de
**modification du fichier** sur le SFTP, pas celle du run. Pire cas normal :
juste avant le run du lendemain, soit ~30h30. 36h laisse ~5h de marge, 48h ne se
déclenche que si une journée entière manque.

Le seuil a été resserré à la bascule dlt (2026-08-02) : l'ancienne source
tolérait 80h parce que le cron Meltano sautait le dimanche.

**C'est la seule détection d'un jour perdu.** Cinq jours ouvrés ont disparu de
l'archive entre novembre 2025 et février 2026, dont quatre consécutifs, sans que
rien ne le signale. La source n'est pas rétroactive.

### `powerbi_activity` — 26h / 48h
Seuil serré sur une source non critique pour la BI, délibérément : **l'API admin
ne conserve que 27 jours glissants**, et `prod_raw` est la seule archive. Le
pipeline tourne 7j/7 à 03:30 ; la perte est irréversible.

### `oracle_*_gcs` — 26h / 48h
Pipelines en fin de soirée, LCDP décalé après NESHU. Gap observé de 23-24h, très
régulier.

### `nesp_co` activite / opportunite — 2j, warn seul
Seules sources de méthode B sans seuil d'erreur. À trancher : aligner sur les
autres, ou documenter pourquoi le warn suffit.

---

## Non couvert — assumé

- **`apptech`** (8 tables) : tables externes GCS, écrites par l'app sans cron
  (cf. mémoire projet — ne jamais rattacher à un pipeline horaire). Un seuil
  horaire n'aurait pas de sens ; un arrêt total passerait aujourd'hui inaperçu.
  **Décision explicite à prendre.**
- **`historic`** (1 table) : archive 2024 figée, la fraîcheur n'a pas d'objet.
