# Pipeline Schedule — État opérationnel

> Vue synchronisée extract → transform → snapshot → BI. Source de vérité pour répondre à :
> *"À quelle heure telle donnée est-elle dispo, et quand est-elle transformée ?"*
>
> **Companion doc :** `docs/architecture/` — un fichier par source avec l'ERD et les points d'attention.
>
> **Dernière revue :** 2026-06-09
> **Source du contenu :** scan automatique de `infra/workflows.tf`, `workflows/*.yaml`,
> `transform/models/sources/*` et `transform/models/exposures/*`.
> Toute modification de cron côté Terraform doit être répercutée ici.
>
> **⚠️ Architecture Option C (2026-06-09)** : les marts ne sont plus construits par des
> workflows `transform-<bu>-daily` dédiés. Chaque pipeline EL build désormais avec
> `source:<source>+` → les marts d'une BU se reconstruisent automatiquement dès qu'une de
> leurs sources atterrit. Le fan-out est résolu par le graphe de lignage dbt. Seuls les
> snapshots gardent un scheduler de transform autonome.

Timezone : **Europe/Paris** pour tous les crons.

---

## 1. Timeline journée type (jour ouvré)

Chaque pipeline EL enchaîne extract → load → `dbt build source:<source>+` (staging + int +
marts aval). Le transform n'a plus d'horaire propre : il suit la fin de l'EL de sa source.

```
00h ─────────────────────────────────────────────────────────────────────────────
01:00  EL+T  oracle_neshu, oracle_lcdp, mssql_sage(1-5), yuman(1-5)   → raw+stg+int + marts neshu/lcdp/finance/technique/supply_chain
03:00  EL+T  zoho_desk (1-5)                                          → raw+stg+int (pas de marts à ce jour)
06:00  EL+T  yuman_gcs (1-6)                                          → + marts supply_chain
07:30  EL+T  nesp_tech (lundi seulement)                              → + marts technique, commerce
08:00  EL+T  sftp_evs/gac, nesp_co                                    → + marts services_generaux, commerce
09:00  SN    transform-snapshots-daily (tous snapshots)
08,11,15h  EL+T  passages-appro-neshu (fast-lane: tap appro + selector passage_appro_fastlane) → fct_neshu__passage_appro
07-18h  EL+T  passages-appro-lcdp (fast-lane: tap appro + selector passage_appro_fastlane) → fct_lcdp__passage_appro
23:00  EL+T  oracle_stock_theorique                                  → + marts supply_chain
23:15  EL+T  oracle_lcdp_stock_theorique                             → + marts supply_chain (lcdp)
─────────────────────────────────────────────────────────────────────────────────
Légende : EL = extract/load · T = transform dbt (source:X+, embarqué dans le pipeline EL) · SN = snapshot
```

> Un mart multi-source est donc rebuildé plusieurs fois/jour (une fois par source qui atterrit) —
> choix assumé : fraîcheur maximale, coût compute négligeable. Pas de filet nocturne.

---

## 2. Cloud Scheduler — vue exhaustive

Tous les schedulers sont déclarés dans `infra/workflows.tf`. Ils invoquent un Cloud Workflow
homonyme dans `workflows/*.yaml`.

### 2.1 Extract (EL)

| Scheduler | Cron | Jours | Workflow ciblé | Source |
|---|---|---|---|---|
| pipeline-oracle-neshu | `0 1 * * *` | tous | pipeline-oracle-neshu.yaml | oracle_neshu |
| pipeline-oracle-neshu-full-refresh | `0 4 * * 0` | dim | pipeline-oracle-neshu-full-refresh.yaml | oracle_neshu (full) |
| pipeline-passages-appro-neshu | `0 8,11,15 * * 1-5` | lun-ven | pipeline-passages-appro-neshu.yaml | oracle_neshu (fast-lane appro: EL `tap-oracle-appro-fastlane` + dbt selector `passage_appro_fastlane` + refresh PBI) |
| pipeline-oracle-lcdp | `0 1 * * *` | tous | pipeline-oracle-lcdp.yaml | oracle_lcdp |
| pipeline-passages-appro-lcdp | `5 7,8,9,11,15,17,18 * * 1-5` | lun-ven | pipeline-passages-appro-lcdp.yaml | oracle_lcdp (fast-lane appro: EL `tap-oracle-lcdp-appro-fastlane` + dbt selector `passage_appro_fastlane` + refresh PBI) |
| pipeline-mssql-sage | `0 1 * * 1-5` | lun-ven | pipeline-mssql-sage.yaml | mssql_sage |
| pipeline-yuman | `0 1 * * 1-5` | lun-ven | pipeline-yuman.yaml | yuman |
| pipeline-zoho-desk | `0 3 * * 1-5` | lun-ven | pipeline-zoho-desk.yaml | zoho_desk |
| pipeline-sftp-gcs-yuman | `0 6 * * 1-6` | lun-sam | pipeline-sftp-gcs-yuman.yaml | yuman_gcs |
| pipeline-nesp-tech | `30 7 * * 1` | lun | pipeline-nesp-tech.yaml | nesp_tech |
| pipeline-sftp-evs | `0 8 * * *` | tous | pipeline-sftp-evs.yaml | gac |
| pipeline-nesp-co | `0 8 * * *` | tous | pipeline-nesp-co.yaml | nesp_co |
| pipeline-oracle-stock-theorique | `0 23 * * *` | tous | pipeline-oracle-stock-theorique.yaml | oracle_neshu_gcs |

### 2.2 Transform (dbt) — embarqué dans les pipelines EL (Option C)

Il n'existe **plus** de scheduler `transform-<bu>-daily`. Chaque pipeline EL de §2.1 exécute,
après son extract/load, un `dbt build --select source:<source>+` (étape `run_dbt`, env var
`DBT_TAG_SELECTOR`). Le sélecteur par pipeline :

| Pipeline EL | Sélecteur build | Marts reconstruits (BU aval via lignage) |
|---|---|---|
| pipeline-oracle-neshu | `source:oracle_neshu+` | neshu, supply_chain |
| pipeline-oracle-lcdp | `source:oracle_lcdp+` | lcdp, supply_chain |
| pipeline-yuman | `source:yuman_api+` | neshu, technique |
| pipeline-sftp-gcs-yuman | `source:yuman_gcs+` | supply_chain |
| pipeline-nesp-tech | `source:nesp_tech+` | technique, commerce |
| pipeline-nesp-co | `source:nesp_co+` | commerce |
| pipeline-sftp-evs | `source:gac+` | services_generaux |
| pipeline-mssql-sage | `source:mssql_sage+` | finance |
| pipeline-oracle-stock-theorique | `source:oracle_neshu_gcs+` | supply_chain |
| pipeline-oracle-lcdp-stock-theorique | `source:oracle_lcdp_gcs+` | supply_chain (lcdp) |
| pipeline-zoho-desk | `source:zoho_desk+` | — (pas de marts à ce jour) |

> `pipeline-oracle-neshu-full-refresh` garde `tag:oracle_neshu` (+`--full-refresh`, staging/int).
> Snapshots exclus de tout `dbt build` via `--exclude resource_type:snapshot` (entrypoint).

### 2.3 Snapshots

| Scheduler | Cron | Workflow | Périmètre |
|---|---|---|---|
| transform-snapshots-daily | `0 9 * * *` | transform-snapshots-daily.yaml | Tous les snapshots de `snapshots/` |

### 2.4 Export

| Scheduler | Cron | Jours | Workflow | Description |
|---|---|---|---|---|
| export-nesp-tech-stock-yuman | `0 10 * * 1` | lun | export-nesp-tech-stock-yuman.yaml | Export stock Nespresso Tech → SFTP Yuman |

---

## 3. Fan-out source → BU (Option C)

Plus de barrière de synchronisation : un mart se reconstruit **dès qu'une seule** de ses sources
atterrit, via le `source:<source>+` du pipeline EL correspondant. Pas besoin que toutes les
sources soient fraîches en même temps (fraîcheur *eventual*). Matrice de déclenchement :

| Source rafraîchie (EL) | BU dont les marts se reconstruisent |
|---|---|
| oracle_neshu | neshu, supply_chain |
| oracle_lcdp | lcdp, supply_chain |
| yuman_api | neshu, technique |
| yuman_gcs | supply_chain |
| oracle_neshu_gcs | supply_chain |
| oracle_lcdp_gcs | supply_chain (lcdp) |
| nesp_tech | technique, commerce |
| nesp_co | commerce |
| mssql_sage | finance |
| gac | services_generaux |

**Conséquences (design assumé) :**
- Un mart multi-source est rebuildé une fois par source (ex. `technique` via oracle_neshu 01h,
  yuman 01h, puis nesp_tech le lundi 07:30). Redondant mais inoffensif, fraîcheur maximale.
- **Pas de filet nocturne** : si un EL échoue ou ne tourne pas (week-end pour yuman/mssql_sage,
  semaine pour nesp_tech), les marts concernés restent sur la dernière donnée chargée jusqu'au
  prochain run de la source. Choix assumé.
- `fct_technique__alerting_consommation_aguila` et `fct_technique__piece_detachee_pricing_nespresso`
  ne dépendent que de `nesp_tech` (hebdo, lundi) → rebuild hebdo = cohérent avec leur source.
- Le fan-out suit le lignage `ref()`/`source()` : il traverse les refs mart→mart cross-BU
  (ex. `source:oracle_neshu+` touche aussi des marts technique via `fct_neshu__workorder_delai`).
  Couverture validée : `dbt ls --select source:X+,tag:marts` → 40/40 marts, 0 orphelin.

---

## 3bis. Sources consommées par BU (lineage dbt)

Vérité issue du lineage dbt (`ref()` traversé jusqu'aux `source()` dans staging).
Permet de répondre à : *"Quelles sources doivent être fraîches pour que la BU X soit à jour ?"*.

| BU | Sources upstream (via lineage) | Sources externes `prod_marts` (Cloud Run hors dbt) | Nb marts |
|---|---|---|---|
| neshu | `oracle_neshu`, `yuman` | — | 13 (6 dim + 7 fct) |
| lcdp | `oracle_lcdp` | — | 4 (3 dim + 1 fct) |
| technique | `yuman`, `nesp_tech` | — | 10 (5 dim + 5 fct) |
| commerce | `nesp_co`, `nesp_tech` | — | 1 (0 dim + 1 fct) |
| finance | `mssql_sage` (+ source statique `historic`) | — | 1 (0 dim + 1 fct) |
| services_generaux | `gac` | — | 1 (0 dim + 1 fct) |
| supply_chain | `oracle_neshu`, `oracle_neshu_gcs`, `yuman_gcs` | — | 3 (0 dim + 3 fct) |

**Notes :**
- `technique` consomme du parc machine Neshu via `yuman` (les machines y sont synchronisées), pas
  directement via `oracle_neshu`.
- `commerce` dépend de `nesp_tech` qui n'est rafraîchi que **le lundi à 07:30** : les autres jours,
  le run commerce retraite les mêmes interventions Nespresso Tech que le lundi précédent.
- `supply_chain` est la seule BU qui consomme les exports GCS (`*_gcs`).

---

## 4. Freshness SLA dbt (sources)

Les sources critiques exposent un `freshness` dans `models/staging/*/_*_sources.yml` :

| Source | warn_after | error_after |
|---|---|---|
| oracle_neshu, oracle_lcdp, yuman, mssql_sage, zoho_desk | 26h | 36h |
| yuman_gcs, oracle_neshu_gcs, gac | 26h | 48h (relaxe) |
| Référentiels (seeds-like sources) | — | — |

`dbt source freshness` peut être lancé manuellement ; pas encore intégré comme gate automatique
dans les workflows transform.

---

## 5. Exposures Power BI (consommation aval)

| BU | Rapports déclarés (`models/exposures/<bu>.yml`) | Owner |
|---|---|---|
| neshu | Business Review, Reporting Appro, Maintenance Préventives, PROD Chargement & Consos, PROD Délais Curatives, PROD Scorecard Neshu-Nespresso, Passage Appro Monitoring | Cebrail Aksoy |
| lcdp | Passage Appro Monitoring LCDP | Cebrail Aksoy |
| technique | Pilotage Technique, Cartographie Partenaires | Etienne Boulinier |
| finance | P&L par BU | Etienne Boulinier |
| supply_chain | Diagramme Flux Neshu (dev) | Etienne Boulinier |
| commerce | PROD Machines & Interventions | Rim Bouchikhi |
| services_generaux | *(aucune exposure déclarée)* | — |

---

## 6. TODO — à compléter à la main

Ces infos ne sont pas dans le code, à enrichir au fil de l'eau :

- [ ] **Durée typique de chaque workflow** (extract et transform) — à relever depuis Cloud Workflows
      exec logs. Cible : ajouter une colonne "durée p50 / p95" aux tableaux §2.
- [ ] **Mécanisme de refresh Power BI** — les jobs `pipeline-passages-appro-*` annoncent un
      refresh PBI mais le déclenchement exact (API call PBI ? Power Automate ?) n'est pas dans
      le code Terraform. Documenter ici.
- [ ] **Horaires de refresh Power BI dataset** par rapport (au-delà des passages appro) — quand
      les datasets PBI quotidiens se rafraîchissent-ils ? (à confirmer côté tenant PBI)
- [ ] **Zoho Desk** — pipeline actif depuis quand, consommé par quel mart ? (pas d'exposure encore)
- [ ] **Exposures `commerce` et `services_generaux`** — créer les fichiers si des rapports PBI
      existent réellement.
- [ ] **Alerting** — où sont remontés les échecs de workflow (Slack, mail, Cloud Monitoring) ?
      Cf. `infra/monitoring.tf` à documenter.

---

## 7. Comment maintenir ce doc

À mettre à jour **dans la même PR** quand :

| Changement | Section à mettre à jour |
|---|---|
| Nouveau Cloud Scheduler EL (cron modifié, jour ajouté) | §2.1 + §1 timeline |
| Nouvelle source dbt | §2.1 + §2.2 (ajouter le `source:X+`) + §3 matrice + §4 |
| Nouveau mart dans une BU existante | rien côté scheduling (rebuild auto via `source:X+`) ; §3bis si nouvelle source upstream |
| Nouveau rapport PBI / exposure | §5 |
| Changement de SLA freshness | §4 |

Source du contenu : `infra/workflows.tf` est la **source de vérité** des crons.
Si un cron change, modifier le `.tf`, appliquer, **puis** mettre à jour ce doc.
