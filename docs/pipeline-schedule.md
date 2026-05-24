# Pipeline Schedule — État opérationnel

> Vue synchronisée extract → transform → snapshot → BI. Source de vérité pour répondre à :
> *"À quelle heure telle donnée est-elle dispo, et quand est-elle transformée ?"*
>
> **Companion docs :**
> - `scheduling_and_tagging_decisions.md` — le **pourquoi** des choix de tags/scheduling
> - `architecture_review.md` — vue archi globale
>
> **Dernière revue :** 2026-05-24
> **Source du contenu :** scan automatique de `infra/workflows.tf`, `workflows/*.yaml`,
> `transform/models/sources/*` et `transform/models/exposures/*`.
> Toute modification de cron côté Terraform doit être répercutée ici.

Timezone : **Europe/Paris** pour tous les crons.

---

## 1. Timeline journée type (jour ouvré)

```
00h ─────────────────────────────────────────────────────────────────────────────
01:00  EL  oracle_neshu, oracle_lcdp, mssql_sage(1-5), yuman(1-5)        → prod_raw
03:00  T   transform-{neshu, lcdp, finance(1-5), technique, supply_chain}-daily
03:00  EL  zoho_desk (1-5)                                                → prod_raw
06:00  EL  yuman_gcs (1-6)                                                → prod_raw
07:00  T   transform-supply-chain-daily (run #2)
07:30  EL  nesp_tech (lundi seulement)                                    → prod_raw
08:00  EL  sftp_evs/gac, nesp_co                                          → prod_raw
08:00  T   transform-technique-daily (run #2)
08:30  T   transform-commerce-daily (run #1, 1-5)
09:00  SN  transform-snapshots-daily (tous snapshots)
09:00  T   transform-services-generaux-daily
10:30  T   transform-commerce-daily (run #2, 1-5)
10:00→18h  EL+T  passages-appro-neshu/lcdp (intra-day, 7-8x/jour, 1-5)
23:00  EL  oracle_stock_theorique                                         → prod_raw
─────────────────────────────────────────────────────────────────────────────────
Légende : EL = extract/load · T = transform dbt · SN = snapshot
```

---

## 2. Cloud Scheduler — vue exhaustive

Tous les schedulers sont déclarés dans `infra/workflows.tf`. Ils invoquent un Cloud Workflow
homonyme dans `workflows/*.yaml`.

### 2.1 Extract (EL)

| Scheduler | Cron | Jours | Workflow ciblé | Source |
|---|---|---|---|---|
| pipeline-oracle-neshu | `0 1 * * *` | tous | pipeline-oracle-neshu.yaml | oracle_neshu |
| pipeline-oracle-neshu-full-refresh | `0 4 * * 0` | dim | pipeline-oracle-neshu-full-refresh.yaml | oracle_neshu (full) |
| pipeline-oracle-lcdp | `0 1 * * *` | tous | pipeline-oracle-lcdp.yaml | oracle_lcdp |
| pipeline-mssql-sage | `0 1 * * 1-5` | lun-ven | pipeline-mssql-sage.yaml | mssql_sage |
| pipeline-yuman | `0 1 * * 1-5` | lun-ven | pipeline-yuman.yaml | yuman |
| pipeline-zoho-desk | `0 3 * * 1-5` | lun-ven | pipeline-zoho-desk.yaml | zoho_desk |
| pipeline-sftp-gcs-yuman | `0 6 * * 1-6` | lun-sam | pipeline-sftp-gcs-yuman.yaml | yuman_gcs |
| pipeline-nesp-tech | `30 7 * * 1` | lun | pipeline-nesp-tech.yaml | nesp_tech |
| pipeline-sftp-evs | `0 8 * * *` | tous | pipeline-sftp-evs.yaml | gac |
| pipeline-nesp-co | `0 8 * * *` | tous | pipeline-nesp-co.yaml | nesp_co |
| pipeline-oracle-stock-theorique | `0 23 * * *` | tous | pipeline-oracle-stock-theorique.yaml | oracle_neshu_gcs |

### 2.2 Extract + ingestion directe en `prod_marts` (Cloud Run, hors dbt)

| Scheduler | Cron | Jours | Workflow | Table cible |
|---|---|---|---|---|
| ingest-oracle-neshu-passages-appro | `0 7-11,13,15 * * 1-5` | lun-ven (8x) | pipeline-passages-appro-neshu.yaml | `prod_marts.fct_neshu__monitoring_passage_appro` |
| ingest-oracle-lcdp-passages-appro | `5 7,8,9,11,15,17,18 * * 1-5` | lun-ven (7x) | pipeline-passages-appro-lcdp.yaml | `prod_marts.fct_lcdp__monitoring_passage_appro` |

> Ces tables sont déclarées en `source()` dbt via `_<bu>__marts_sources.yml` (cf. `CLAUDE.md`).

### 2.3 Transform (dbt par BU)

| Scheduler | Cron | Jours | BU | Sélecteur dbt |
|---|---|---|---|---|
| transform-neshu-daily | `0 3 * * *` | tous | neshu | `tag:neshu` |
| transform-lcdp-daily | `0 3 * * *` | tous | lcdp | `tag:lcdp` |
| transform-finance-daily | `0 3 * * 1-5` | lun-ven | finance | `tag:finance` |
| transform-technique-daily | `0 3,8 * * *` | tous (2 runs) | technique | `tag:technique` |
| transform-supply-chain-daily | `0 3,7 * * *` | tous (2 runs) | supply_chain | `tag:supply_chain` |
| transform-services-generaux-daily | `0 9 * * *` | tous | services_generaux | `tag:services_generaux` |
| transform-commerce-daily | `30 8,10 * * 1-5` | lun-ven (2 runs) | commerce | `tag:commerce` |

### 2.4 Snapshots

| Scheduler | Cron | Workflow | Périmètre |
|---|---|---|---|
| transform-snapshots-daily | `0 9 * * *` | transform-snapshots-daily.yaml | Tous les snapshots de `snapshots/` |

### 2.5 Export

| Scheduler | Cron | Jours | Workflow | Description |
|---|---|---|---|---|
| export-nesp-tech-stock-yuman | `0 10 * * 1` | lun | export-nesp-tech-stock-yuman.yaml | Export stock Nespresso Tech → SFTP Yuman |

---

## 3. Synchronisation extract → transform par BU

Pour chaque BU, vérifier que **toutes les sources tags consommées sont fraîches** avant le lancement
du `transform-<bu>-daily`. Tableau de contrôle :

| BU | Lancement | Sources requises | Source la plus tardive (jour ouvré) | Marge |
|---|---|---|---|---|
| neshu | 03:00 | oracle_neshu (01h), yuman (01h, lun-ven) | 01:00 | ~2h |
| lcdp | 03:00 | oracle_lcdp (01h) | 01:00 | ~2h |
| finance | 03:00 (lun-ven) | mssql_sage (01h, lun-ven) | 01:00 | ~2h |
| technique (run #1 03h) | 03:00 | oracle_neshu, yuman, nesp_tech (lundi 07:30) | 01:00 (sauf lundi → 07:30) | ⚠️ lundi : **insuffisant** au run #1 |
| technique (run #2 08h) | 08:00 | idem | 07:30 (lundi) | ~30 min ⚠️ serré le lundi |
| supply_chain (run #1 03h) | 03:00 | oracle_neshu (01h), oracle_stock_theorique (23h J-1) | 01:00 | 2h ✅ traite les stocks Neshu |
| supply_chain (run #2 07h) | 07:00 | + yuman_gcs (06h) | 06:00 | 1h ✅ rattrape les stocks Yuman |
| services_generaux | 09:00 | gac (08h) | 08:00 | 1h |
| commerce (run #1 08:30) | 08:30 | nesp_co (08h), nesp_tech (lundi 07:30) | 08:00 | 30 min |
| commerce (run #2 10:30) | 10:30 | idem | 08:00 | 2h30 |

**Remarques (design assumé) :**
- `supply_chain` a **2 runs intentionnels** : run #1 à 03h traite les stocks Neshu (dispo depuis
  23h J-1 via `oracle_stock_theorique`), run #2 à 07h intègre les stocks Yuman (dispo à 06h
  via `yuman_gcs`). Pas un contournement, c'est le design.
- `technique` a **2 runs intentionnels** : run #1 à 03h traite les sources Yuman (technique) et
  Oracle Neshu (dispo dès 01-02h), run #2 à 08h intègre `nesp_tech` (qui n'arrive que le lundi à
  07:30 et seulement le lundi). Les autres jours, le run #2 est redondant mais inoffensif.
- `commerce` run #1 à 08:30 a une marge de 30 min après nesp_co (08:00) — surveiller la durée
  réelle du EL.

---

## 3bis. Sources consommées par BU (lineage dbt)

Vérité issue du lineage dbt (`ref()` traversé jusqu'aux `source()` dans staging).
Permet de répondre à : *"Quelles sources doivent être fraîches pour que la BU X soit à jour ?"*.

| BU | Sources upstream (via lineage) | Sources externes `prod_marts` (Cloud Run hors dbt) | Nb marts |
|---|---|---|---|
| neshu | `oracle_neshu`, `yuman` | `marts_neshu_external` (passage_appro) | 13 (6 dim + 7 fct) |
| lcdp | `oracle_lcdp` | `marts_lcdp_external` (passage_appro) | 3 (3 dim + 0 fct) |
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
- Marts external (`marts_*_external`) sont écrits par les Cloud Run `ingest-oracle-*-passages-appro`
  (cf. §2.2), pas par dbt. Référencés en `source()` dans `_<bu>__marts_sources.yml`.

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
| Nouveau Cloud Scheduler (cron modifié, jour ajouté) | §2 + §1 timeline |
| Nouvelle BU / nouveau workflow transform | §2.3 + §3 + §1 |
| Nouvelle source dbt | §2.1 + §4 |
| Nouveau rapport PBI / exposure | §5 |
| Changement de SLA freshness | §4 |

Source du contenu : `infra/workflows.tf` est la **source de vérité** des crons.
Si un cron change, modifier le `.tf`, appliquer, **puis** mettre à jour ce doc.
