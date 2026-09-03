# Architecture — Power BI activity (`powerbi_activity`)

> Dernière mise à jour : 2026-09-03
> **État : source en production, AUCUN modèle dbt encore construit.**

---

## Vue d'ensemble

`powerbi_activity` expose les **journaux d'administration du locataire Power BI** :
qui a consulté quel rapport et quand, plus l'inventaire complet des espaces de
travail, rapports et modèles sémantiques.

Objectif métier : piloter l'usage réel du parc de rapports — lesquels servent,
lesquels sont morts, quelle direction les utilise, et quel coût de
rafraîchissement porte un rapport que personne n'ouvre.

Donnée clé exposée :
- **Événements d'activité** (`powerbi_activity_events`) — 1 ligne = 1 événement
  d'audit, identifié par `id` (GUID du journal d'audit unifié)
- **Inventaire** (`powerbi_workspaces`, `powerbi_reports`, `powerbi_datasets`) —
  snapshot quotidien, 1 ligne = 1 objet du locataire

Source alimentée par l'API d'administration Power BI via dlt
(`ingestion/pipelines/powerbi_activity`). Volume faible : ~2 100 événements pour
27 jours, ~80 événements/jour.

> Voir `docs/pipeline-schedule.md` pour le cron et l'orchestration.

---

## Flux de données

```
┌────────────────────────┐                  ┌──────────────────────────────────┐
│ API admin Power BI     │  dlt             │  prod_raw                        │
│ api.powerbi.com        │ ───────────────► │  powerbi_activity_events (merge) │
│ /v1.0/myorg/admin/     │  elt-powerbi-    │  powerbi_workspaces    (replace) │
│                        │  activity        │  powerbi_reports       (replace) │
│ 4 endpoints            │  7 j/7 à 03:30   │  powerbi_datasets      (replace) │
└────────────────────────┘                  └──────────────┬───────────────────┘
                                                           │ dbt staging
                                                           ▼
                                              (à construire)
```

---

## ⚠️ TROIS FILTRES OBLIGATOIRES au staging

**C'est le point le plus important de ce document.** Le raw est délibérément
fidèle à la source (règle n°3 d'`ingestion/`) : il contient des artefacts
techniques que Power BI génère lui-même. Sans ces filtres, **les chiffres sont
faux, et faux dans le sens alarmiste**.

| Filtre | Table | Pourquoi |
|---|---|---|
| `app_id is null` | `powerbi_reports` | Power BI crée une **copie** de chaque rapport publié dans une App, avec un `id` distinct et un `app_id` renseigné. Les événements `ViewReport` référencent **toujours l'original** du workspace : les copies affichent donc 0 vue et passent pour dormantes. 29 copies sur 75 rapports. |
| `name not ilike '%usage metrics report%'` | `powerbi_reports` | Rapports de métriques d'usage générés automatiquement par Power BI. 10 au 2026-09-03. |
| `type = 'Workspace' and state = 'Active'` | `powerbi_workspaces` | Les espaces personnels portent **DEUX** types distincts (`PersonalGroup` **et** `Personal`) — filtrer sur le seul `PersonalGroup` en laisse passer. Deux espaces sont par ailleurs `Deleted`. |

**L'écart mesuré** : sans les filtres, 55 rapports « jamais consultés » sur 75.
Avec, **17 sur 36**. Les 38 points d'écart sont intégralement des artefacts.

Un premier tableau de bord a été publié avec les mauvais chiffres avant que
l'erreur soit trouvée. Ne pas refaire ce chemin.

**Et pour mesurer l'usage humain : `activity = 'ViewReport'` uniquement.**
`RefreshDataset` domine le volume d'événements (~80 % ) mais c'est du
rafraîchissement automatique, pas de l'usage. Il reste précieux pour autre
chose : c'est un monitoring gratuit de nos propres pipelines, et le croisement
`rapport dormant × modèle rafraîchi` donne le coût de maintien d'un rapport que
personne ne lit.

---

## Grain et clés

| Table | Grain | Clé | Écriture |
|---|---|---|---|
| `powerbi_activity_events` | 1 événement d'audit | `id` (unique, non nul — vérifié : 2 135 lignes / 2 135 ids) | `merge` |
| `powerbi_workspaces` | 1 espace de travail | `id` | `replace` |
| `powerbi_reports` | 1 rapport (copies App incluses) | `id` ; `workspace_id` → workspaces ; `dataset_id` → datasets ; `original_report_object_id` → le rapport source quand `app_id` est renseigné | `replace` |
| `powerbi_datasets` | 1 modèle sémantique | `id` | `replace` |

Jointure des événements vers l'inventaire : `report_id`, `workspace_id`,
`dataset_id`. **Attention**, ces colonnes ne sont renseignées que sur les
événements qui les concernent (12,6 % pour `report_id`) — un `inner join`
écraserait les autres types d'activité.

---

## Points d'attention

**L'API est incohérente sur la casse.** Elle rend `WorkspaceId` mais
`WorkSpaceName`, ce qui donne `workspace_id` face à **`work_space_name`** après
normalisation dlt. Ce n'est pas une faute de frappe du pipeline.

**Trois tables enfants** dérivées de tableaux imbriqués :
`powerbi_activity_events__schedules__days`, `__schedules__time`,
`__models_snapshots`. Faible volume, liées au parent par `_dlt_parent_id` →
`_dlt_id`. Elles n'existent que si un événement de rafraîchissement planifié
apparaît dans la fenêtre — **ne pas construire de modèle qui en dépende sans
gérer leur absence**.

**La forme d'un événement dépend de son type d'activité.** Les colonnes
`report_*`, `app_*`, `distribution_method`, `consumption_method` n'existent que
sur les consultations. Elles sont **épinglées côté extraction** précisément pour
que leur présence dans le schéma soit garantie même sur une journée sans
consultation — le contrat est donc stable, mais les valeurs sont massivement
nulles.

**Rétention de 27 jours à la source, et c'est irréversible.** L'API ne conserve
rien au-delà. `prod_raw` est donc la **seule** archive : un `--full-refresh` sur
cette source détruirait un historique non reconstituable. Interdit ici comme
côté ingestion.

**Freshness dbt applicable**, contrairement à `zoho_desk` : `_extracted_at` est
un vrai `TIMESTAMP` (et non `_dlt_load_id`, string). C'est le `loaded_at_field`
à déclarer.

**Partition et conservation** : `powerbi_activity_events` est partitionnée par
jour sur `creation_time`, avec expiration à **365 jours**. Les modèles aval
gagnent à filtrer sur `creation_time` pour bénéficier de l'élagage de partition.

---

## Données personnelles — contrainte de modélisation

Les événements identifient nominativement l'utilisateur (`user_id`, `user_key`).
Le dispositif relève du **suivi d'activité des salariés**, avec une finalité
déclarée : rationaliser le parc de rapports, **pas** évaluer les personnes.

Conséquence directe sur les marts : **exposer des agrégats, pas des identités.**
Un `count(distinct user_id)` est légitime ; une liste nominative de qui a ouvert
quel rapport ne l'est pas. L'identifiant reste cantonné à `prod_raw`, à accès
restreint.

Voir `ingestion/pipelines/powerbi_activity/HABILITATION.md` pour les mesures
complètes (registre des traitements, information des salariés, arbitrage CSE).

---

## Chiffres de référence — pour valider un modèle

Relevé du 2026-09-03, fenêtre 2026-08-07 → 2026-09-02 (27 jours) :

| Mesure | Valeur |
|---|---|
| Événements | 2 135 (2 135 ids distincts) |
| dont `ViewReport` | 246 |
| Espaces de travail (tous) | 149 — dont **13** partagés actifs |
| Rapports (inventaire brut) | 139 — dont **36** métier après filtres |
| Rapports métier consultés | 19 |
| Rapports métier dormants | **17** |
| Utilisateurs distincts | 32 |
| Rafraîchissements de modèles sans lecteur | 312 |

Un staging correct doit retrouver 36 rapports métier et 13 espaces partagés.
S'il en trouve 75 et 149, les filtres ne sont pas appliqués.

---

## À faire

1. **`_powerbi_activity__sources.yml`** — 4 tables, `loaded_at_field:
   _extracted_at`, freshness applicable. Suivre le pattern de
   `staging/zoho_desk/` (`database: "{{ var('raw_project') }}"`,
   `schema: "{{ var('raw_schema', 'prod_raw') }}"`).
2. **Staging** : `stg_powerbi_activity__events`, `__workspaces`, `__reports`,
   `__datasets`. Les filtres ci-dessus vont ici.
3. **Intermediate / marts** : `dim_powerbi_report`, `fct_powerbi_report_views`,
   et un mart d'usage (vues/jour/rapport, dernière consultation, rapports
   dormants, coût de rafraîchissement des dormants).
4. **Ajouter l'étape dbt au workflow.** `infra/workflows/pipeline-powerbi-activity.yaml`
   n'a **qu'une étape EL** aujourd'hui. Conformément à l'architecture Option C,
   y ajouter `dbt build source:powerbi_activity+` sur le modèle de
   `pipeline-yuman-evs.yaml`.
