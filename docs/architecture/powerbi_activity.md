# Architecture — Power BI activity

| | |
|---|---|
| Source dbt | `powerbi_activity` (`models/staging/powerbi_activity/_powerbi_activity__sources.yml`) |
| Pipeline dlt | `powerbi_activity` (`ingestion/pipelines/powerbi_activity`) |
| Tables raw | `powerbi_activity_events`, `powerbi_workspaces`, `powerbi_reports`, `powerbi_datasets` |
| Chargement | `events` : `merge` sur `id` ; inventaire (`workspaces`, `reports`, `datasets`) : `replace` |

Fraîcheur : `docs/freshness.md`. Cadence et orchestration : `docs/pipeline-schedule.md`.

La source expose les **journaux d'administration du locataire Power BI** (qui a
consulté quel rapport, quand) et l'inventaire des espaces, rapports et modèles
sémantiques. Objectif : piloter l'usage réel du parc de rapports (lesquels
servent, lesquels sont dormants, quel coût de rafraîchissement porte un rapport
que personne n'ouvre).

Chaîne : `prod_raw` → 4 modèles `stg_powerbi_activity__*` → marts de la BU `bi`
(`dim_bi__rapport`, `fct_bi__activite_rapport_jour`, `fct_bi__usage_rapport`,
`fct_bi__consultation`). Pas de couche intermediate. La BU `bi` est de la
télémétrie de la plateforme BI, pas un domaine métier.

---

## Grain et clés

| Table | Grain | Clé |
|---|---|---|
| `powerbi_activity_events` | 1 événement d'audit | `id` (GUID du journal unifié) |
| `powerbi_workspaces` | 1 espace de travail | `id` |
| `powerbi_reports` | 1 rapport, copies d'App incluses | `id` ; `original_report_object_id` pointe vers l'original quand `app_id` est renseigné |
| `powerbi_datasets` | 1 modèle sémantique | `id` |

Jointure des événements vers l'inventaire : `report_id`, `workspace_id`,
`dataset_id`. Ces colonnes ne sont renseignées que sur les événements concernés :
un `inner join` écraserait les autres types d'activité.

---

## Trois filtres obligatoires

Le raw est fidèle à la source et contient des artefacts que Power BI génère lui-même.
Sans ces filtres, les chiffres sont faux, **dans le sens alarmiste** (des rapports
actifs paraissent jamais consultés).

| Filtre | Table | Pourquoi |
|---|---|---|
| `app_id is null` | `powerbi_reports` | Power BI crée une **copie** de chaque rapport publié dans une App, avec un `id` distinct. Les événements `ViewReport` référencent **toujours l'original** : les copies affichent 0 vue et passent pour dormantes. |
| `name not ilike '%usage metrics report%'` | `powerbi_reports` | Rapports de métriques d'usage générés automatiquement. |
| `type = 'Workspace' and state = 'Active'` | `powerbi_workspaces` | Les espaces personnels portent **deux** types (`PersonalGroup` **et** `Personal`) : filtrer sur un seul en laisse passer. Des espaces supprimés restent listés. |

- Les deux filtres sur `reports` et celui sur `workspaces` sont appliqués au
  staging, avec un test `expression_is_true`.
- Le périmètre « rapports métier » n'est **pas** atteint par le staging seul : il
  exige en plus la restriction aux espaces partagés actifs. Cette jointure est
  interdite en staging ; elle se fait dans `dim_bi__rapport`.
- Pour mesurer l'usage humain : `activity = 'ViewReport'` uniquement, appliqué
  en marts (le staging garde toutes les activités). `RefreshDataset` domine le
  volume mais c'est du rafraîchissement automatique ; il sert au coût de
  maintien d'un rapport dormant.
- Le parc est en 1:1 rapport / modèle sémantique, ce qui permet de rattacher au
  rapport les rafraîchissements portés par le modèle sans double comptage. Un
  test `unique` sur `dim_bi__rapport.dataset_id` casse avant que les mesures
  soient faussées.

---

## Pièges

### Casse incohérente de l'API
- **Symptôme** : `workspace_id` d'un côté, `work_space_name` de l'autre.
- **Règle** : l'API rend `WorkspaceId` mais `WorkSpaceName` ; ce n'est pas une
  faute de frappe du pipeline.

### Tables enfants conditionnelles
- **Symptôme** : un `source()` sur `powerbi_activity_events__schedules__days`,
  `__schedules__time` ou `__models_snapshots` casse le build.
- **Règle** : ces tables dérivent de tableaux imbriqués et n'existent que si un
  rafraîchissement planifié apparaît dans la fenêtre. Elles ne sont pas déclarées
  en source ; ne pas en dépendre sans gérer leur absence.

### La forme d'un événement dépend de son activité
- **Règle** : les colonnes `report_*`, `app_*`, `distribution_method`,
  `consumption_method` ne concernent que les consultations ; leurs valeurs sont
  massivement nulles. Elles sont épinglées côté extraction, donc présentes dans
  le schéma même un jour sans consultation.

### Rétention de 27 jours à la source
- **Symptôme** : impossible de recharger un historique.
- **Règle** : l'API ne conserve rien au-delà de 27 jours. `prod_raw` est la
  **seule** archive : un `--full-refresh` détruirait un historique non
  reconstituable. Interdit ici comme côté ingestion.

### Partition et expiration
- **Règle** : `powerbi_activity_events` est partitionnée par jour sur
  `creation_time`, avec expiration. Filtrer sur `creation_time` dans les modèles
  aval pour bénéficier de l'élagage.

### Freshness applicable
- **Règle** : `_extracted_at` est un vrai `TIMESTAMP` : c'est le `loaded_at_field`
  de la source (contrairement à `zoho_desk`, dont la colonne est une chaîne).

---

## Données personnelles

Les événements identifient nommément l'utilisateur (`user_id`, `user_key`). Le
dispositif relève du **suivi d'activité des salariés**, avec une finalité
déclarée : rationaliser le parc de rapports, **pas** évaluer les personnes. Cette
finalité délimite les usages légitimes.

| Mart | Nominatif ? |
|---|---|
| `fct_bi__consultation` | Oui : `user_id` en clair, 1 ligne = une personne a ouvert un rapport un jour donné |
| `fct_bi__activite_rapport_jour` | Non : `count(distinct user_id)` |
| `fct_bi__usage_rapport` | Non : `count(distinct user_id)` |
| `dim_bi__rapport` | `created_by` / `modified_by` = UPN d'**auteur** (métadonnée d'objet, pas de consommation) |

- `client_ip` et `user_agent` ne sont **pas** remontés en marts. Ils restent dans
  `stg_powerbi_activity__events`.
- `user_domain` permet une segmentation par entité sans désigner personne.
- L'exposition de `fct_bi__consultation` (journal nominatif d'usage) suppose que
  le registre des traitements et l'information des salariés soient faits : à
  valider avec le DSI/DPO avant tout accès Power BI. Voir
  `ingestion/pipelines/powerbi_activity/HABILITATION.md`.

---

## Consommateurs

```bash
dbt ls -s source:powerbi_activity+
```
