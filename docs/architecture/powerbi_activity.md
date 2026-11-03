# Architecture — Power BI activity

| | |
|---|---|
| Source dbt | `powerbi_activity` (`models/staging/powerbi_activity/_powerbi_activity__sources.yml`) |
| Pipeline | `ingestion/pipelines/powerbi_activity` : API d'administration Power BI |
| Chargement | événements : `merge` sur `id`, le raw est la seule archive au-delà de la rétention de l'API ; inventaire (espaces, rapports, modèles sémantiques) : `replace`, état du jour, un objet supprimé disparaît |

Fraîcheur : [`docs/freshness.md`](../freshness.md). Cadence : [`docs/pipeline-schedule.md`](../pipeline-schedule.md).

La source expose les **journaux d'administration du locataire Power BI** (qui a consulté quel
rapport, quand) et l'inventaire des espaces, rapports et modèles sémantiques. Finalité : piloter
l'usage réel du parc (rapports utiles, dormants, coût de rafraîchissement d'un rapport que
personne n'ouvre). Les marts de la BU `bi` lisent le staging sans couche intermediate ; la BU
`bi` est de la télémétrie de la plateforme BI, pas un domaine métier.

---

## Clés et jointures

- `powerbi_reports` contient les copies d'App : `original_report_object_id` pointe vers
  l'original quand `app_id` est renseigné.
- Événements → inventaire par `report_id`, `workspace_id`, `dataset_id`. Ces colonnes ne sont
  renseignées que sur les événements concernés : un `inner join` écraserait les autres types
  d'activité.

---

## Trois filtres obligatoires

Le raw est fidèle à la source et contient des artefacts que Power BI génère lui-même. Sans ces
filtres, les chiffres sont faux, **dans le sens alarmiste** (des rapports actifs paraissent
jamais consultés).

| n° | Filtre | Table | Pourquoi |
|---|---|---|---|
| 1 | `app_id is null` | `powerbi_reports` | Power BI crée une **copie** de chaque rapport publié dans une App, avec un `id` distinct. Les événements `ViewReport` référencent **toujours l'original** : les copies affichent 0 vue et passent pour dormantes. |
| 2 | `lower(name) not like '%usage metrics report%'` | `powerbi_reports` | Rapports de métriques d'usage générés automatiquement. |
| 3 | `type = 'Workspace' and state = 'Active'` | `powerbi_workspaces` | Les espaces personnels portent **deux** types (`PersonalGroup` **et** `Personal`) : filtrer sur un seul en laisse passer. Des espaces supprimés restent listés. |

- Les trois filtres sont appliqués au staging : n°1 et n°2 dans `stg_powerbi_activity__reports`
  (prouvés par des tests `expression_is_true`), n°3 dans `stg_powerbi_activity__workspaces`
  (prouvé par des `accepted_values` sur `type` et `state`).
- Le périmètre « rapports métier » exige en plus la restriction aux espaces partagés actifs.
  Cette jointure est interdite en staging ; elle se fait dans `dim_bi__rapport`.
- Usage humain : `activity = 'ViewReport'` uniquement, appliqué en marts (le staging garde toutes
  les activités). `RefreshDataset` est du rafraîchissement automatique ; il sert au coût de
  maintien d'un rapport dormant.
- Le parc est en 1:1 rapport / modèle sémantique, ce qui permet de rattacher au rapport les
  rafraîchissements portés par le modèle sans double comptage. Le test `unique` sur
  `dim_bi__rapport.dataset_id` casse avant que les mesures soient faussées.

---

## Pièges

### Casse incohérente de l'API
- **Symptôme** : `workspace_id` d'un côté, `work_space_name` de l'autre.
- **Règle** : l'API rend `WorkspaceId` mais `WorkSpaceName` ; ce n'est pas une faute de frappe
  du pipeline.

### Tables enfants conditionnelles
- **Symptôme** : un `source()` sur `powerbi_activity_events__schedules__days`,
  `__schedules__time` ou `__models_snapshots` casse le build.
- **Règle** : ces tables dérivent de tableaux imbriqués et n'existent que si un rafraîchissement
  planifié apparaît dans la fenêtre. Elles ne sont pas déclarées en source ; ne pas en dépendre
  sans gérer leur absence.
- **Appliqué par** : `_powerbi_activity__sources.yml` (non déclarées, raison en commentaire).

### La forme d'un événement dépend de son activité
- **Symptôme** : `report_*`, `app_*`, `distribution_method`, `consumption_method` massivement nuls.
- **Règle** : ces colonnes ne concernent que les consultations. Elles sont épinglées côté
  extraction (`tables.py`), donc présentes dans le schéma même un jour sans consultation.

### Rétention de 27 jours à la source
- **Symptôme** : impossible de recharger un historique.
- **Règle** : l'API ne rend rien au-delà de 27 jours. `prod_raw` est la **seule** archive :
  effacer ou recharger la table raw perd un historique non reconstituable. Le `--full-refresh`
  du pipeline est refusé en prod (`ingestion/pipelines/powerbi_activity/CLAUDE.md`).

### Partition et expiration
- **Règle** : la table raw des événements est partitionnée par jour sur `creation_time`, avec
  expiration (`tables.py`) ; le staging est partitionné sur `created_at` (le même instant
  renommé). Filtrer sur `created_at` dans les modèles aval pour bénéficier de l'élagage.
- **Appliqué par** : `stg_powerbi_activity__events`.

---

## Données personnelles

Les événements identifient nommément l'utilisateur (`user_id`, `user_key`). Le dispositif relève
du **suivi d'activité des salariés**, avec une finalité déclarée : rationaliser le parc de
rapports, **pas** évaluer les personnes. Cette finalité délimite les usages légitimes.

- **Un seul mart nominatif** : `fct_bi__consultation`. Les autres faits agrègent
  (`count(distinct user_id)`), et `dim_bi__rapport` ne porte que les auteurs des objets. Le
  détail par mart est dans l'en-tête de `models/marts/bi/_bi__marts_models.yml`.
- `client_ip` et `user_agent` restent au staging, jamais en marts. Pour segmenter par entité,
  `user_domain` suffit.
- Tout accès Power BI à `fct_bi__consultation` suppose que les formalités (registre des
  traitements, information des salariés) soient validées avec le DSI/DPO. Voir
  `ingestion/pipelines/powerbi_activity/HABILITATION.md`.

---

## Consommateurs

```bash
dbt ls -s source:powerbi_activity+
```
