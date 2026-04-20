# Architecture — Zoho Desk

> Dernière mise à jour : 2026-04-20

---

## Vue d'ensemble

Zoho Desk est le logiciel de support client utilisé pour gérer les tickets entrants
(email, téléphone, chat, formulaire web). Ce pipeline extrait les données de l'API
Zoho Desk (EU region) et les rend disponibles dans BigQuery pour l'analyse.

---

## Flux de données

```
┌─────────────────┐     dlt (Python)     ┌──────────────────────┐
│   API Zoho Desk │ ──────────────────►  │  prod_raw (BigQuery)  │
│   EU region     │   extraction full    │  zoho_desk_*          │
└─────────────────┘   write_disposition  └──────────┬───────────┘
                        = merge/replace             │
                                                    │ dbt staging
                                                    ▼
                                       ┌──────────────────────────┐
                                       │  staging (BigQuery)       │
                                       │  stg_zoho_desk__*         │
                                       │  matérialisé en TABLE     │
                                       └──────────────────────────┘
```

**Ce que fait chaque couche :**

| Couche | Rôle | Localisation |
|---|---|---|
| `prod_raw` | Données brutes telles que reçues de l'API — aucune transformation | `evs-datastack-prod.prod_raw` |
| `staging` | Nettoyage, renommage des PKs, cast des types, documentation | `evs-datastack-prod.staging` |
| `marts` *(à venir)* | Métriques et dimensions BI-ready | `evs-datastack-prod.marts` |

---

## Modèle de données

### Diagramme des relations

```mermaid
erDiagram

    stg_zoho_desk__departments {
        string department_id PK
        string name
        boolean is_enabled
    }

    stg_zoho_desk__agents {
        string agent_id PK
        string _dlt_id
        string email_id
        string status
    }

    stg_zoho_desk__accounts {
        string account_id PK
        string account_name
    }

    stg_zoho_desk__contacts {
        string contact_id PK
        string email
        string account_id FK
    }

    stg_zoho_desk__agent_departments {
        string _dlt_id PK
        string _dlt_parent_id FK
        string department_id FK
    }

    stg_zoho_desk__tickets {
        string ticket_id PK
        string department_id FK
        string assignee_id FK
        string contact_id FK
        string account_id FK
        string status
        timestamp created_time
        timestamp closed_time
    }

    stg_zoho_desk__ticket_details {
        string ticket_id PK
        boolean is_over_due
        boolean is_escalated
        string sla_id
    }

    stg_zoho_desk__ticket_history {
        string _dlt_id PK
        string _zoho_desk_associated_tickets_id FK
        string event_name
        timestamp event_time
    }

    stg_zoho_desk__ticket_history_event_info {
        string _dlt_id PK
        string _dlt_parent_id FK
        string property_name
        string property_value__previous_value
        string property_value__updated_value
    }

    stg_zoho_desk__ticket_metrics {
        string ticket_id PK
        string _dlt_id
        int first_response_time_minutes
        int resolution_time_minutes
        int reopen_count
        int reassign_count
    }

    stg_zoho_desk__ticket_metrics_agents_handled {
        string _dlt_id PK
        string _dlt_parent_id FK
        string agent_id
        string agent_name
        int handling_time_minutes
    }

    stg_zoho_desk__ticket_metrics_staging_data {
        string _dlt_id PK
        string _dlt_parent_id FK
        string status
        int handled_time_minutes
    }

    %% Dimensions → Tickets
    stg_zoho_desk__tickets }o--|| stg_zoho_desk__departments : "department_id"
    stg_zoho_desk__tickets }o--|| stg_zoho_desk__agents : "assignee_id → agent_id"
    stg_zoho_desk__tickets }o--|| stg_zoho_desk__contacts : "contact_id"
    stg_zoho_desk__tickets }o--|| stg_zoho_desk__accounts : "account_id"

    %% Contact → Account
    stg_zoho_desk__contacts }o--|| stg_zoho_desk__accounts : "account_id"

    %% Tickets → enrichissements 1:1
    stg_zoho_desk__tickets ||--|| stg_zoho_desk__ticket_details : "ticket_id"

    %% Tickets → historique 1:N
    stg_zoho_desk__tickets ||--o{ stg_zoho_desk__ticket_history : "ticket_id"

    %% Historique → détail événement 1:N
    stg_zoho_desk__ticket_history ||--o{ stg_zoho_desk__ticket_history_event_info : "_dlt_id"

    %% Tickets → métriques 1:1
    stg_zoho_desk__tickets ||--|| stg_zoho_desk__ticket_metrics : "ticket_id"

    %% Métriques → sous-tables 1:N
    stg_zoho_desk__ticket_metrics ||--o{ stg_zoho_desk__ticket_metrics_agents_handled : "_dlt_id"
    stg_zoho_desk__ticket_metrics ||--o{ stg_zoho_desk__ticket_metrics_staging_data : "_dlt_id"

    %% Agent → départements (bridge)
    stg_zoho_desk__agents ||--o{ stg_zoho_desk__agent_departments : "_dlt_id"
    stg_zoho_desk__departments ||--o{ stg_zoho_desk__agent_departments : "department_id"
```

---

## Rôle de chaque table

### Dimensions — les référentiels

| Table | Ce qu'elle contient | Lignes (~) |
|---|---|---|
| `stg_zoho_desk__accounts` | Entreprises clientes | 251 |
| `stg_zoho_desk__contacts` | Personnes physiques (clients) liées à un compte | 3 362 |
| `stg_zoho_desk__agents` | Membres de l'équipe support | 5 |
| `stg_zoho_desk__departments` | Groupes organisationnels (ex : Service Client, Facturation) | 4 |
| `stg_zoho_desk__agent_departments` | Table pont agent ↔ département (un agent peut appartenir à plusieurs départements) | 11 |

### Facts — les événements

| Table | Ce qu'elle contient | Lignes (~) |
|---|---|---|
| `stg_zoho_desk__tickets` | **Table centrale** — un ticket par ligne, avec tous ses attributs courants | 20 678 |
| `stg_zoho_desk__ticket_details` | Enrichissement 1:1 : flags SLA, champs custom (`cf_*`), résolution | 20 678 |
| `stg_zoho_desk__ticket_metrics` | Enrichissement 1:1 : durées SLA et compteurs (réponse, réouverture, réassignation) | 20 678 |
| `stg_zoho_desk__ticket_history` | Journal d'audit : un événement par ligne (création, changement de statut, etc.) | 259 677 |
| `stg_zoho_desk__ticket_history_event_info` | Détail de chaque événement : champ modifié, valeur avant, valeur après | 823 452 |
| `stg_zoho_desk__ticket_metrics_agents_handled` | Agents ayant traité chaque ticket avec leur temps de traitement individuel | 33 608 |
| `stg_zoho_desk__ticket_metrics_staging_data` | Temps passé par chaque ticket dans chaque statut Zoho ("staging" = étape de statut) | 46 663 |

---

## Jointures clés

### Cas d'usage typiques

**Ticket avec ses dimensions :**
```sql
select
    t.ticket_id,
    t.ticket_number,
    t.status,
    t.created_time,
    d.name          as department,
    a.name          as agent_name,
    c.email         as contact_email,
    acc.account_name
from stg_zoho_desk__tickets        t
left join stg_zoho_desk__departments   d   on d.department_id = t.department_id
left join stg_zoho_desk__agents        a   on a.agent_id      = t.assignee_id
left join stg_zoho_desk__contacts      c   on c.contact_id    = t.contact_id
left join stg_zoho_desk__accounts      acc on acc.account_id  = t.account_id
```

**Tickets fermés en un mois donné** *(via l'historique — ne pas utiliser `closed_time`)* :
```sql
select
    t.ticket_id,
    h.event_time as closed_at
from stg_zoho_desk__ticket_history           h
join stg_zoho_desk__tickets                  t  on t.ticket_id = h._zoho_desk_associated_tickets_id
join stg_zoho_desk__ticket_history_event_info ei on ei._dlt_parent_id = h._dlt_id
where ei.property_name                 = 'Status'
  and ei.property_value__updated_value = 'Closed'
  and date_trunc(h.event_time, month)  = '2026-03-01'
```

**Départements d'un agent :**
```sql
select
    a.agent_id,
    a.name    as agent_name,
    d.name    as department_name
from stg_zoho_desk__agents             a
join stg_zoho_desk__agent_departments  ad on ad._dlt_parent_id = a._dlt_id
join stg_zoho_desk__departments        d  on d.department_id   = ad.department_id
```

---

## Points d'attention

### `closed_time` ne suffit pas pour les métriques temporelles
`closed_time` sur `stg_zoho_desk__tickets` ne contient que **la fermeture la plus récente**
et est `NULL` si le ticket a été rouvert. Pour compter les tickets fermés par mois,
utiliser `stg_zoho_desk__ticket_history` filtré sur `property_name = 'Status'`
et `property_value__updated_value = 'Closed'`.

### La jointure agent ↔ département passe par `_dlt_id`, pas `agent_id`
`stg_zoho_desk__agent_departments` est une sous-table générée par dlt à partir
d'un tableau JSON. La jointure vers l'agent se fait via la clé interne dlt :
`agent_departments._dlt_parent_id = agents._dlt_id` — **pas** via `agent_id`.

### `ticket_details._zoho_desk_tickets_id` renommé en `ticket_id`
Dans la source brute, la FK de `ticket_details` se nomme `_zoho_desk_tickets_id`
(et non `_zoho_desk_associated_tickets_id` comme on pourrait s'y attendre).
C'est un effet de bord du nommage interne du pipeline dlt.
Dans le staging, cette colonne est renommée en `ticket_id` pour la cohérence.

### Les champs custom `cf_*` sont tous en `STRING`
Même les champs qui contiennent des dates ou des booléens — c'est ainsi que
l'API Zoho les retourne. Caster dans les modèles marts si nécessaire.
