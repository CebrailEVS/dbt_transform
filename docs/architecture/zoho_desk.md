# Architecture — Zoho Desk (`zoho_desk`)

> **Pipeline en pause.** Le scheduler `pipeline_zoho_desk` est déclaré `paused = true` dans
> `infra/workflows_el.tf` : un run complet consomme l'essentiel du budget quotidien de crédits
> de l'API Zoho, sans marge pour un second run le même jour. Le raw `prod_raw.zoho_desk_*` est
> donc **figé** à la date du dernier run : tout ce qui suit décrit des données qui ne bougent
> plus, et le test de récence du staging reste en avertissement tant que dure la pause. Avant
> de relancer un run à la main, vérifier qu'aucun autre n'a tourné dans la journée.

| | |
|---|---|
| Source dbt | `zoho_desk` — `models/staging/zoho_desk/_zoho_desk__sources.yml` |
| Pipeline dlt | `ingestion/pipelines/zoho_desk` — API REST Zoho Desk, région EU ; périmètre dans `tables.py` |
| Tables raw | `prod_raw.zoho_desk_*` |
| Fraîcheur | [`docs/freshness.md`](../freshness.md) |
| Cadence | [`docs/pipeline-schedule.md`](../pipeline-schedule.md) ; le workflow enchaîne l'extraction et `dbt build -s source:zoho_desk+` |

**Modes de chargement** (`tables.py`) :

| Mode | Tables raw | Conséquence |
|---|---|---|
| `merge` sur `id` | `accounts`, `agents`, `contacts`, `departments`, `associated_tickets` (la table des tickets), `ticket_details`, `ticket_threads`, `ticket_conversations` | snapshot accumulé : un ticket sorti de la fenêtre de l'API, ou supprimé dans Zoho, reste dans le raw |
| `merge` sur `_zoho_desk_associated_tickets_id` | `ticket_metrics` | `/metrics` ne rend pas d'`id` : la clé est la colonne injectée depuis le ticket parent |
| `replace` | `ticket_history` | Zoho ne donne pas d'identifiant d'événement : rien sur quoi merger. La table ne contient que ce que le dernier run a lu |
| sous-tables dlt | `ticket_history__event_info`, `ticket_metrics__agents_handled`, `ticket_metrics__staging_data`, `agents__associated_department_ids` | tableaux JSON aplatis par dlt ; suivent le mode de leur parent |

Pas d'incrémental : `/associatedTickets` (le seul endpoint qui rende tous les tickets) ne donne
pas `modifiedTime`. Chaque run relit tous les tickets, et fait plusieurs appels par ticket.

---

## Grain et clés

| Modèle de staging | Grain | Clé, jointure |
|---|---|---|
| `stg_zoho_desk__tickets` | 1 ticket | `ticket_id` (`id` renommé) ; FK `department_id`, `assignee_id` → agent, `contact_id`, `account_id` |
| `stg_zoho_desk__ticket_details` | 1 ticket : champs personnalisés `cf_*`, résolution, drapeaux SLA | `ticket_id` |
| `stg_zoho_desk__ticket_metrics` | 1 ticket : durées et compteurs calculés par Zoho | `ticket_id` ; `_dlt_id` vers ses sous-tables |
| `stg_zoho_desk__ticket_metrics_agents_handled` | 1 agent × ticket, temps de traitement | `_dlt_parent_id = ticket_metrics._dlt_id` |
| `stg_zoho_desk__ticket_metrics_staging_data` | 1 statut × ticket, temps passé (« staging » = étape de statut Zoho) | `_dlt_parent_id = ticket_metrics._dlt_id` |
| `stg_zoho_desk__ticket_threads` | 1 échange (e-mail, chat) | `_zoho_desk_associated_tickets_id` → ticket |
| `stg_zoho_desk__ticket_history` | 1 événement d'audit | `_dlt_id` ; `_zoho_desk_associated_tickets_id` → ticket |
| `stg_zoho_desk__ticket_history_event_info` | 1 propriété modifiée par événement | `_dlt_parent_id = ticket_history._dlt_id` |
| `stg_zoho_desk__agents`, `__departments`, `__accounts`, `__contacts` | référentiels | `<entité>_id` |
| `stg_zoho_desk__agent_departments` | pont agent × département | `_dlt_parent_id = agents._dlt_id`, `department_id` |

---

## Pièges

**Nom de la clé ticket dans les tables enfants.** Les flux enfants (`ticket_details`,
`ticket_metrics`, `ticket_threads`, `ticket_history`) tirent leur ticket de la ressource
parente `associated_tickets` : la colonne s'appelle `_zoho_desk_associated_tickets_id`
partout. Le staging la renomme en `ticket_id` sur `ticket_details` et `ticket_metrics`, et la
garde telle quelle sur `ticket_threads` et `ticket_history`.

**Sous-tables dlt : jointure par `_dlt_id`.** Une sous-table se joint à son parent par
`_dlt_parent_id = parent._dlt_id`, jamais par un identifiant métier (agent ↔ département
compris). `_dlt_id` est attribué par dlt au chargement : c'est une clé de jointure, pas un
identifiant à conserver d'un run à l'autre, surtout sur `ticket_history`, réécrite à chaque run.

**Historique incomplet, tickets complets.** `ticket_history` est en `replace`, les tickets en
`merge`. Un run interrompu ou partiel réécrit l'historique avec ce qu'il a lu, sans retirer
aucun ticket : les modèles d'événements et de SLA sont alors faux sans qu'aucun test ne casse.
Règle : avant de lire un SLA, comparer le nombre de tickets distincts de `ticket_history` à
celui de `stg_zoho_desk__tickets`.

**`closed_time` ne donne pas les fermetures.** Il ne porte que la fermeture courante et vaut
`NULL` si le ticket a été rouvert. Règle : compter fermetures et réouvertures depuis
l'historique, via `int_zoho_desk__ticket_status_events`.

**Bruit des fusions de tickets.** Une fusion dans l'interface Zoho ré-estampille toutes les
propriétés du ticket (`event_name = 'TicketMergedMaster'`) sans changer de valeur. Règle : les
modèles d'événements exposent `event_name` sans filtrer ; `int_zoho_desk__ticket_lifecycle_segments`
ne garde que `TicketCreated` et `TicketUpdated`, et les fermetures de `int_zoho_desk__ticket_sla`
que `TicketUpdated`. Tout nouveau consommateur doit filtrer de même.

**Valeur avant / après selon le type de propriété.** Une propriété scalaire (Status, Priority,
Department) se lit dans `property_value__previous_value` / `__updated_value` ; une propriété
objet (Case Owner) dans `…__previous_value__id` / `__name` et `…__updated_value__id` / `__name`.
À la création, Zoho met parfois la valeur initiale dans `property_value` avec avant et après à
`NULL` : `is_creation_event` dans `int_zoho_desk__ticket_status_events`.

**Valeurs de types mélangés.** `updatedValue` porte selon la propriété une date, un booléen ou
du texte. Le pipeline l'épingle en texte (`colonnes_imbriquees` de `tables.py`) : laissé à
l'inférence, dlt range une partie des valeurs dans des colonnes variantes que le staging ne lit
pas. Toute valeur arrive en chaîne ; caster au moment de l'usage.

**Colonne absente du raw.** dlt ne crée une colonne que si une ligne la renseigne. Une colonne
rarement remplie peut disparaître d'un run, et le staging qui la sélectionne casse
(`Unrecognized name`). Règle : toute colonne sélectionnée par le staging et peu renseignée est
épinglée dans `tables.py` du pipeline.

**Champs personnalisés `cf_*` en `STRING`.** L'API les rend en texte, y compris les dates et
les booléens. Le staging ne les caste pas ; caster dans le modèle qui les utilise.

**Sous-requête corrélée refusée par BigQuery.** Un calcul d'heures ouvrées en sous-requête
corrélée sur `unnest(generate_date_array(...))` n'est pas planifiable à côté d'un `lead()` ou
répété dans un même `select`. Règle : `cross join unnest` + `left join` des fériés +
`group by`, comme dans `int_zoho_desk__ticket_lifecycle_segments` et `int_zoho_desk__ticket_sla`.

**Écart d'une minute avec l'interface Zoho.** `timestamp_diff(..., minute)` tronque les
secondes. Recalculer en secondes si l'écart compte.

---

## Règles métier et leur source

| Règle | Source | Appliquée dans |
|---|---|---|
| Type de statut (`Open`, `On Hold`, fermé) d'un libellé Zoho | seed `ref_zoho_desk__status_mapping` | `int_zoho_desk__ticket_status_events` |
| Heures ouvrées : lundi–vendredi, 9 h 00–17 h 30 heure de Paris, hors jours fériés | seed `ref_general__feries_metropole` | `int_zoho_desk__ticket_lifecycle_segments`, `int_zoho_desk__ticket_sla` |
| Première réponse : premier échange sortant d'un agent, hors échange de description | choix interne | `int_zoho_desk__ticket_sla` |
| Fermeture : passage d'un statut non fermé à un statut fermé ; réouverture : passage d'un statut fermé à un statut non fermé. L'interface Zoho appelle « rouvert » tout retour à « Nouveau » : ses compteurs diffèrent | choix interne | `int_zoho_desk__ticket_sla` |
| Délai de résolution calculé deux fois, à la première et à la dernière fermeture, en minutes calendaires et ouvrées ; temps en attente isolé | choix interne, non aligné sur le tableau de bord Zoho | `int_zoho_desk__ticket_sla` |
| Compte d'un ticket : celui du ticket, à défaut celui de son contact | choix interne | `int_zoho_desk__ticket_enriched` |

Pour ajouter un type d'événement (département, catégorie…) : un modèle intermediate par
propriété, sur le modèle de `int_zoho_desk__ticket_priority_events`, plutôt qu'une table
d'événements générique.

---

## Consommateurs

La chaîne s'arrête à l'intermediate : aucun mart ni application ne lit cette source. Liste à
jour :

```bash
dbt ls -s source:zoho_desk+ --resource-type model
```
