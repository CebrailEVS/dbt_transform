# Architecture — Zoho Desk (`zoho_desk`)

> Le scheduler `pipeline_zoho_desk` peut être en pause : son état et sa raison sont dans
> `infra/workflows_el.tf`. Avant un run manuel, vérifier qu'aucun autre n'a tourné dans la
> journée (quota quotidien de l'API).

| | |
|---|---|
| Source dbt | `zoho_desk` (`models/staging/zoho_desk/_zoho_desk__sources.yml`) |
| Pipeline | `ingestion/pipelines/zoho_desk` : API REST Zoho Desk, région EU ; périmètre et modes dans `tables.py` |
| Chargement | `merge` (snapshot accumulé : un ticket sorti de la fenêtre de l'API, ou supprimé dans Zoho, reste dans le raw), sauf l'historique en `replace` (Zoho ne donne pas d'identifiant d'événement : la table ne contient que ce que le dernier run a lu) |

Fraîcheur : [`docs/freshness.md`](../freshness.md). Cadence : [`docs/pipeline-schedule.md`](../pipeline-schedule.md).

Zoho Desk est l'outil de tickets de support. La chaîne reconstruit le cycle de vie des tickets et
des SLA maison à partir de l'historique des événements.

- **Clés de merge** : `id` partout, sauf `ticket_metrics` : `/metrics` ne rend pas d'`id`, la clé
  est `_zoho_desk_associated_tickets_id`, injectée depuis le ticket parent.
- **Sous-tables dlt** (`ticket_history__event_info`, `ticket_metrics__agents_handled`…) : tableaux
  JSON aplatis par dlt ; elles suivent le mode de leur parent.
- **Pas d'incrémental** : `/associatedTickets`, le seul endpoint qui rende tous les tickets, ne
  donne pas `modifiedTime`. Chaque run relit tous les tickets, avec plusieurs appels par ticket.

---

## Clés et grain

- La table des tickets s'appelle `associated_tickets` dans le raw ; le staging l'expose en
  `stg_zoho_desk__tickets` (`ticket_id`).
- `ticket_history` n'a pas d'id source : sa clé de ligne est `_dlt_id`.
- Dans `stg_zoho_desk__ticket_metrics_staging_data`, « staging » désigne une étape de statut Zoho
  (1 statut × ticket, temps passé), pas la couche dbt.

---

## Pièges

### Nom de la clé ticket dans les tables enfants
- **Symptôme** : pas de `ticket_id` sur `ticket_threads` ni `ticket_history`.
- **Règle** : les flux enfants tirent leur ticket de la ressource parente `associated_tickets` :
  la colonne s'appelle `_zoho_desk_associated_tickets_id` partout. Le staging la renomme en
  `ticket_id` sur `ticket_details` et `ticket_metrics`, et la garde telle quelle sur
  `ticket_threads` et `ticket_history`.

### Sous-tables dlt : jointure par `_dlt_id`
- **Règle** : une sous-table se joint à son parent par `_dlt_parent_id = parent._dlt_id`, jamais
  par un identifiant métier (agent ↔ département compris). `_dlt_id` est attribué au chargement :
  clé de jointure, pas un identifiant stable d'un run à l'autre, surtout sur `ticket_history`.

### Historique incomplet, tickets complets
- **Symptôme** : SLA ou événements faux sans qu'aucun test ne casse.
- **Règle** : un run interrompu ou partiel réécrit l'historique avec ce qu'il a lu, sans retirer
  aucun ticket. Avant de lire un SLA, comparer le nombre de tickets distincts de `ticket_history`
  à celui de `stg_zoho_desk__tickets`.

### `closed_time` ne donne pas les fermetures
- **Règle** : il ne porte que la fermeture courante et vaut `NULL` si le ticket a été rouvert.
  Compter fermetures et réouvertures depuis l'historique.
- **Appliqué par** : `int_zoho_desk__ticket_status_events`.

### Bruit des fusions de tickets
- **Symptôme** : des événements qui ne changent aucune valeur.
- **Règle** : une fusion dans l'interface Zoho ré-estampille toutes les propriétés du ticket
  (`event_name = 'TicketMergedMaster'`). Les modèles d'événements exposent `event_name` sans
  filtrer ; tout consommateur doit filtrer.
- **Appliqué par** : `int_zoho_desk__ticket_lifecycle_segments` (`TicketCreated`,
  `TicketUpdated`), fermetures de `int_zoho_desk__ticket_sla` (`TicketUpdated`).

### Valeur avant / après selon le type de propriété
- **Règle** : une propriété scalaire (Status, Priority, Department) se lit dans
  `property_value__previous_value` / `__updated_value` ; une propriété objet (Case Owner) dans
  `…__previous_value__id` / `__name` et `…__updated_value__id` / `__name`. À la création, Zoho met
  parfois la valeur initiale dans `property_value`, avant et après à `NULL`.
- **Appliqué par** : `int_zoho_desk__ticket_status_events` (`is_creation_event`).

### Valeurs de types mélangés
- **Règle** : `updatedValue` porte selon la propriété une date, un booléen ou du texte. Le pipeline
  l'épingle en texte (`colonnes_imbriquees` de `tables.py`) : laissé à l'inférence, dlt range une
  partie des valeurs dans des colonnes variantes que le staging ne lit pas. Caster à l'usage.

### Colonne absente du raw
- **Symptôme** : le staging casse sur `Unrecognized name`.
- **Règle** : dlt ne crée une colonne que si une ligne la renseigne. Toute colonne sélectionnée
  par le staging et peu renseignée est épinglée dans `tables.py` du pipeline.

### Champs personnalisés `cf_*` en `STRING`
- **Règle** : l'API les rend en texte, y compris les dates et les booléens. Le staging ne les
  caste pas ; caster dans le modèle qui les utilise.

### Sous-requête corrélée refusée par BigQuery
- **Symptôme** : un calcul d'heures ouvrées en sous-requête corrélée sur
  `unnest(generate_date_array(...))` n'est pas planifiable à côté d'un `lead()` ou répété dans un
  même `select`.
- **Règle** : `cross join unnest` + `left join` des fériés + `group by`.
- **Appliqué par** : `int_zoho_desk__ticket_lifecycle_segments`, `int_zoho_desk__ticket_sla`.

### Écart d'une minute avec l'interface Zoho
- **Règle** : `timestamp_diff(..., minute)` tronque les secondes. Recalculer en secondes si
  l'écart compte.

---

## Règles métier

- Le type de statut d'un libellé Zoho vient du seed `ref_zoho_desk__status_mapping`
  (`int_zoho_desk__ticket_status_events`).
- Les SLA sont des **règles maison**, non alignées sur le tableau de bord Zoho : heures ouvrées
  (fériés : seed `ref_general__feries_metropole`), première réponse, fermeture et réouverture,
  trois définitions de résolution. Définitions dans le YAML de `int_zoho_desk__ticket_sla`.
  L'interface Zoho appelle « rouvert » tout retour à « Nouveau » : ses compteurs diffèrent.
- Compte d'un ticket : celui du ticket, à défaut celui de son contact
  (`int_zoho_desk__ticket_enriched`).
- Nouveau type d'événement (département, catégorie…) : un modèle intermediate par propriété, sur
  le modèle de `int_zoho_desk__ticket_priority_events`, plutôt qu'une table d'événements générique.

---

## Consommateurs

```bash
dbt ls -s source:zoho_desk+ --resource-type model
```
