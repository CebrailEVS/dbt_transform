
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_zoho_desk__ticket_assignee_events`
      
    partition by event_date_paris
    cluster by ticket_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Une ligne par changement de propri\u00e9taire (agent assign\u00e9) d'un ticket. Permet d'analyser la charge agent dans le temps et le taux de r\u00e9assignation.\n[COMMENT CONSTRUITE] stg_zoho_desk__ticket_history \u00d7 ticket_history_event_info, filtr\u00e9 property_name = 'Case Owner' (nomenclature Zoho h\u00e9rit\u00e9e de Salesforce = \u00ab Propri\u00e9taire du ticket \u00bb dans l'UI). La valeur est un objet (agent id + name) : lecture dans les champs __id / __name, fallback property_value__* pour l'attribution initiale.\n[GRAIN] 1 ligne par \u00e9v\u00e9nement de (r\u00e9)assignation. Depuis f\u00e9vrier 2024.\n[NOTES] \u00ab Case Owner \u00bb = l'agent assign\u00e9 (assignee dans ticket_enriched), pas l'auteur du changement (qui est actor_*).\n"""
    )
    as (
      

with history as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__ticket_history`
),

event_info as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__ticket_history_event_info`
),

assignee_events as (
    select
        h._zoho_desk_associated_tickets_id as ticket_id,
        h.event_time as event_at,
        date(h.event_time, 'Europe/Paris') as event_date_paris,
        h.event_name,
        h.actor__id as actor_id,
        h.actor__name as actor_name,
        h.actor__type as actor_type,
        -- Assignee is stored as an object (id + name), not a scalar.
        -- For transitions, Zoho populates previous_value__id/__name and updated_value__id/__name.
        -- For the initial assignment at creation, only property_value__id/__name is set.
        ei.property_value__previous_value__id as prev_assignee_id,
        ei.property_value__previous_value__name as prev_assignee_name,
        coalesce(
            ei.property_value__updated_value__id,
            ei.property_value__id
        ) as new_assignee_id,
        coalesce(
            ei.property_value__updated_value__name,
            ei.property_value__name
        ) as new_assignee_name,
        ei.property_value__previous_value__id is null
        and ei.property_value__updated_value__id is null
        and ei.property_value__id is not null as is_initial_assignment
    from history as h
    inner join event_info as ei
        on h._dlt_id = ei._dlt_parent_id
    where ei.property_name = 'Case Owner'
)

select * from assignee_events
    );
  