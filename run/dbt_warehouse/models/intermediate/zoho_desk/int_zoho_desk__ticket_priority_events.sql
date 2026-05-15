
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_zoho_desk__ticket_priority_events`
      
    partition by event_date_paris
    cluster by ticket_id

    
    OPTIONS(
      description="""Une ligne par changement de priorit\u00e9 du ticket (filtr\u00e9 sur property_name = 'Priority'). Source : stg_zoho_desk__ticket_history \u00d7 stg_zoho_desk__ticket_history_event_info. M\u00eame pattern que ticket_status_events : la priorit\u00e9 est une valeur scalaire, donc on lit prev/new dans property_value__previous_value / __updated_value, avec un fallback sur property_value pour l'attribution initiale. Permet d'analyser les escalades (ex : Low \u2192 High en cours de traitement).\n"""
    )
    as (
      

with history as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__ticket_history`
),

event_info as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__ticket_history_event_info`
),

priority_events as (
    select
        h._zoho_desk_associated_tickets_id as ticket_id,
        h.event_time as event_at,
        date(h.event_time, 'Europe/Paris') as event_date_paris,
        h.event_name,
        h.actor__id as actor_id,
        h.actor__name as actor_name,
        h.actor__type as actor_type,
        ei.property_value__previous_value as prev_priority,
        coalesce(
            ei.property_value__updated_value,
            ei.property_value
        ) as new_priority,
        ei.property_value__previous_value is null
        and ei.property_value__updated_value is null
        and ei.property_value is not null as is_initial_assignment
    from history as h
    inner join event_info as ei
        on h._dlt_id = ei._dlt_parent_id
    where ei.property_name = 'Priority'
)

select * from priority_events
    );
  