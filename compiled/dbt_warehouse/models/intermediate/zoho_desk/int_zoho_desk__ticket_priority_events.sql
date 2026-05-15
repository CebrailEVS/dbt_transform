

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