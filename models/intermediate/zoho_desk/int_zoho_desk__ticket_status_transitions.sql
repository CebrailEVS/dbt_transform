{{
    config(
        materialized='table',
        partition_by={'field': 'event_date_paris', 'data_type': 'date'},
        cluster_by=['ticket_id'],
        tags=['zoho_desk', 'intermediate']
    )
}}

with created_raw as (
    select
        t.ticket_id,
        h._dlt_id as event_id,
        h.event_time,
        h.event_name,
        h.actor__type as actor_type,
        cast(null as string) as from_status_raw,
        ei.property_value as to_status_raw
    from {{ ref('stg_zoho_desk__ticket_history') }} as h
    inner join {{ ref('stg_zoho_desk__ticket_history_event_info') }} as ei on h._dlt_id = ei._dlt_parent_id
    inner join {{ ref('stg_zoho_desk__tickets') }} as t on h._zoho_desk_associated_tickets_id = t.ticket_id
    where
        h.event_name = 'TicketCreated'
        and ei.property_name = 'Status'
        and ei.property_value is not null
),

updated_raw as (
    select
        t.ticket_id,
        h._dlt_id as event_id,
        h.event_time,
        h.event_name,
        h.actor__type as actor_type,
        ei.property_value__previous_value as from_status_raw,
        ei.property_value__updated_value as to_status_raw
    from {{ ref('stg_zoho_desk__ticket_history') }} as h
    inner join {{ ref('stg_zoho_desk__ticket_history_event_info') }} as ei on h._dlt_id = ei._dlt_parent_id
    inner join {{ ref('stg_zoho_desk__tickets') }} as t on h._zoho_desk_associated_tickets_id = t.ticket_id
    where
        h.event_name = 'TicketUpdated'
        and ei.property_name = 'Status'
        and ei.property_value__previous_value is not null
        and ei.property_value__updated_value is not null
),

all_raw as (
    select * from created_raw
    union all
    select * from updated_raw
),

normalized as (
    -- Map legacy English labels (pre-May 2024) to current French labels
    select
        ticket_id,
        event_id,
        event_time,
        event_name,
        actor_type,
        case from_status_raw
            when 'Open' then 'Nouveau'
            when 'Closed' then 'Clôturée'
            when 'On Hold' then 'En attente infos complémentaires'
            else from_status_raw
        end as from_status,
        case to_status_raw
            when 'Open' then 'Nouveau'
            when 'Closed' then 'Clôturée'
            when 'On Hold' then 'En attente infos complémentaires'
            else to_status_raw
        end as to_status
    from all_raw
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['ticket_id', 'event_id', 'from_status', 'to_status']) }}
            as transition_id,
        ticket_id,
        event_time,
        date(event_time, 'Europe/Paris') as event_date_paris,
        event_name,
        actor_type,
        from_status,
        to_status,
        case
            when event_name = 'TicketCreated' then 'ticket_created'
            when to_status = 'Clôturée' then 'closed'
            when from_status = 'Clôturée' then 'reopened'
            when to_status in ('En attente infos complémentaires', 'En attente interne') then 'on_hold_enter'
            when from_status in ('En attente infos complémentaires', 'En attente interne') then 'on_hold_exit'
            else 'status_change'
        end as transition_type
    from normalized
)

select * from final
