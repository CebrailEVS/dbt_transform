{{
    config(
        materialized='table',
        partition_by={
            'field': 'event_date_paris',
            'data_type': 'date'
        },
        cluster_by=['ticket_id']
    )
}}

with history as (
    select * from {{ ref('stg_zoho_desk__ticket_history') }}
),

event_info as (
    select * from {{ ref('stg_zoho_desk__ticket_history_event_info') }}
),

status_mapping as (
    select * from {{ ref('ref_zoho_desk__status_mapping') }}
),

raw_events as (
    select
        h._zoho_desk_associated_tickets_id as ticket_id,
        h.event_time as event_at,
        date(h.event_time, 'Europe/Paris') as event_date_paris,
        h.event_name,
        h.actor__id as actor_id,
        h.actor__name as actor_name,
        h.actor__type as actor_type,
        ei.property_value__previous_value as prev_status,
        -- creation events have NULL prev/new but the initial status in property_value
        coalesce(
            ei.property_value__updated_value,
            ei.property_value
        ) as new_status,
        ei.property_value__previous_value is null
        and ei.property_value__updated_value is null
        and ei.property_value is not null as is_creation_event
    from history as h
    inner join event_info as ei
        on h._dlt_id = ei._dlt_parent_id
    where ei.property_name = 'Status'
),

normalized as (
    select
        e.ticket_id,
        e.event_at,
        e.event_date_paris,
        e.event_name,
        e.actor_id,
        e.actor_name,
        e.actor_type,

        -- raw status labels (kept for inspection / Zoho UI cross-check)
        e.prev_status,
        e.new_status,
        e.is_creation_event,

        -- normalized attributes from the seed
        prev_map.status_type as prev_status_type,
        prev_map.is_on_hold as prev_is_on_hold,
        prev_map.is_closed as prev_is_closed,

        new_map.status_type as new_status_type,
        new_map.is_on_hold as new_is_on_hold,
        new_map.is_closed as new_is_closed
    from raw_events as e
    left join status_mapping as prev_map
        on e.prev_status = prev_map.status
    left join status_mapping as new_map
        on e.new_status = new_map.status
)

select * from normalized
