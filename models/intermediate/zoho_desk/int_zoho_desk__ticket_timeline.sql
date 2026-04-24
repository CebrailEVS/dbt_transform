{{
    config(
        materialized='table',
        cluster_by=['ticket_id'],
        tags=['zoho_desk', 'intermediate']
    )
}}

-- First time an agent handled the ticket — matches Zoho's `first_response_time_minutes`
-- semantics. Zoho attributes workflow-driven field updates to the current owner, so a
-- raw "agent TicketUpdated" isn't proof of human action. We keep only TicketUpdated
-- events that actually modified a human-meaningful field (Status, Category, Priority,
-- ownership, department). ThreadAdded and CommentAdded always count.
with meaningful_agent_events as (
    select distinct
        h._zoho_desk_associated_tickets_id as ticket_id,
        h.event_time
    from {{ ref('stg_zoho_desk__ticket_history') }} as h
    left join {{ ref('stg_zoho_desk__ticket_history_event_info') }} as ei
        on
            h._dlt_id = ei._dlt_parent_id
            and ei.property_name in (
                'Status', 'Category', 'Sub Category',
                'Priority', 'Case Owner', 'Assignee', 'Department'
            )
    where
        h.actor__type = 'Agent'
        and (
            h.event_name in ('ThreadAdded', 'CommentAdded')
            or (h.event_name = 'TicketUpdated' and ei.property_name is not null)
        )
),

first_handling as (
    select
        ticket_id,
        min(event_time) as first_response_at
    from meaningful_agent_events
    group by ticket_id
),

final as (
    select
        t.ticket_id,
        fh.first_response_at,
        -- Zoho's pre-computed business-hours metrics, sourced directly from ticket_metrics.
        -- Validated against the Zoho web app export — these match the "dans les heures
        -- d'ouverture" columns exactly, so we stop recomputing them.
        tm.first_response_time_minutes,
        tm.resolution_time_minutes,
        tm.total_response_time_minutes
    from {{ ref('stg_zoho_desk__tickets') }} as t
    left join first_handling as fh on t.ticket_id = fh.ticket_id
    left join {{ ref('stg_zoho_desk__ticket_metrics') }} as tm on t.ticket_id = tm.ticket_id
)

select * from final
