{{
    config(
        materialized='table',
        cluster_by=['ticket_id'],
        tags=['zoho_desk', 'intermediate']
    )
}}

with thread_events as (
    -- Pivot ThreadAdded properties per event to filter on all three conditions at once
    select
        h._zoho_desk_associated_tickets_id as ticket_id,
        h.event_time,
        max(case when ei.property_name = 'ThreadType' then ei.property_value end) as thread_type,
        max(case when ei.property_name = 'Direction' then ei.property_value end) as direction,
        max(case when ei.property_name = 'SendStatus' then ei.property_value end) as send_status
    from {{ ref('stg_zoho_desk__ticket_history') }} as h
    inner join {{ ref('stg_zoho_desk__ticket_history_event_info') }} as ei
        on h._dlt_id = ei._dlt_parent_id
    where
        h.event_name = 'ThreadAdded'
        and h.actor__type = 'Agent'
    group by h._zoho_desk_associated_tickets_id, h.event_time
),

first_reply as (
    -- First outbound sent reply by an agent — excludes drafts (18k), forwards (1.3k)
    select
        ticket_id,
        min(event_time) as first_response_at
    from thread_events
    where
        thread_type = 'ReplyThread'
        and direction = 'Outgoing'
        and send_status = 'Success'
    group by ticket_id
),

transitions_with_next as (
    -- Attach the next event timestamp per ticket — used to close each status window
    select
        ticket_id,
        event_time,
        transition_type,
        lead(event_time) over (partition by ticket_id order by event_time) as next_event_time
    from {{ ref('int_zoho_desk__ticket_status_transitions') }}
),

-- Intermediate Clôturée windows: only closes followed by a reopen
-- (the final close has next_event_time is null → excluded)
closed_windows as (
    select
        ticket_id,
        event_time as closed_at,
        next_event_time as reopened_at
    from transitions_with_next
    where
        transition_type = 'closed'
        and next_event_time is not null
),

-- Business minutes in intermediate Clôturée periods — subtracted from resolution
paused_per_ticket as (
    select
        cw.ticket_id,
        sum(greatest(
            timestamp_diff(
                least(timestamp(datetime(d, time(17, 30, 0)), 'Europe/Paris'), cw.reopened_at),
                greatest(timestamp(datetime(d, time(9, 0, 0)), 'Europe/Paris'), cw.closed_at),
                minute
            ), 0)
        ) as paused_business_minutes
    from closed_windows as cw
    cross join unnest(generate_date_array(
        date(cw.closed_at, 'Europe/Paris'),
        date(cw.reopened_at, 'Europe/Paris')
    )) as d
    where
        extract(dayofweek from d) not in (1, 7)
        and d not in (
            select cast(date_ferie as date)
            from {{ ref('ref_general__feries_metropole') }}
        )
    group by cw.ticket_id
),

-- On-hold windows: each on_hold_enter lasts until the next transition
-- Still-open on-hold tickets: use current_timestamp() as the window end
on_hold_windows as (
    select
        ticket_id,
        event_time as hold_start,
        coalesce(next_event_time, current_timestamp()) as hold_end
    from transitions_with_next
    where transition_type = 'on_hold_enter'
),

-- Business minutes in on-hold periods
-- Used for SLA compliance: elapsed ≤ sla_threshold + on_hold_business_minutes
on_hold_per_ticket as (
    select
        ohw.ticket_id,
        sum(greatest(
            timestamp_diff(
                least(timestamp(datetime(d, time(17, 30, 0)), 'Europe/Paris'), ohw.hold_end),
                greatest(timestamp(datetime(d, time(9, 0, 0)), 'Europe/Paris'), ohw.hold_start),
                minute
            ), 0)
        ) as on_hold_business_minutes
    from on_hold_windows as ohw
    cross join unnest(generate_date_array(
        date(ohw.hold_start, 'Europe/Paris'),
        date(ohw.hold_end, 'Europe/Paris')
    )) as d
    where
        extract(dayofweek from d) not in (1, 7)
        and d not in (
            select cast(date_ferie as date)
            from {{ ref('ref_general__feries_metropole') }}
        )
    group by ohw.ticket_id
),

-- Business minutes from creation to first reply (only for tickets with a sent reply)
first_response_per_ticket as (
    select
        t.ticket_id,
        sum(greatest(
            timestamp_diff(
                least(timestamp(datetime(d, time(17, 30, 0)), 'Europe/Paris'), fr.first_response_at),
                greatest(timestamp(datetime(d, time(9, 0, 0)), 'Europe/Paris'), t.created_time),
                minute
            ), 0)
        ) as first_response_business_minutes
    from {{ ref('stg_zoho_desk__tickets') }} as t
    inner join first_reply as fr on t.ticket_id = fr.ticket_id
    cross join unnest(generate_date_array(
        date(t.created_time, 'Europe/Paris'),
        date(fr.first_response_at, 'Europe/Paris')
    )) as d
    where
        extract(dayofweek from d) not in (1, 7)
        and d not in (
            select cast(date_ferie as date)
            from {{ ref('ref_general__feries_metropole') }}
        )
    group by t.ticket_id
),

-- Gross business minutes from creation to final close (before subtracting paused periods)
gross_resolution_per_ticket as (
    select
        t.ticket_id,
        sum(greatest(
            timestamp_diff(
                least(timestamp(datetime(d, time(17, 30, 0)), 'Europe/Paris'), t.closed_time),
                greatest(timestamp(datetime(d, time(9, 0, 0)), 'Europe/Paris'), t.created_time),
                minute
            ), 0)
        ) as gross_resolution_business_minutes
    from {{ ref('stg_zoho_desk__tickets') }} as t
    cross join unnest(generate_date_array(
        date(t.created_time, 'Europe/Paris'),
        date(t.closed_time, 'Europe/Paris')
    )) as d
    where
        t.closed_time is not null
        and extract(dayofweek from d) not in (1, 7)
        and d not in (
            select cast(date_ferie as date)
            from {{ ref('ref_general__feries_metropole') }}
        )
    group by t.ticket_id
),

final as (
    select
        t.ticket_id,
        fr.first_response_at,
        frp.first_response_business_minutes,
        -- Formula: bh(created → closed) − sum(bh of intermediate Clôturée windows)
        -- On-hold is NOT subtracted: it extends the SLA deadline (confirmed ticket #19960)
        case
            when gr.gross_resolution_business_minutes is not null
                then gr.gross_resolution_business_minutes
                    - coalesce(pm.paused_business_minutes, 0)
        end as resolution_business_minutes,
        coalesce(oh.on_hold_business_minutes, 0) as on_hold_business_minutes,
        coalesce(pm.paused_business_minutes, 0) as paused_business_minutes
    from {{ ref('stg_zoho_desk__tickets') }} as t
    left join first_reply as fr on t.ticket_id = fr.ticket_id
    left join first_response_per_ticket as frp on t.ticket_id = frp.ticket_id
    left join gross_resolution_per_ticket as gr on t.ticket_id = gr.ticket_id
    left join paused_per_ticket as pm on t.ticket_id = pm.ticket_id
    left join on_hold_per_ticket as oh on t.ticket_id = oh.ticket_id
)

select * from final
