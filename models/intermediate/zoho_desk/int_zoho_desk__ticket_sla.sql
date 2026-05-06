{{
    config(
        materialized='table',
        cluster_by=['ticket_id']
    )
}}

with enriched as (
    select
        ticket_id,
        created_time
    from {{ ref('int_zoho_desk__ticket_enriched') }}
),

threads as (
    select
        _zoho_desk_associated_tickets_id as ticket_id,
        min(case
            when
                direction = 'out'
                and author__type = 'AGENT'
                and not coalesce(is_description_thread, false)
                then created_time
        end) as first_agent_response_at,
        min(case
            when direction = 'in' then created_time
        end) as first_inbound_at
    from {{ ref('stg_zoho_desk__ticket_threads') }}
    group by ticket_id
),

closes as (
    select
        ticket_id,
        min(case
            when new_is_closed and not coalesce(prev_is_closed, false)
                then event_at
        end) as first_close_at,
        max(case
            when new_is_closed and not coalesce(prev_is_closed, false)
                then event_at
        end) as last_close_at,
        countif(new_is_closed and not coalesce(prev_is_closed, false)) as nb_closes,
        countif(prev_is_closed and not new_is_closed) as nb_reopens
    from {{ ref('int_zoho_desk__ticket_status_events') }}
    where event_name = 'TicketUpdated'
    group by ticket_id
),

holidays as (
    select cast(date_ferie as date) as holiday_date
    from {{ ref('ref_general__feries_metropole') }}
),

-- Long-format: 1 row per (ticket, metric, start_ts, end_ts).
-- We compute 3 business durations per ticket; this lets us share the
-- CROSS JOIN UNNEST against the date array instead of running 3 correlated subqueries.
duration_pairs as (
    select
        e.ticket_id,
        'first_response' as metric_key,
        e.created_time as start_ts,
        t.first_agent_response_at as end_ts
    from enriched as e
    left join threads as t on e.ticket_id = t.ticket_id
    where t.first_agent_response_at is not null
    union all
    select
        e.ticket_id,
        'first_close' as metric_key,
        e.created_time as start_ts,
        c.first_close_at as end_ts
    from enriched as e
    left join closes as c on e.ticket_id = c.ticket_id
    where c.first_close_at is not null
    union all
    select
        e.ticket_id,
        'last_close' as metric_key,
        e.created_time as start_ts,
        c.last_close_at as end_ts
    from enriched as e
    left join closes as c on e.ticket_id = c.ticket_id
    where c.last_close_at is not null
),

business_durations as (
    select
        dp.ticket_id,
        dp.metric_key,
        sum(greatest(
            timestamp_diff(
                least(
                    timestamp(datetime(d, time(17, 30, 0)), 'Europe/Paris'),
                    dp.end_ts
                ),
                greatest(
                    timestamp(datetime(d, time(9, 0, 0)), 'Europe/Paris'),
                    dp.start_ts
                ),
                minute
            ),
            0
        )) as minutes_business
    from duration_pairs as dp
    cross join
        unnest(generate_date_array(
            date(dp.start_ts, 'Europe/Paris'),
            date(dp.end_ts, 'Europe/Paris')
        )) as d
    left join holidays as h
        on d = h.holiday_date
    where
        extract(dayofweek from d) not in (1, 7)
        and h.holiday_date is null
    group by dp.ticket_id, dp.metric_key
),

business_pivoted as (
    select
        ticket_id,
        max(if(metric_key = 'first_response', minutes_business, null))
            as first_response_minutes_business,
        max(if(metric_key = 'first_close', minutes_business, null))
            as resolution_minutes_first_close_business,
        max(if(metric_key = 'last_close', minutes_business, null))
            as resolution_minutes_last_close_business
    from business_durations
    group by ticket_id
),

segments_agg as (
    select
        ticket_id,
        sum(if(status_type = 'Open', duration_minutes_calendar, 0))
            as time_in_open_minutes_calendar,
        sum(if(status_type = 'On Hold', duration_minutes_calendar, 0))
            as time_in_on_hold_minutes_calendar,
        sum(if(status_type = 'Open', duration_minutes_business, 0))
            as time_in_open_minutes_business,
        sum(if(status_type = 'On Hold', duration_minutes_business, 0))
            as time_in_on_hold_minutes_business
    from {{ ref('int_zoho_desk__ticket_lifecycle_segments') }}
    group by ticket_id
)

select
    e.ticket_id,
    e.created_time,

    -- first response
    t.first_agent_response_at,
    t.first_inbound_at,
    if(
        t.first_agent_response_at is null,
        null,
        timestamp_diff(t.first_agent_response_at, e.created_time, minute)
    ) as first_response_minutes_calendar,
    bp.first_response_minutes_business,

    -- closure events
    c.first_close_at,
    c.last_close_at,
    coalesce(c.nb_closes, 0) as nb_closes,
    coalesce(c.nb_reopens, 0) as nb_reopens,

    -- resolution time relative to FIRST close
    if(
        c.first_close_at is null,
        null,
        timestamp_diff(c.first_close_at, e.created_time, minute)
    ) as resolution_minutes_first_close_calendar,
    bp.resolution_minutes_first_close_business,

    -- resolution time relative to LAST close
    if(
        c.last_close_at is null,
        null,
        timestamp_diff(c.last_close_at, e.created_time, minute)
    ) as resolution_minutes_last_close_calendar,
    bp.resolution_minutes_last_close_business,

    -- time in each status type
    coalesce(s.time_in_open_minutes_calendar, 0) as time_in_open_minutes_calendar,
    coalesce(s.time_in_on_hold_minutes_calendar, 0) as time_in_on_hold_minutes_calendar,
    coalesce(s.time_in_open_minutes_business, 0) as time_in_open_minutes_business,
    coalesce(s.time_in_on_hold_minutes_business, 0) as time_in_on_hold_minutes_business,

    -- resolution excluding hold (= time the ticket was actively Open)
    coalesce(s.time_in_open_minutes_calendar, 0) as resolution_excluding_hold_minutes_calendar,
    coalesce(s.time_in_open_minutes_business, 0) as resolution_excluding_hold_minutes_business

from enriched as e
left join threads as t on e.ticket_id = t.ticket_id
left join closes as c on e.ticket_id = c.ticket_id
left join business_pivoted as bp on e.ticket_id = bp.ticket_id
left join segments_agg as s on e.ticket_id = s.ticket_id
