{{
    config(
        materialized='table',
        partition_by={
            'field': 'segment_start_date_paris',
            'data_type': 'date'
        },
        cluster_by=['ticket_id']
    )
}}

with events as (
    select
        ticket_id,
        event_at,
        new_status as status,
        new_status_type as status_type,
        new_is_on_hold as is_on_hold,
        new_is_closed as is_closed
    from {{ ref('int_zoho_desk__ticket_status_events') }}
    where event_name in ('TicketCreated', 'TicketUpdated')
),

segments as (
    select
        ticket_id,
        row_number() over (partition by ticket_id order by event_at) as segment_idx,
        event_at as segment_start_at,
        date(event_at, 'Europe/Paris') as segment_start_date_paris,
        lead(event_at) over (partition by ticket_id order by event_at) as segment_end_at,
        status,
        status_type,
        is_on_hold,
        is_closed
    from events
),

holidays as (
    select cast(date_ferie as date) as holiday_date
    from {{ ref('ref_general__feries_metropole') }}
),

-- Compute business minutes per segment via explicit CROSS JOIN UNNEST.
-- Same logic as the business_minutes_between macro but un-correlated,
-- so BigQuery can plan it alongside the LEAD() window above.
business_durations as (
    select
        s.ticket_id,
        s.segment_idx,
        sum(greatest(
            timestamp_diff(
                least(
                    timestamp(datetime(d, time(17, 30, 0)), 'Europe/Paris'),
                    s.segment_end_at
                ),
                greatest(
                    timestamp(datetime(d, time(9, 0, 0)), 'Europe/Paris'),
                    s.segment_start_at
                ),
                minute
            ),
            0
        )) as duration_minutes_business
    from segments as s
    cross join
        unnest(generate_date_array(
            date(s.segment_start_at, 'Europe/Paris'),
            date(s.segment_end_at, 'Europe/Paris')
        )) as d
    left join holidays as h
        on d = h.holiday_date
    where
        s.segment_end_at is not null
        and extract(dayofweek from d) not in (1, 7)
        and h.holiday_date is null
    group by s.ticket_id, s.segment_idx
)

select
    s.ticket_id,
    s.segment_idx,
    s.segment_start_at,
    s.segment_start_date_paris,
    s.segment_end_at,
    s.status,
    s.status_type,
    s.is_on_hold,
    s.is_closed,
    if(
        s.segment_end_at is null,
        null,
        timestamp_diff(s.segment_end_at, s.segment_start_at, minute)
    ) as duration_minutes_calendar,
    if(
        s.segment_end_at is null,
        null,
        coalesce(bd.duration_minutes_business, 0)
    ) as duration_minutes_business
from segments as s
left join business_durations as bd
    on
        s.ticket_id = bd.ticket_id
        and s.segment_idx = bd.segment_idx
