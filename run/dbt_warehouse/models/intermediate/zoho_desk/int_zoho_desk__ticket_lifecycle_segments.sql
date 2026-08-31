
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_zoho_desk__ticket_lifecycle_segments`
      
    partition by segment_start_date_paris
    cluster by ticket_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Une ligne par p\u00e9riode (segment) pendant laquelle un ticket reste dans un m\u00eame statut, avec sa dur\u00e9e calendaire et sa dur\u00e9e en heures ouvr\u00e9es. Brique de calcul des temps pass\u00e9s en Open / On Hold / Closed (utilis\u00e9e par int_zoho_desk__ticket_sla).\n[COMMENT CONSTRUITE] LEAD() sur int_zoho_desk__ticket_status_events (\u00e9v\u00e9nements TicketCreated/TicketUpdated, fusions exclues) : le segment N va de event_at[N] \u00e0 event_at[N+1]. Dur\u00e9e ouvr\u00e9e = minutes dans la plage 9h00-17h30, jours ouvr\u00e9s, hors f\u00e9ri\u00e9s FR (ref_general__feries_metropole).\n[GRAIN] 1 ligne par (ticket_id, segment_idx). ~65,3k segments. Le dernier segment (statut courant) a segment_end_at et les dur\u00e9es \u00e0 NULL.\n[NOTES] Heures ouvr\u00e9es = 09:00\u201317:30, lun-ven, hors jours f\u00e9ri\u00e9s m\u00e9tropole.\n"""
    )
    as (
      

with events as (
    select
        ticket_id,
        event_at,
        new_status as status,
        new_status_type as status_type,
        new_is_on_hold as is_on_hold,
        new_is_closed as is_closed
    from `evs-datastack-prod`.`prod_intermediate`.`int_zoho_desk__ticket_status_events`
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
    from `evs-datastack-prod`.`prod_reference`.`ref_general__feries_metropole`
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
    );
  