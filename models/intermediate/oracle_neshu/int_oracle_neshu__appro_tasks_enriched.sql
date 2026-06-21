{{ config(materialized='table') }}

-- ============================================================
-- CTE 1 : Roadman associé à chaque tâche appro
-- Jointure stg_oracle_neshu__task_has_resources × resources (type=2 = PERSON)
-- ============================================================
with resources_roadman as (
    select
        thr.idtask,
        min(r.idresources) as resources_roadman_id,
        min(r.code) as roadman_code
    from {{ ref('stg_oracle_neshu__task_has_resources') }} as thr
    inner join {{ ref('stg_oracle_neshu__resources') }} as r
        on
            thr.idresources = r.idresources
            and r.idresources_type = 2
    group by thr.idtask
),

-- ============================================================
-- CTE 2 : Enrichissement tâche appro
-- Joint le référentiel company, device, GEA. Normalise le statut
-- (ENCOURS reclassé en FAIT après validation métier). Calcule la
-- durée brute du passage.
-- ============================================================
enriched as (
    select
        -- IDs
        pa.task_id,
        pa.device_id,
        pa.company_id,
        pa.product_source_id,
        r.resources_roadman_id,
        pa.product_destination_id,
        pa.product_source_type,
        pa.product_destination_type,

        -- Codes et libellés aplatis
        pa.company_code,
        c.name as company_name,
        c.name || ' - ' || pa.company_code as company_info,
        d.code as device_code,
        d.name as device_name,
        d.name || ' - ' || d.code as device_info,
        g.gea_code,
        r.roadman_code,
        pa.task_location_info,

        -- Statut normalisé (ENCOURS → FAIT)
        case
            when pa.task_status_code in ('FAIT', 'ENCOURS') then 'FAIT'
            else pa.task_status_code
        end as task_status_code,
        case when pa.task_status_code in ('FAIT', 'ENCOURS') then 1 else 0 end as is_done,
        case
            when pa.task_status_code in ('PREVU', 'FAIT', 'ENCOURS', 'ANOMALIE') then 1 else 0
        end as is_planned,
        case when pa.task_status_code = 'ANOMALIE' then 1 else 0 end as is_anomaly,

        -- Dates
        date(pa.task_start_date) as start_date_day,
        pa.task_start_date,
        pa.task_end_date,

        -- Durée du passage
        pa.task_end_date - pa.task_start_date as passage_duration_interval,
        case
            when
                pa.task_start_date is not null
                and pa.task_end_date is not null
                and pa.task_status_code = 'FAIT'
                then
                    timestamp_diff(
                        pa.task_end_date,
                        pa.task_start_date,
                        second
                    ) / 60.0
        end as passage_duration_min,
        case
            when
                pa.task_start_date is not null
                and pa.task_end_date is not null
                and pa.task_status_code = 'FAIT'
                then
                    timestamp_diff(
                        pa.task_end_date,
                        pa.task_start_date,
                        second
                    ) / 3600.0
        end as passage_duration_hours,

        -- Métadonnées
        pa.created_at,
        pa.updated_at

    from {{ ref('int_oracle_neshu__appro_tasks') }} as pa

    left join resources_roadman as r
        on pa.task_id = r.idtask

    left join {{ ref('ref_oracle_neshu__roadman_gea') }} as g
        on r.roadman_code = g.roadman_code

    left join {{ ref('stg_oracle_neshu__company') }} as c
        on pa.company_id = c.idcompany

    left join {{ ref('stg_oracle_neshu__device') }} as d
        on pa.device_id = d.iddevice

    where r.resources_roadman_id is not null
)

select * from enriched
