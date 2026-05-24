{{ config(materialized='table') }}

-- ============================================================
-- int_oracle_neshu__appro_machine_context
--
-- Grain : 1 ligne par device_id.
--
-- Objectif : fournir le "contexte appro" de chaque machine ayant eu
-- au moins un passage appro :
--   - dernier passage FAIT (date, roadman, GEA)
--   - prochain passage PRÉVU (date, roadman, GEA)
--   - compteurs (total, à venir, 30j, 90j)
--
-- Source unique : int_oracle_neshu__appro_tasks_enriched (grain task).
-- Pas de filtre temporel ni de statut device — l'intermediate reste
-- agnostique, les marts en aval filtrent (is_active, période, etc.).
-- ============================================================

with appro_tasks as (

    select
        task_id,
        device_id,
        company_id,
        task_start_date,
        task_end_date,
        task_status_code,
        resources_roadman_id,
        roadman_code,
        gea_code
    from {{ ref('int_oracle_neshu__appro_tasks_enriched') }}
    where device_id is not null

),

-- ============================================================
-- CTE 2 : référentiel devices distincts + company_id stable
-- (any_value : si un device a eu plusieurs companies au fil du temps,
-- on prend l'une d'elles — anomalie rare, à surveiller côté data
-- quality)
-- ============================================================
devices as (

    select
        device_id,
        any_value(company_id) as company_id
    from appro_tasks
    group by device_id

),

-- ============================================================
-- CTE 3 : dernier passage FAIT par device (row_number)
-- ============================================================
last_done_ranked as (

    select
        device_id,
        task_id,
        task_start_date,
        resources_roadman_id,
        roadman_code,
        gea_code,
        row_number() over (
            partition by device_id
            order by task_start_date desc
        ) as rn
    from appro_tasks
    where task_status_code = 'FAIT'

),

last_done as (

    select
        device_id,
        task_id as last_appro_task_id,
        task_start_date as last_appro_date,
        resources_roadman_id as last_appro_roadman_id,
        roadman_code as last_appro_roadman_code,
        gea_code as last_appro_gea_code,
        date_diff(
            current_date(),
            date(task_start_date),
            day
        ) as days_since_last_appro
    from last_done_ranked
    where rn = 1

),

-- ============================================================
-- CTE 4 : prochain passage PRÉVU par device (row_number)
-- Filtre : passage encore à venir (task_start_date >= maintenant)
-- ============================================================
next_planned_ranked as (

    select
        device_id,
        task_id,
        task_start_date,
        resources_roadman_id,
        roadman_code,
        gea_code,
        row_number() over (
            partition by device_id
            order by task_start_date asc
        ) as rn
    from appro_tasks
    where
        task_status_code = 'PREVU'
        and task_start_date >= current_timestamp()

),

next_planned as (

    select
        device_id,
        task_id as next_appro_task_id,
        task_start_date as next_appro_date,
        resources_roadman_id as next_appro_roadman_id,
        roadman_code as next_appro_roadman_code,
        gea_code as next_appro_gea_code,
        date_diff(
            date(task_start_date),
            current_date(),
            day
        ) as days_until_next_appro
    from next_planned_ranked
    where rn = 1

),

-- ============================================================
-- CTE 5 : compteurs par device
--   - total réalisés (toute période)
--   - à venir (prévus dans le futur)
--   - réalisés sur 30/90 derniers jours
-- ============================================================
counters as (

    select
        device_id,
        countif(task_status_code = 'FAIT') as nb_appros_realises_total,
        countif(
            task_status_code = 'PREVU'
            and task_start_date >= current_timestamp()
        ) as nb_appros_planifies_a_venir,
        countif(
            task_status_code = 'FAIT'
            and task_start_date >= timestamp_sub(
                current_timestamp(), interval 30 day
            )
        ) as nb_appros_realises_30d,
        countif(
            task_status_code = 'FAIT'
            and task_start_date >= timestamp_sub(
                current_timestamp(), interval 90 day
            )
        ) as nb_appros_realises_90d
    from appro_tasks
    group by device_id

)

select
    -- IDs propres
    d.device_id,
    d.company_id,

    -- Dernier passage FAIT
    ld.last_appro_task_id,
    ld.last_appro_date,
    ld.last_appro_roadman_id,
    ld.last_appro_roadman_code,
    ld.last_appro_gea_code,
    ld.days_since_last_appro,

    -- Prochain passage PRÉVU
    np.next_appro_task_id,
    np.next_appro_date,
    np.next_appro_roadman_id,
    np.next_appro_roadman_code,
    np.next_appro_gea_code,
    np.days_until_next_appro,

    -- Compteurs (NULL devient 0 pour les machines sans passage)
    coalesce(c.nb_appros_realises_total, 0) as nb_appros_realises_total,
    coalesce(c.nb_appros_planifies_a_venir, 0) as nb_appros_planifies_a_venir,
    coalesce(c.nb_appros_realises_30d, 0) as nb_appros_realises_30d,
    coalesce(c.nb_appros_realises_90d, 0) as nb_appros_realises_90d

from devices as d

left join last_done as ld
    on d.device_id = ld.device_id

left join next_planned as np
    on d.device_id = np.device_id

left join counters as c
    on d.device_id = c.device_id
