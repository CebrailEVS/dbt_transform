

-- ============================================================
-- CTE 1 : Début de journée par roadman/jour
-- Premier pointage START_DAY à partir de 03:00:00
-- ============================================================
with pointage_debut as (
    select
        resources_roadman_id,
        roadman_code,
        date_pointage_jour,
        min(case
            when
                label_code = 'START_DAY'
                and format_timestamp('%H:%M:%S', date_pointage) >= '03:00:00'
                then date_pointage
        end) as date_pointage_debut
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__pointage_tasks`
    group by resources_roadman_id, roadman_code, date_pointage_jour
),

-- ============================================================
-- CTE 2 : Fin de journée = dernier END_DAY postérieur au début
-- (évite une fin antérieure au début le même jour)
-- ============================================================
pointage_final as (
    select
        d.resources_roadman_id,
        d.roadman_code,
        d.date_pointage_jour,
        d.date_pointage_debut,
        max(case
            when
                pf.label_code = 'END_DAY'
                and pf.date_pointage >= d.date_pointage_debut
                then pf.date_pointage
        end) as date_pointage_fin
    from pointage_debut as d
    left join `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__pointage_tasks` as pf
        on
            d.resources_roadman_id = pf.resources_roadman_id
            and d.date_pointage_jour = pf.date_pointage_jour
    group by
        d.resources_roadman_id,
        d.roadman_code,
        d.date_pointage_jour,
        d.date_pointage_debut
),

-- ============================================================
-- CTE 3 : Tâches appro enrichies + pointage début/fin joint
-- ============================================================
passage_appro as (
    select
        e.*,
        p.date_pointage_debut,
        p.date_pointage_fin,
        p.date_pointage_jour,
        case when p.date_pointage_debut is null then 1 else 0 end as pointage_missing_flag
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__appro_tasks_enriched` as e
    left join pointage_final as p
        on
            e.start_date_day = p.date_pointage_jour
            and e.resources_roadman_id = p.resources_roadman_id
)

-- ============================================================
-- Sélection finale (grain : 1 ligne par task_id)
-- ============================================================
select
    -- 1️⃣ IDs
    task_id,
    device_id,
    company_id,
    resources_roadman_id,
    product_source_id,
    product_destination_id,

    -- 2️⃣ Attributs
    company_code,
    company_name,
    company_info,
    device_code,
    device_name,
    device_info,
    roadman_code,
    roadman_name,
    product_source_type,
    product_destination_type,
    task_location_info,
    task_status_code,

    -- 3️⃣ Dates / Temps
    date_pointage_debut,
    date_pointage_fin,
    start_date_day,
    task_start_date,
    task_end_date,

    -- 4️⃣ Mesures / Flags
    passage_duration_min,
    is_planned,
    is_done,
    is_anomaly,
    pointage_missing_flag,

    -- 5️⃣ Métadonnées
    created_at,
    updated_at,
    current_timestamp() as dbt_updated_at,
    'a7f5414a-28d7-499b-9bf5-f2facb8fa172' as dbt_invocation_id  -- noqa: TMP

from passage_appro