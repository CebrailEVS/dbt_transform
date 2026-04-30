{{
    config(
        materialized='table'
    )
}}

-- CTE 1 : Déduplique les workorders Yuman — une ligne par (machine, workorder)
-- en prenant la date de réalisation la plus tardive et la date planifiée la plus tôt.
with clean_workorders as (
    select
        material_id,
        material_serial_number,
        workorder_id,
        max(date_done) as date_done,
        min(date_planned) as date_planned
    from {{ ref('int_yuman__demands_workorders_enriched') }}
    group by
        material_id,
        material_serial_number,
        workorder_id
),

-- CTE 2 : Dernière intervention réalisée par machine dans les 30 derniers jours.
-- Le ROW_NUMBER permet d'isoler le workorder le plus récent par machine.
last_done as (
    select
        material_id,
        material_serial_number,
        workorder_id as last_done_workorder_id,
        date_done as last_done_date,
        row_number() over (
            partition by material_id
            order by date_done desc
        ) as rn
    from clean_workorders
    where
        date_done is not null
        and date(date_done) between date_sub(current_date(), interval 30 day) and current_date()
),

-- CTE 3 : Prochaine intervention planifiée par machine dans le futur.
-- Le ROW_NUMBER permet d'isoler la prochaine échéance (date la plus proche) par machine.
future_planned as (
    select
        material_id,
        material_serial_number,
        workorder_id as future_planned_workorder_id,
        date_planned as future_planned_date,
        row_number() over (
            partition by material_id
            order by date_planned asc
        ) as rn
    from clean_workorders
    where
        date_planned is not null
        and date(date_planned) > current_date()
),

-- CTE 4 : Full outer join entre dernière réalisée et prochaine planifiée pour couvrir
-- les machines qui n'ont qu'une seule des deux informations disponibles.
-- Le numéro de série est nettoyé du préfixe 'NESH_' pour la jointure avec Oracle.
yuman_interventions as (
    select
        coalesce(d.material_id, f.material_id) as material_id,
        coalesce(d.material_serial_number, f.material_serial_number) as material_serial_number,
        replace(coalesce(d.material_serial_number, f.material_serial_number), 'NESH_', '') as serial_clean,
        d.last_done_workorder_id,
        d.last_done_date,
        f.future_planned_workorder_id,
        f.future_planned_date
    from (
        select * from last_done
        where rn = 1
    ) as d
    full outer join (
        select * from future_planned
        where rn = 1
    ) as f
        on d.material_id = f.material_id
)

-- Résultat final : passages Oracle NESHU (appro) des 30 derniers jours
-- enrichis avec les informations d'intervention Yuman (dernière réalisée + prochaine planifiée).
select
    p.task_id,
    p.roadman_code,
    p.device_code,
    p.company_code,
    p.task_start_date,
    p.device_name,
    p.task_location_info,
    p.task_status_code,
    p.is_planned,
    p.is_done,
    y.last_done_workorder_id,
    y.last_done_date,
    y.future_planned_workorder_id,
    y.future_planned_date
from `evs-datastack-prod.prod_marts.fct_oracle_neshu__monitoring_passages_appro` as p
left join yuman_interventions as y
    on p.device_code = y.serial_clean
where date(p.start_date_day) between date_sub(current_date(), interval 30 day) and current_date()
order by p.task_start_date desc
