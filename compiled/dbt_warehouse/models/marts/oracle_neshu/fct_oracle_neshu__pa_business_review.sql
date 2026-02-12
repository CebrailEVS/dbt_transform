

select
    -- Identifiants
    pa.task_id,
    pa.company_id,
    pa.device_id,
    pa.company_code,

    -- Company
    c.company_name,
    concat(c.company_name, ' - ', pa.company_code) as company_info,

    -- Device
    d.device_brand,
    d.device_code,
    concat(d.device_brand, ' - ', d.device_code) as device_info,

    -- Contexte temporel
    pa.task_start_date,
    date(pa.task_start_date) as task_start_date_day,
    pa.task_end_date,

    -- Statut
    pa.task_status_code,
    case when pa.task_status_code = 'FAIT' then 1 else 0 end as mission_faite,
    case when pa.task_status_code in ('PREVU', 'FAIT', 'ENCOURS') then 1 else 0 end as mission_prevue,

    -- Métadonnées dbt
    current_timestamp() as dbt_updated_at,
    '1b10aaf4-04c5-4cde-aca5-7038835cf9f5' as dbt_invocation_id  -- noqa: TMP

from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__appro_tasks` as pa
inner join `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__device` as d
    on pa.device_id = d.device_id
inner join `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__company` as c
    on pa.company_id = c.company_id
where
    date(pa.task_start_date) >= '2025-01-01'
    and pa.task_status_code != 'ANNULE'