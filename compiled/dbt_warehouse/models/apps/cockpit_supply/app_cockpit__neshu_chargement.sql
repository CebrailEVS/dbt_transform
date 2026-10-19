

-- Chargements machines Neshu validés, enrichis du client et de la machine : l'app
-- interroge cette table à la demande (filtres, regroupements, période) au lieu de
-- garder tout l'historique en mémoire. Partition mensuelle : un écran filtré sur
-- un mois ne lit que ce mois. Client et machine viennent du staging, pas des dims.

with client as (
    select
        idcompany,
        name as company_name
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company`
    where
        idcompany_type in (1, 2, 4, 6)
        and code_status_record <> -1
),

machine as (
    select
        d.iddevice,
        d.name as device_name,
        max(case when lf.code = 'MARQUE' then l.code end) as device_brand
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device` as d
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_device` as lhd
        on d.iddevice = lhd.iddevice
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` as l
        on lhd.idlabel = l.idlabel
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family` as lf
        on l.idlabel_family = lf.idlabel_family
    where
        d.idcompany_customer is not null
        and d.code_status_record <> -1
    group by d.iddevice, d.name
)

select
    c.task_product_id,
    c.task_id,
    timestamp(datetime(c.task_start_date, 'Europe/Paris')) as task_start_date,
    c.task_status_code,
    c.load_type_code,
    c.task_location_info,
    c.vehicle_code,
    c.roadman_code,
    c.company_id,
    c.company_code,
    co.company_name,
    c.device_id,
    c.device_code,
    d.device_name,
    d.device_brand,
    c.product_code,
    c.unit_coeff_multi,
    c.unit_coeff_div,
    c.base_unit_quantity,
    c.load_quantity,
    c.load_valuation
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__chargement_tasks` as c
left join client as co
    on c.company_id = co.idcompany
left join machine as d
    on c.device_id = d.iddevice
-- Statuts retenus par l'app (config.STATUS_VALIDES)
where c.task_status_code in ('FAIT', 'VALIDE')