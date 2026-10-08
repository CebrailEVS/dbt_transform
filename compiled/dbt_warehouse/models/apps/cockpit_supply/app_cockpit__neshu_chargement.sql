

-- Chargements machines Neshu validés, enrichis du client et de la machine : l'app
-- interroge cette table à la demande (filtres, regroupements, période) au lieu de
-- garder tout l'historique en mémoire. Partition mensuelle : un écran filtré sur
-- un mois ne lit que ce mois.
select
    c.task_product_id,
    c.task_id,
    c.task_start_date,
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
left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__company` as co
    on c.company_id = co.company_id
left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__device` as d
    on c.device_id = d.device_id
-- Statuts retenus par l'app (config.STATUS_VALIDES)
where c.task_status_code in ('FAIT', 'VALIDE')