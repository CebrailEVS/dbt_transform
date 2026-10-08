

-- Retraits d'invendus Neshu (retours machine), pour Cockpit Supply : seules les colonnes lues par l'app,
-- tâches validées seulement (FAIT, VALIDE), comme l'app.
select
    task_product_id,
    task_id,
    company_id,
    destination_code,
    vehicle_code,
    product_code,
    task_status_code,
    task_start_date,
    unit_coeff_multi,
    quantity,
    valuation
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__invendus_tasks`
where task_status_code in ('FAIT', 'VALIDE')