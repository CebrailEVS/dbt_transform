

-- Réceptions fournisseurs Neshu, pour Cockpit Supply : seules les colonnes lues par l'app,
-- tâches validées seulement (FAIT, VALIDE), comme l'app.
select
    task_product_id,
    task_id,
    company_id,
    destination_code,
    product_code,
    task_status_code,
    task_start_date,
    delivery_lead_time_days,
    is_lead_time_valid,
    quantity,
    valuation
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__reception_tasks`
where task_status_code in ('FAIT', 'VALIDE')