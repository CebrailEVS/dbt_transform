

-- Livraisons internes Cafés du Phare, pour Cockpit Supply : seules les colonnes lues par l'app,
-- tâches validées seulement (FAIT, VALIDE), comme l'app.
select
    task_product_id,
    task_id,
    task_start_date,
    task_status_code,
    source_code,
    destination_code,
    product_code,
    quantity,
    valuation
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__livraison_interne_tasks`
where task_status_code in ('FAIT', 'VALIDE')