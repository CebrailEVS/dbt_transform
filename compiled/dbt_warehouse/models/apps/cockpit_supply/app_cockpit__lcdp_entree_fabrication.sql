

-- Entrées en fabrication (torréfaction) Cafés du Phare, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    task_product_id,
    task_id,
    product_code,
    task_start_date,
    source_code,
    destination_code,
    quantity,
    valuation
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__entree_fabrication_tasks`