

-- Entrées en fabrication (torréfaction) Cafés du Phare, pour Cockpit Supply : seules les colonnes lues par l'app,
-- tâches validées (FAIT, VALIDE) de café vert passé de VERT à FABRICATION ; une entrée saisie pour un simple
-- transfert est comptée en livraison interne (app_cockpit__lcdp_livraison_interne).
select
    task_product_id,
    task_id,
    product_code,
    timestamp(datetime(task_start_date, 'Europe/Paris')) as task_start_date,
    source_code,
    destination_code,
    quantity,
    valuation
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__entree_fabrication_tasks`
where
    task_status_code in ('FAIT', 'VALIDE')
    and not is_transfert