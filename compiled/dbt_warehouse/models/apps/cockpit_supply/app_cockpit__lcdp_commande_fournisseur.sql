

-- Commandes fournisseurs Cafés du Phare, pour Cockpit Supply (taux de service des fiches fournisseurs,
-- lot ISO P1) : mêmes colonnes et même filtre que app_cockpit__neshu_commande_fournisseur,
-- tâches validées seulement (FAIT, VALIDE) ; toutes les commandes, livrées ou non.
select
    task_product_id,
    task_id,
    company_id,
    destination_code,
    product_code,
    task_status_code,
    delivery_status_code,
    timestamp(datetime(task_start_date, 'Europe/Paris')) as task_start_date,
    unit_coeff_multi,
    unit_coeff_div,
    quantity,
    valuation
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__commande_fournisseur_tasks`
where task_status_code in ('FAIT', 'VALIDE')