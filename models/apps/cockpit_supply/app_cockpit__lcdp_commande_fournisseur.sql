{{ config(materialized='table') }}

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
    {{ utc_vers_paris_affichage('task_start_date') }} as task_start_date,
    unit_coeff_multi,
    unit_coeff_div,
    quantity,
    valuation
from {{ ref('int_oracle_lcdp__commande_fournisseur_tasks') }}
where task_status_code in ('FAIT', 'VALIDE')
