{{ config(materialized='table') }}

-- Commandes fournisseurs Neshu, pour Cockpit Supply : seules les colonnes lues par l'app,
-- tâches validées seulement (FAIT, VALIDE), comme l'app ; toutes les commandes, livrées ou non (taux de service fournisseur).
select
    task_product_id,
    task_id,
    company_id,
    destination_code,
    product_code,
    task_status_code,
    delivery_status_code,
    task_start_date,
    unit_coeff_multi,
    unit_coeff_div,
    quantity,
    valuation
from {{ ref('int_oracle_neshu__commande_fournisseur_tasks') }}
where task_status_code in ('FAIT', 'VALIDE')
