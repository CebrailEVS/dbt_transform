{{ config(materialized='table') }}

-- Réceptions fournisseurs Cafés du Phare, pour Cockpit Supply : seules les colonnes lues par l'app,
-- tâches validées seulement (FAIT, VALIDE), comme l'app.
select
    task_product_id,
    task_id,
    document_number,
    company_id,
    destination_code,
    product_code,
    task_status_code,
    {{ utc_vers_paris_affichage('task_start_date') }} as task_start_date,
    delivery_lead_time_days,
    is_lead_time_valid,
    quantity,
    valuation,
    amount_excl_tax,
    product_unit_price_task as unit_price_excl_tax,
    product_unit_price_at_reception
from {{ ref('int_oracle_lcdp__reception_tasks') }}
where task_status_code in ('FAIT', 'VALIDE')
