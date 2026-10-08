{{ config(materialized='table') }}

-- Livraisons Cafés du Phare (bons de livraison), pour Cockpit Supply : seules les colonnes lues par l'app,
-- tâches validées seulement (FAIT, VALIDE), comme l'app.
select
    task_product_id,
    task_id,
    company_id,
    product_source_id,
    product_code,
    task_status_code,
    task_start_date,
    quantity,
    valuation
from {{ ref('int_oracle_lcdp__livraison_tasks') }}
where task_status_code in ('FAIT', 'VALIDE')
