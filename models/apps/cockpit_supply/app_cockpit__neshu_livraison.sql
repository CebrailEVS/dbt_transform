{{ config(materialized='table') }}

-- Livraisons Neshu (bons de livraison), pour Cockpit Supply : seules les colonnes lues par l'app,
-- tâches validées seulement (FAIT, VALIDE), comme l'app.
select
    task_product_id,
    task_id,
    company_id,
    product_source_id,
    company_code,
    product_code,
    task_status_code,
    task_start_date,
    quantity,
    valuation
from {{ ref('int_oracle_neshu__livraison_tasks') }}
where task_status_code in ('FAIT', 'VALIDE')
