{{ config(materialized='table') }}

-- Retraits d'invendus Cafés du Phare, pour Cockpit Supply : seules les colonnes lues par l'app,
-- tâches validées seulement (FAIT, VALIDE), comme l'app.
select
    task_product_id,
    task_id,
    task_status_code,
    task_start_date,
    product_code,
    quantity,
    valuation
from {{ ref('int_oracle_lcdp__invendus_tasks') }}
where task_status_code in ('FAIT', 'VALIDE')
