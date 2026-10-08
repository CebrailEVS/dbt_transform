{{ config(materialized='table') }}

-- Sorties de fabrication (torréfaction) Cafés du Phare, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    task_product_id,
    task_id,
    product_code,
    task_start_date,
    source_code,
    destination_code,
    quantity,
    valuation
from {{ ref('int_oracle_lcdp__sortie_fabrication_tasks') }}
