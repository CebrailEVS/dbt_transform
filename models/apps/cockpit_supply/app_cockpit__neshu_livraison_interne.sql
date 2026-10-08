{{ config(materialized='table') }}

-- Livraisons internes Neshu (dépôt → véhicule, entre dépôts), pour Cockpit Supply : seules les colonnes lues par l'app,
-- tâches validées seulement (FAIT, VALIDE), comme l'app.
select
    task_product_id,
    task_id,
    task_start_date,
    task_status_code,
    source_code,
    destination_code,
    product_code,
    quantity,
    valuation,
    unit_coeff_multi
from {{ ref('int_oracle_neshu__livraison_interne_tasks') }}
where task_status_code in ('FAIT', 'VALIDE')
