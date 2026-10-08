{{ config(
    materialized='table',
    partition_by={
        'field': 'task_start_date',
        'data_type': 'timestamp',
        'granularity': 'month'
    },
    cluster_by=['vehicle_code', 'task_id']
) }}

-- Chargements machines Cafés du Phare Distribution Auto validés : l'app en lit un
-- mois à la fois (flux des véhicules) au lieu de garder tout l'historique en
-- mémoire. Partition mensuelle : un mois demandé = un mois lu.
select
    task_product_id,
    task_id,
    task_start_date,
    task_status_code,
    load_type_code,
    vehicle_code,
    roadman_code,
    company_id,
    company_code,
    device_id,
    device_code,
    product_code,
    unit_coeff_multi,
    unit_coeff_div,
    base_unit_quantity,
    load_quantity,
    load_valuation
from {{ ref('int_oracle_lcdp__chargement_tasks') }}
-- Statuts retenus par l'app (config.STATUS_VALIDES)
where task_status_code in ('FAIT', 'VALIDE')
