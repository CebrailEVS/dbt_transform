{{ config(materialized='table') }}

-- Référentiel des machines Neshu (actives ou non, pour les préventives TechCare), pour Cockpit Supply : seules les colonnes lues par l'app.
select
    device_id,
    device_code,
    is_active,
    company_name
from {{ ref('dim_neshu__device') }}
