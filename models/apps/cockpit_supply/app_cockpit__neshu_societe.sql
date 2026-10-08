{{ config(materialized='table') }}

-- Référentiel des sociétés Neshu (clients, fournisseurs, dépôts), pour Cockpit Supply : seules les colonnes lues par l'app.
select
    company_id,
    company_code,
    company_name,
    is_depot
from {{ ref('dim_neshu__company') }}
