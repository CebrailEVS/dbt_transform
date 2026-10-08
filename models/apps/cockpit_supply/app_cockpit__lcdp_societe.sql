{{ config(materialized='table') }}

-- Référentiel des sociétés Cafés du Phare, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    company_id,
    company_code,
    company_name
from {{ ref('dim_lcdp__company') }}
