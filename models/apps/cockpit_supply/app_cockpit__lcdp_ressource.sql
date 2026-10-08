{{ config(materialized='table') }}

-- Référentiel des ressources Cafés du Phare (véhicules), pour Cockpit Supply : seules les colonnes lues par l'app.
select
    resources_id,
    resources_code,
    resources_name,
    resources_type
from {{ ref('dim_lcdp__resource') }}
