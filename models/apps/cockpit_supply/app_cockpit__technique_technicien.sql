{{ config(materialized='table') }}

-- Référentiel des techniciens TechCare, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    user_id,
    user_name,
    entrepot_rattachement,
    is_active,
    user_type
from {{ ref('dim_technique__technician') }}
