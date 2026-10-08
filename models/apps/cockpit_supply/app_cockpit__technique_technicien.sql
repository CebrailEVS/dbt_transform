{{ config(materialized='table') }}

-- Référentiel des techniciens TechCare : copie pour Cockpit Supply de dim_technique__technician
-- (colonnes techniques de chargement exclues). L'app ne lit plus que son
-- dataset ; ses règles restent appliquées par l'app.
select *
from {{ ref('dim_technique__technician') }}
