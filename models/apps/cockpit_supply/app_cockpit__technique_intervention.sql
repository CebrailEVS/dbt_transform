{{ config(materialized='table') }}

-- Interventions des techniciens TechCare (Yuman) : copie pour Cockpit Supply de int_yuman__interventions
-- (colonnes techniques de chargement exclues). L'app ne lit plus que son
-- dataset ; ses règles restent appliquées par l'app.
select *
from {{ ref('int_yuman__interventions') }}
