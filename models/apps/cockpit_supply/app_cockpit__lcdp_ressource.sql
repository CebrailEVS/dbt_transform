{{ config(materialized='table') }}

-- Référentiel des ressources Cafés du Phare (véhicules, personnes) : copie pour Cockpit Supply de dim_lcdp__resource
-- (colonnes techniques de chargement exclues). L'app ne lit plus que son
-- dataset ; ses règles restent appliquées par l'app.
select *
from {{ ref('dim_lcdp__resource') }}
