{{ config(materialized='table') }}

-- Référentiel des sociétés Cafés du Phare : copie pour Cockpit Supply de dim_lcdp__company
-- (colonnes techniques de chargement exclues). L'app ne lit plus que son
-- dataset ; ses règles restent appliquées par l'app.
select *
from {{ ref('dim_lcdp__company') }}
