{{ config(materialized='table') }}

-- Ruptures quotidiennes des dépôts TechCare (stock Yuman) : copie pour Cockpit Supply de fct_supply_chain__rupture_depot_yuman
-- (colonnes techniques de chargement exclues). L'app ne lit plus que son
-- dataset ; ses règles restent appliquées par l'app.
select * except (dbt_updated_at, dbt_invocation_id)
from {{ ref('fct_supply_chain__rupture_depot_yuman') }}
