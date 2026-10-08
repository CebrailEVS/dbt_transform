{{ config(materialized='table') }}

-- Prix des pièces Nespresso par intervention : copie pour Cockpit Supply de fct_technique__piece_detachee_pricing_nespresso
-- (colonnes techniques de chargement exclues). L'app ne lit plus que son
-- dataset ; ses règles restent appliquées par l'app.
select * except (dbt_updated_at, dbt_invocation_id)
from {{ ref('fct_technique__piece_detachee_pricing_nespresso') }}
