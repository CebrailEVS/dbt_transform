{{ config(materialized='table') }}

-- Pièces Nespresso consommées par les techniciens (système Nomad) : copie pour Cockpit Supply de fct_technique__consommation_article_nespresso
-- (colonnes techniques de chargement exclues). L'app ne lit plus que son
-- dataset ; ses règles restent appliquées par l'app.
select * except (dbt_updated_at, dbt_invocation_id)
from {{ ref('fct_technique__consommation_article_nespresso') }}
