{{ config(materialized='table') }}

-- Passages des approvisionneurs Cafés du Phare Distribution Auto : copie pour Cockpit Supply de int_oracle_lcdp__appro_tasks_enriched
-- (colonnes techniques de chargement exclues). L'app ne lit plus que son
-- dataset ; ses règles restent appliquées par l'app.
select *
from {{ ref('int_oracle_lcdp__appro_tasks_enriched') }}
