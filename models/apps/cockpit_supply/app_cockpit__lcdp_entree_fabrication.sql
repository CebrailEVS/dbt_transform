{{ config(materialized='table') }}

-- Entrées en fabrication (torréfaction) Cafés du Phare : copie pour Cockpit Supply de int_oracle_lcdp__entree_fabrication_tasks
-- (colonnes techniques de chargement exclues). L'app ne lit plus que son
-- dataset ; ses règles restent appliquées par l'app.
select * except (extracted_at)
from {{ ref('int_oracle_lcdp__entree_fabrication_tasks') }}
