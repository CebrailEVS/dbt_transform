{{ config(materialized='table') }}

-- Référentiel des articles Neshu : copie pour Cockpit Supply de dim_neshu__product
-- (colonnes techniques de chargement exclues). L'app ne lit plus que son
-- dataset ; ses règles restent appliquées par l'app.
select *
from {{ ref('dim_neshu__product') }}
