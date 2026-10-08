{{ config(materialized='table') }}

-- Référentiel des machines (distributeurs) Neshu : copie pour Cockpit Supply de dim_neshu__device
-- (colonnes techniques de chargement exclues). L'app ne lit plus que son
-- dataset ; ses règles restent appliquées par l'app.
select *
from {{ ref('dim_neshu__device') }}
