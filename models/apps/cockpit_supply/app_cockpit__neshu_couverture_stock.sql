{{ config(materialized='table') }}

-- Couverture de stock des dépôts Neshu : copie pour Cockpit Supply de fct_supply_chain__couverture_stock_neshu
-- (colonnes techniques de chargement exclues). L'app ne lit plus que son
-- dataset ; ses règles restent appliquées par l'app.
select *
from {{ ref('fct_supply_chain__couverture_stock_neshu') }}
