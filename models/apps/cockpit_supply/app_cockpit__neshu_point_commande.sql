{{ config(materialized='table') }}

-- Point de commande Neshu (« Commandes à passer ») : copie pour Cockpit Supply de fct_supply_chain__point_commande_neshu
-- (colonnes techniques de chargement exclues). L'app ne lit plus que son
-- dataset ; ses règles restent appliquées par l'app.
select *
from {{ ref('fct_supply_chain__point_commande_neshu') }}
