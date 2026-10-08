{{ config(materialized='table') }}

-- Disponibilité mensuelle des articles dans les dépôts Cafés du Phare : copie pour Cockpit Supply de fct_supply_chain__disponibilite_article_lcdp_depot_mensuel
-- (colonnes techniques de chargement exclues). L'app ne lit plus que son
-- dataset ; ses règles restent appliquées par l'app.
select *
from {{ ref('fct_supply_chain__disponibilite_article_lcdp_depot_mensuel') }}
