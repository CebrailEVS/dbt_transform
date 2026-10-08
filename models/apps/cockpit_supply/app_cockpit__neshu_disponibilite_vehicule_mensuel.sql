{{ config(materialized='table') }}

-- Disponibilité mensuelle des articles dans les véhicules Neshu : copie pour Cockpit Supply de fct_supply_chain__disponibilite_article_neshu_vehicule_mensuel
-- (colonnes techniques de chargement exclues). L'app ne lit plus que son
-- dataset ; ses règles restent appliquées par l'app.
select *
from {{ ref('fct_supply_chain__disponibilite_article_neshu_vehicule_mensuel') }}
