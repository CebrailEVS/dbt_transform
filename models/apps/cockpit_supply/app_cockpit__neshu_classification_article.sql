{{ config(materialized='table') }}

-- Classification ABC/XYZ des articles Neshu : copie pour Cockpit Supply de fct_supply_chain__classification_article_neshu
-- (colonnes techniques de chargement exclues). L'app ne lit plus que son
-- dataset ; ses règles restent appliquées par l'app.
select *
from {{ ref('fct_supply_chain__classification_article_neshu') }}
