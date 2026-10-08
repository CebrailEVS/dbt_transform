{{ config(materialized='table') }}

-- Disponibilité mensuelle des articles dans les dépôts Cafés du Phare, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    mois,
    company_id,
    entity_code,
    product_code,
    taux_disponibilite_pct
from {{ ref('fct_supply_chain__disponibilite_article_lcdp_depot_mensuel') }}
