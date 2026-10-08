{{ config(materialized='table') }}

-- Disponibilité mensuelle des articles dans les véhicules Neshu, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    mois,
    resources_id,
    entity_code,
    entity_name,
    product_code,
    product_name,
    jours_observes,
    jours_disponibles,
    is_vehicle_active
from {{ ref('fct_supply_chain__disponibilite_article_neshu_vehicule_mensuel') }}
