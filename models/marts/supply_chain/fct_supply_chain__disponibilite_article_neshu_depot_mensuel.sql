{{ config(
    materialized='table',
    cluster_by=['company_id']
) }}

-- Règle de la macro disponibilite_depot_mensuelle, la même que pour l'application Cockpit Supply.
with disponibilite as (
    {{ disponibilite_depot_mensuelle(ref('fct_supply_chain__stock_neshu')) }}
)

select
    mois,
    company_id,
    entity_code,
    entity_name,
    product_code,
    product_name,
    jours_observes,
    jours_disponibles,
    taux_disponibilite_pct
from disponibilite
