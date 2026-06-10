{{ config(
    materialized='table',
    cluster_by=['resources_id']
) }}

with monthly as (
    select
        date_trunc(date(date_system), month) as mois,
        id_entity as resources_id,
        product_code,
        any_value(entity_code having max date_system) as entity_code,
        any_value(entity_name having max date_system) as entity_name,
        any_value(product_name having max date_system) as product_name,
        count(distinct date(date_system)) as jours_observes,
        count(distinct case
            when is_out_of_stock = false then date(date_system)
        end) as jours_disponibles
    from {{ ref('fct_supply_chain__stock_neshu') }}
    where entity_type = 'resource'
    group by 1, 2, 3
)

select
    mois,
    resources_id,
    entity_code,
    entity_name,
    product_code,
    product_name,
    jours_observes,
    jours_disponibles,
    round(safe_divide(jours_disponibles, jours_observes) * 100, 1) as taux_disponibilite_pct
from monthly
