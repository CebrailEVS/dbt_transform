{{ config(materialized='table') }}

-- Disponibilité mensuelle des articles dans les véhicules Neshu, pour Cockpit Supply, construite depuis
-- l'intermediate. Libellés et statut du véhicule pris au dernier jour du mois ; ce statut est l'état COURANT
-- du véhicule et ne filtre pas.
select
    date_trunc(date(date_system), month) as mois,
    id_entity as resources_id,
    any_value(entity_code having max date_system) as entity_code,
    any_value(entity_name having max date_system) as entity_name,
    product_code,
    any_value(product_name having max date_system) as product_name,
    count(distinct date(date_system)) as jours_observes,
    count(distinct case when is_out_of_stock = false then date(date_system) end) as jours_disponibles,
    any_value(is_vehicle_active having max date_system) as is_vehicle_active
from {{ ref('int_oracle_neshu__stock_theorique') }}
where entity_type = 'resource'
group by date_trunc(date(date_system), month), id_entity, product_code
