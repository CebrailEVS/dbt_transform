{{ config(
    materialized='table',
    partition_by={"field": "date_done", "data_type": "timestamp"},
    cluster_by=['product_id', 'client_id', 'site_id']
) }}

with consumed_products as (
    -- Grain source = ligne de saisie (workorder_product_id) : un même produit
    -- peut être saisi plusieurs fois sur un workorder. On agrège au grain
    -- analytique workorder x produit.
    select
        workorder_id,
        product_id,
        sum(product_quantity) as quantity,
        min(product_created_at) as first_recorded_at,
        max(product_updated_at) as last_updated_at
    from {{ ref('stg_yuman__workorder_products') }}
    group by workorder_id, product_id
),

workorders as (
    select
        workorder_id,
        demand_id,
        material_id,
        site_id,
        client_id,
        technician_id,
        partner_name,
        workorder_number,
        workorder_type,
        workorder_category,
        workorder_status,
        demand_category_name,
        demand_status,
        is_workorder_not_done,
        date_done
    from {{ ref('int_yuman__demands_workorders_enriched') }}
    where workorder_id is not null
    qualify row_number() over (partition by workorder_id order by date_done desc) = 1
)

select
    cp.workorder_id,
    w.demand_id,
    cp.product_id,
    w.material_id,
    w.site_id,
    w.client_id,
    w.technician_id,
    w.partner_name,
    w.workorder_number,
    w.workorder_type,
    w.workorder_category,
    w.workorder_status,
    w.demand_category_name,
    w.demand_status,
    w.is_workorder_not_done,
    w.date_done,
    cp.quantity,
    cp.first_recorded_at,
    cp.last_updated_at
from consumed_products as cp
left join workorders as w
    on cp.workorder_id = w.workorder_id
