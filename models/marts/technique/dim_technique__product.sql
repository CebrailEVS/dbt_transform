{{ config(
    materialized='table'
) }}

select
    product_id,
    product_code,
    product_name,
    product_type,
    product_brand,
    product_unit,
    product_purchase_price,
    product_sale_price,
    is_active,
    is_forbidden_article,
    is_mandatory_article,
    created_at,
    updated_at

from {{ ref('stg_yuman__products') }}
