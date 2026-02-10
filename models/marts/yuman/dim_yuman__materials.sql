{{ config(
    materialized='table',
    description='Dimension materials enrichie avec les informations catégorie material'
) }}

select
    ym.material_id,
    ym.site_id,
    ym.material_description,
    ym.material_name,
    ym.material_brand,
    ym.material_serial_number,
    ycat.category_name,
    ym.material_in_service_date,
    ym.created_at,
    ym.updated_at

from {{ ref('stg_yuman__materials') }} as ym
left join {{ ref('stg_yuman__materials_categories') }} as ycat
    on ym.category_id = ycat.category_id
