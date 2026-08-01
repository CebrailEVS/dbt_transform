{{
    config(
        materialized = 'table',
        description = 'Catégories dintervention nettoyés depuis l Yuman API'
    )
}}

with source_data as (

    select *
    from {{ source('yuman_api', 'yuman_evs_workorders_categories') }}

),

cleaned_categories as (

    select
        id as category_id,
        name as category_name,
        timestamp(created_at) as created_at,
        timestamp(updated_at) as updated_at,
        _extracted_at as extracted_at
    from source_data

)

select *
from cleaned_categories
