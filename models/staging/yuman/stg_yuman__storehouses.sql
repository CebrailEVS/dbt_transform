{{
    config(
        materialized = 'table',
        description = 'Storehouses Yuman nettoyés depuis l API'
    )
}}

with source_data as (

    select *
    from {{ source('yuman_api', 'yuman_evs_products_storehouses') }}

),

cleaned_storehouses as (

    select
        id as storehouses_id,
        name as storehouses_name,
        address as storehouses_address,
        _extracted_at as extracted_at
    from source_data

)

select *
from cleaned_storehouses
