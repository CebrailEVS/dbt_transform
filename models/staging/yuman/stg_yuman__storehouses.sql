{{
  config(
    materialized='incremental',
    unique_key='storehouses_id',
    incremental_strategy='merge',
    description='Storehouses Yuman nettoyés depuis l API avec soft delete',
  )
}}

with source_data as (
    select *
    from {{ source('yuman_api', 'yuman_storehouses') }}
),

cleaned_storehouses as (
    select
        id as storehouses_id,
        name as storehouses_name,
        address as storehouses_address,
        cast(_sdc_extracted_at as timestamp) as extracted_at,
        cast(null as timestamp) as deleted_at
    from source_data
)

select *
from cleaned_storehouses

{% if is_incremental() %}

union all

-- lignes supprimées depuis la source
select
    s.storehouses_id,
    s.storehouses_name,
    s.storehouses_address,
    s.extracted_at,
    current_timestamp() as deleted_at
from {{ this }} s
left join cleaned_storehouses c
on s.storehouses_id = c.storehouses_id
where c.storehouses_id is null

{% endif %}
