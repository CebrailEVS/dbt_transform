{{
    config(
        materialized = 'table',
        description = 'Materials (Machines) nettoyés depuis yuman_materials'
    )
}}

with source_data as (

    select *
    from {{ source('yuman_api', 'yuman_materials') }}

),

cleaned_materials as (

    select
        id as material_id,
        site_id,
        category_id,
        name as material_name,
        serial_number as material_serial_number,
        brand as material_brand,
        description as material_description,
        in_service_date as material_in_service_date,

        (
            select json_value(elem, '$.value')
            from unnest(json_query_array(_embed_fields)) as elem
            where json_value(elem, '$.name') = 'LOCALISATION'
        ) as material_localisation,

        timestamp(created_at) as created_at,
        timestamp(updated_at) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
    from source_data
    where id is not null

)

select *
from cleaned_materials
