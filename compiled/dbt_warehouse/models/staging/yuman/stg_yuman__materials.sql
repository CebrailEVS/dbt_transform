

with source_data as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`yuman_evs_materials`

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
        active as is_active,

        (
            select json_value(elem, '$.value')
            from unnest(json_query_array(_embed, '$.fields')) as elem
            where json_value(elem, '$.name') = 'LOCALISATION'
        ) as material_localisation,

        timestamp(created_at) as created_at,
        timestamp(updated_at) as updated_at,
        _extracted_at as extracted_at
    from source_data
    where id is not null

)

select *
from cleaned_materials