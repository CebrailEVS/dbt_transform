

with source_data as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`yuman_users`

),

cleaned_users as (

    select
        id as user_id,
        manager_id,
        nullif((
            select json_value(field, '$.value')
            from unnest(json_query_array(_embed_fields)) as field
            where json_value(field, '$.name') = 'ID NOMAD'
        ), '') as nomad_id,
        nullif((
            select json_value(field, '$.value')
            from unnest(json_query_array(_embed_fields)) as field
            where json_value(field, '$.name') = 'SECTEUR'
        ), '') as user_secteur,
        name as user_name,
        email as user_email,
        user_type,
        nullif(phone, '') as user_phone,
        manager_as_technician as is_manager_as_technician,
        (
            select json_value(field, '$.value')
            from unnest(json_query_array(_embed_fields)) as field
            where json_value(field, '$.name') = 'INACTIF'
        ) as user_inactif,
        timestamp(created_at) as created_at,
        timestamp(updated_at) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at

    from source_data

),

final as (

    select
        user_id,
        manager_id,
        nomad_id,
        user_secteur,
        user_name,
        user_email,
        user_type,
        user_phone,
        is_manager_as_technician,
        lower(user_inactif) != 'oui' as is_active,
        created_at,
        updated_at,
        extracted_at,
        deleted_at

    from cleaned_users

)

select *
from final