
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_yuman__users`
      
    
    

    
    OPTIONS(
      description="""Utilisateurs transform\u00e9s et nettoy\u00e9s depuis l'API Yuman"""
    )
    as (
      

with source_data as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`yuman_users`

),

cleaned_users as (

    select
        id as user_id,
        manager_id,
        (
            select json_value(field, '$.value')
            from unnest(json_query_array(_embed_fields)) as field
            where json_value(field, '$.name') = 'ID NOMAD'
        ) as nomad_id,
        (
            select json_value(field, '$.value')
            from unnest(json_query_array(_embed_fields)) as field
            where json_value(field, '$.name') = 'SECTEUR'
        ) as user_secteur,
        name as user_name,
        email as user_email,
        user_type,
        phone as user_phone,
        manager_as_technician as is_manager_as_technician,
        timestamp(created_at) as created_at,
        timestamp(updated_at) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at

    from source_data

)

select *
from cleaned_users
    );
  