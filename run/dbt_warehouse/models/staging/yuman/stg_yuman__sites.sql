
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_yuman__sites`
      
    
    

    
    OPTIONS(
      description="""Sites transform\u00e9s et nettoy\u00e9s depuis l'API Yuman"""
    )
    as (
      

with source_data as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`yuman_sites`

),

extracted_postal_code as (

    select
        sd.*,
        (
            select json_extract_scalar(elem, '$.value')
            from unnest(json_extract_array(sd._embed_fields)) as elem
            where json_extract_scalar(elem, '$.name') = 'CODE POSTAL'
        ) as raw_code_postal
    from source_data as sd

),

cleaned as (

    select
        id as site_id,
        client_id,
        agency_id,
        code as site_code,
        name as site_name,
        address as site_address,
        -- nettoyage du code postal : suppression du '.0'
        regexp_replace(raw_code_postal, r'\.0$', '') as site_postal_code,
        timestamp(created_at) as created_at,
        timestamp(updated_at) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
    from extracted_postal_code

)

select *
from cleaned
    );
  