
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_yuman__contacts`
      
    
    

    
    OPTIONS(
      description="""Contacts transform\u00e9s et nettoy\u00e9s depuis l'API Yuman"""
    )
    as (
      

with source_data as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`yuman_contacts`

),

cleaned as (

    select
        id as contact_id,
        client_id,
        site_id,
        category_id,
        name as contact_name,
        title as contact_title,
        phone as contact_phone,
        mobile as contact_mobile,
        email as contact_email,
        observation as contact_observation,
        external_reference,
        external_reference_vendor,
        timestamp(created_at) as created_at,
        timestamp(updated_at) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
    from source_data
    where id is not null

)

select *
from cleaned
    );
  