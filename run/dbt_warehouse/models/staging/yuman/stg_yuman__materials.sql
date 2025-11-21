
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_yuman__materials`
      
    
    

    
    OPTIONS(
      description="""Mat\u00e9riaux transform\u00e9s et nettoy\u00e9s depuis l'API Yuman"""
    )
    as (
      

with source_data as (
    select * 
    from `evs-datastack-prod`.`prod_raw`.`yuman_materials`
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
          select JSON_VALUE(elem, '$.value')
          from UNNEST(JSON_QUERY_ARRAY(_embed_fields)) elem
          where JSON_VALUE(elem, '$.name') = 'LOCALISATION'
        ) as material_localisation,

        timestamp(created_at) as created_at,
        timestamp(updated_at) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at

    from source_data
    where id is not null
)

select * from cleaned_materials
    );
  