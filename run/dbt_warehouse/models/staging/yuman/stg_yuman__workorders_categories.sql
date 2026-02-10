
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorders_categories`
      
    
    

    
    OPTIONS(
      description=""""""
    )
    as (
      

with source_data as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`yuman_workorder_categories`

),

cleaned_categories as (

    select
        id as category_id,
        name as category_name,
        timestamp(created_at) as created_at,
        timestamp(updated_at) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
    from source_data

)

select *
from cleaned_categories
    );
  