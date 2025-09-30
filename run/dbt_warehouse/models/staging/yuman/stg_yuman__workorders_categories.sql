
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorders_categories`
      
    
    

    
    OPTIONS(
      description=""""""
    )
    as (
      

with source_data as (
    select * from `evs-datastack-prod`.`prod_raw`.`yuman_workorder_categories`
),

cleaned_categories as (
    select
        id as category_id,
        name as category_name,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(_sdc_extracted_at as timestamp) as extracted_at,
        cast(_sdc_deleted_at as timestamp) as deleted_at
    from source_data
)

select * from cleaned_categories
    );
  