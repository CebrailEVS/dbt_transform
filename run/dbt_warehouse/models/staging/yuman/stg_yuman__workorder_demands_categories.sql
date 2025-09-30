
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorder_demands_categories`
      
    
    

    
    OPTIONS(
      description="""Cat\u00e9gories de demandes d'intervention transform\u00e9es et nettoy\u00e9es depuis l'API Yuman"""
    )
    as (
      

with source_data as (
    select * from `evs-datastack-prod`.`prod_raw`.`yuman_workorder_demands_categories`
),

cleaned_categories as (
    select
        id as demand_category_id,
        name as demand_category_name,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(_sdc_extracted_at as timestamp) as extracted_at,
        cast(_sdc_deleted_at as timestamp) as deleted_at
    from source_data
)

select * from cleaned_categories
    );
  