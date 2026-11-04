
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_yuman__materials_categories`
      
    
    

    
    OPTIONS(
      description="""Cat\u00e9gories de mat\u00e9riels Yuman (API Yuman) : r\u00e9f\u00e9rentiel des cat\u00e9gories de machines, une ligne par cat\u00e9gorie."""
    )
    as (
      

with source_data as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`yuman_evs_materials_categories`

),

cleaned_material_categories as (

    select
        id as category_id,
        name as category_name,
        timestamp(created_at) as created_at,
        timestamp(updated_at) as updated_at,
        _extracted_at as extracted_at
    from source_data
    where id is not null

)

select *
from cleaned_material_categories
    );
  