
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_yuman__materials`
      
    
    

    
    OPTIONS(
      description="""Mat\u00e9riaux Marts Yuman enrichie avec les informations cat\u00e9gorie material"""
    )
    as (
      

SELECT 
-- Informations Machine
ym.material_id,
ym.site_id,
ym.material_description,
ym.material_name,
ym.material_brand,
ym.material_serial_number,
ycat.category_name,
ym.material_in_service_date,
ym.created_at,
ym.updated_at
FROM `evs-datastack-prod`.`prod_staging`.`stg_yuman__materials` ym
left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__materials_categories` ycat on ycat.category_id = ym.category_id
    );
  