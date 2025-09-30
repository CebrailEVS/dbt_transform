
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_yuman__materials_clients`
      
    
    

    
    OPTIONS(
      description="""Dimension materials enrichie avec les informations de sites et de clients\n"""
    )
    as (
      

SELECT 
-- Informations Machine
ym.material_id,
ym.material_description,
ym.material_name,
ym.material_brand,
ym.material_serial_number,
ycat.category_name,
ym.material_in_service_date,
-- Informations Client 
yc.client_id,
yc.client_code,
yc.client_address,
yc.client_name,
yc.partner_name,
-- Informations Site
ys.site_id,
ys.site_address,
ys.site_name, 
ys.site_postal_code
FROM `evs-datastack-prod`.`prod_staging`.`stg_yuman__materials` ym
left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__sites` ys on ym.site_id=ys.site_id
left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__clients` yc on ys.client_id= yc.client_id
left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__materials_categories` ycat on ycat.category_id = ym.category_id
    );
  