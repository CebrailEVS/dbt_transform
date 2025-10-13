
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_yuman__sites`
      
    
    

    
    OPTIONS(
      description="""Sites Marts Yuman"""
    )
    as (
      

SELECT 
-- Informations Client 
site_id,
client_id,
agency_id,
site_code,
site_name,
site_address,
site_postal_code,
created_at,
updated_at

FROM `evs-datastack-prod`.`prod_staging`.`stg_yuman__sites` ym
    );
  