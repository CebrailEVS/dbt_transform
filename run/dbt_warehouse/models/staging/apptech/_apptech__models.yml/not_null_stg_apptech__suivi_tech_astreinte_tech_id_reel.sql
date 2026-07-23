
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select tech_id_reel
from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_astreinte`
where tech_id_reel is null



  
  
      
    ) dbt_internal_test