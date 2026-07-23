
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select a_facturer
from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_mee`
where a_facturer is null



  
  
      
    ) dbt_internal_test