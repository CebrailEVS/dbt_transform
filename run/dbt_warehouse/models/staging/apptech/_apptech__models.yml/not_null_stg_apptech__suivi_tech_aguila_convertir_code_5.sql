
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select convertir_code_5
from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_aguila`
where convertir_code_5 is null



  
  
      
    ) dbt_internal_test