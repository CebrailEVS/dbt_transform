
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select extracted_at
from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_pause`
where extracted_at is null



  
  
      
    ) dbt_internal_test