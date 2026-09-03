
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select activity
from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__events`
where activity is null



  
  
      
    ) dbt_internal_test