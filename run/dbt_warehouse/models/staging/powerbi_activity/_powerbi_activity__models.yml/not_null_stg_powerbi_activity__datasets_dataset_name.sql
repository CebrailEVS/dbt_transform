
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dataset_name
from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__datasets`
where dataset_name is null



  
  
      
    ) dbt_internal_test