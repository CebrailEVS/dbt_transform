
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_active
from `evs-datastack-prod`.`prod_staging`.`stg_yuman__users`
where is_active is null



  
  
      
    ) dbt_internal_test