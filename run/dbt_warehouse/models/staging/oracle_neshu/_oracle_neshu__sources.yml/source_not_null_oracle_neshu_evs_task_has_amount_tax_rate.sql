
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select tax_rate
from `evs-datastack-prod`.`prod_raw`.`evs_task_has_amount`
where tax_rate is null



  
  
      
    ) dbt_internal_test