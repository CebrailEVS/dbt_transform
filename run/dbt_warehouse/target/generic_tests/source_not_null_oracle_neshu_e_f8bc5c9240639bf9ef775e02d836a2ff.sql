
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select modification_date
from `evs-datastack-prod`.`prod_raw`.`evs_task_historic`
where modification_date is null



  
  
      
    ) dbt_internal_test