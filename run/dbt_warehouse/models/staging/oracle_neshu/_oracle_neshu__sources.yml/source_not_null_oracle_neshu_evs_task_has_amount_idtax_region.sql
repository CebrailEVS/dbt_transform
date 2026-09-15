
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idtax_region
from `evs-datastack-prod`.`prod_raw`.`evs_task_has_amount`
where idtax_region is null



  
  
      
    ) dbt_internal_test