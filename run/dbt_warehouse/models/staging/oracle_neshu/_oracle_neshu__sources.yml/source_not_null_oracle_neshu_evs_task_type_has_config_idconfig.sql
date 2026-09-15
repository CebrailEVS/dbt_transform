
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idconfig
from `evs-datastack-prod`.`prod_raw`.`evs_task_type_has_config`
where idconfig is null



  
  
      
    ) dbt_internal_test