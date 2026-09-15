
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select current_is_workorder_paused
from `evs-datastack-prod`.`prod_marts`.`fct_neshu__machine_appro_intervention`
where current_is_workorder_paused is null



  
  
      
    ) dbt_internal_test