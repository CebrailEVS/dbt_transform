
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idhistoric_action
from `evs-datastack-prod`.`prod_raw`.`evs_historic_action`
where idhistoric_action is null



  
  
      
    ) dbt_internal_test