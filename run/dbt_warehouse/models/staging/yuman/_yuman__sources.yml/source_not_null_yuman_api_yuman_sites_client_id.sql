
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select client_id
from `evs-datastack-prod`.`prod_raw`.`yuman_sites`
where client_id is null



  
  
      
    ) dbt_internal_test