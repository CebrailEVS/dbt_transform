
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select load_id
from `evs-datastack-prod`.`prod_raw`.`_dlt_loads`
where load_id is null



  
  
      
    ) dbt_internal_test