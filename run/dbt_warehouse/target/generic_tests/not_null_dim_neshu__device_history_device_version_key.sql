
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select device_version_key
from `evs-datastack-prod`.`prod_marts`.`dim_neshu__device_history`
where device_version_key is null



  
  
      
    ) dbt_internal_test