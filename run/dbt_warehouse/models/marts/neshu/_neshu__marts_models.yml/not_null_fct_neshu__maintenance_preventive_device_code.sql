
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select device_code
from `evs-datastack-prod`.`prod_marts`.`fct_neshu__maintenance_preventive`
where device_code is null



  
  
      
    ) dbt_internal_test