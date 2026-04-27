
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select device_last_installation_date
from `evs-datastack-prod`.`prod_marts`.`fct_technique__neshu_maintenance_preventives`
where device_last_installation_date is null



  
  
      
    ) dbt_internal_test