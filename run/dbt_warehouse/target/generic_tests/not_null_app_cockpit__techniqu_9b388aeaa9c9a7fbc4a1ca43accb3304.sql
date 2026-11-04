
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select technician_name
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_conso_nespresso_detail`
where technician_name is null



  
  
      
    ) dbt_internal_test