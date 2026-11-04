
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select num_intervention
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_conso_nespresso_detail`
where num_intervention is null



  
  
      
    ) dbt_internal_test