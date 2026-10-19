
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select statut
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_equipement_stock`
where statut is null



  
  
      
    ) dbt_internal_test