
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select commande_date
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_commande_delai`
where commande_date is null



  
  
      
    ) dbt_internal_test