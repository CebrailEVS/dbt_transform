
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select mois
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_vente_mensuelle`
where mois is null



  
  
      
    ) dbt_internal_test