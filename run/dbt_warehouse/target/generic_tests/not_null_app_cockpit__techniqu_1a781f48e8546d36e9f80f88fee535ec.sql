
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select piece_prix_unitaire
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_prix_nespresso`
where piece_prix_unitaire is null



  
  
      
    ) dbt_internal_test