
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select numero
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_commande_fournisseur`
where numero is null



  
  
      
    ) dbt_internal_test