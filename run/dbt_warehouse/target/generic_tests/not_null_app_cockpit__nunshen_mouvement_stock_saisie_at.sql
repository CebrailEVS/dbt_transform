
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select saisie_at
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_mouvement_stock`
where saisie_at is null



  
  
      
    ) dbt_internal_test