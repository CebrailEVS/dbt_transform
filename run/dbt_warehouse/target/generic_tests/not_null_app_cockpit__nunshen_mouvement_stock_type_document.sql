
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select type_document
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_mouvement_stock`
where type_document is null



  
  
      
    ) dbt_internal_test