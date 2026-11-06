
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ligne_id
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_equipement_stock`
where ligne_id is null



  
  
      
    ) dbt_internal_test