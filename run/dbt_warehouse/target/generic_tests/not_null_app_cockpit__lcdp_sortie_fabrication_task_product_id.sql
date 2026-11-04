
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select task_product_id
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_sortie_fabrication`
where task_product_id is null



  
  
      
    ) dbt_internal_test