
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_reference
from `evs-datastack-prod`.`prod_app_suivi_tech`.`app_suivi_tech__technique_piece_nespresso`
where product_reference is null



  
  
      
    ) dbt_internal_test