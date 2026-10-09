
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_fin
from `evs-datastack-prod`.`prod_marts`.`fct_technique__piece_detachee_pricing_nespresso`
where date_fin is null



  
  
      
    ) dbt_internal_test