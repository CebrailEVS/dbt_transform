
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select has_retraitement
from `evs-datastack-prod`.`prod_marts`.`fct_technique__intervention_retraitee`
where has_retraitement is null



  
  
      
    ) dbt_internal_test