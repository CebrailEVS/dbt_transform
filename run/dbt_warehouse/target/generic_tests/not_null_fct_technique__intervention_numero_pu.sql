
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select numero_pu
from `evs-datastack-prod`.`prod_marts`.`fct_technique__intervention`
where numero_pu is null



  
  
      
    ) dbt_internal_test