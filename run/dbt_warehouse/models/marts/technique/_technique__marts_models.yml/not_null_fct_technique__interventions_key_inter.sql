
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select key_inter
from `evs-datastack-prod`.`prod_marts`.`fct_technique__interventions`
where key_inter is null



  
  
      
    ) dbt_internal_test