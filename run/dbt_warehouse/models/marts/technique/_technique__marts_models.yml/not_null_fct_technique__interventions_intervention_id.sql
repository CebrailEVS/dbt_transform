
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select intervention_id
from `evs-datastack-prod`.`prod_marts`.`fct_technique__interventions`
where intervention_id is null



  
  
      
    ) dbt_internal_test