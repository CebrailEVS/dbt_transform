
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select intervention
from `evs-datastack-prod`.`prod_marts`.`fct_commerce__machines_avec_interventions`
where intervention is null



  
  
      
    ) dbt_internal_test