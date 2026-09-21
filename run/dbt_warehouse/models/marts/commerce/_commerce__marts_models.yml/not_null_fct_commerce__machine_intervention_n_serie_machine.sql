
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select n_serie_machine
from `evs-datastack-prod`.`prod_marts`.`fct_commerce__machine_intervention`
where n_serie_machine is null



  
  
      
    ) dbt_internal_test