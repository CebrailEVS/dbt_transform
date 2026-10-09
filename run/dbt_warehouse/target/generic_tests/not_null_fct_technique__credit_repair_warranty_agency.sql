
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select agency
from `evs-datastack-prod`.`prod_marts`.`fct_technique__credit_repair_warranty`
where agency is null



  
  
      
    ) dbt_internal_test