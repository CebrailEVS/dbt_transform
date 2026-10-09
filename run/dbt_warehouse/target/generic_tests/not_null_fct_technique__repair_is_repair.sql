
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_repair
from `evs-datastack-prod`.`prod_marts`.`fct_technique__repair`
where is_repair is null



  
  
      
    ) dbt_internal_test