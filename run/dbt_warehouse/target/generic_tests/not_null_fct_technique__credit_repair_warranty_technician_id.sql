
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select technician_id
from `evs-datastack-prod`.`prod_marts`.`fct_technique__credit_repair_warranty`
where technician_id is null



  
  
      
    ) dbt_internal_test