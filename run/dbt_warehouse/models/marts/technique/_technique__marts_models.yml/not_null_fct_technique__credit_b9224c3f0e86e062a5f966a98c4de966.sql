
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select key_inter_fautive
from `evs-datastack-prod`.`prod_marts`.`fct_technique__credit_repair_warranty`
where key_inter_fautive is null



  
  
      
    ) dbt_internal_test