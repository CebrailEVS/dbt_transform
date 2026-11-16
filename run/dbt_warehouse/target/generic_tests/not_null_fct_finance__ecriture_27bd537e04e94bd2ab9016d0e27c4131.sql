
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ecriture_comptable_id
from `evs-datastack-prod`.`prod_marts`.`fct_finance__ecriture_non_ventilee`
where ecriture_comptable_id is null



  
  
      
    ) dbt_internal_test