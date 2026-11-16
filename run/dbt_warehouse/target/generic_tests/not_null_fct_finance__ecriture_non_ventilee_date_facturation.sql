
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_facturation
from `evs-datastack-prod`.`prod_marts`.`fct_finance__ecriture_non_ventilee`
where date_facturation is null



  
  
      
    ) dbt_internal_test