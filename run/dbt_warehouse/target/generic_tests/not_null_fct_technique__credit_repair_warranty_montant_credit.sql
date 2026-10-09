
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select montant_credit
from `evs-datastack-prod`.`prod_marts`.`fct_technique__credit_repair_warranty`
where montant_credit is null



  
  
      
    ) dbt_internal_test