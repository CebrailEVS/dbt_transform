
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select mois
from `evs-datastack-prod`.`prod_marts`.`fct_finance__pnl_bu`
where mois is null



  
  
      
    ) dbt_internal_test