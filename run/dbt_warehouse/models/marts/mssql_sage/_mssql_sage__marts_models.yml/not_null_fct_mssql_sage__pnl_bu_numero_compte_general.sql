
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select numero_compte_general
from `evs-datastack-prod`.`prod_marts`.`fct_mssql_sage__pnl_bu`
where numero_compte_general is null



  
  
      
    ) dbt_internal_test