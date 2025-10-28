
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_facturation
from `evs-datastack-prod`.`prod_marts`.`fct_mssql_sage__pnl_bu`
where date_facturation is null



  
  
      
    ) dbt_internal_test