
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select scenario
from `evs-datastack-prod`.`prod_marts`.`fct_mssql_sage__pnl_bu_kpis`
where scenario is null



  
  
      
    ) dbt_internal_test