
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select annee
from `evs-datastack-prod`.`prod_marts`.`mssql_sage__pnl_bu_kpis`
where annee is null



  
  
      
    ) dbt_internal_test