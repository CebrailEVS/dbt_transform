
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select numero_ecriture_comptable
from `evs-datastack-prod`.`prod_intermediate`.`int_mssql_sage__pnl_bu`
where numero_ecriture_comptable is null



  
  
      
    ) dbt_internal_test