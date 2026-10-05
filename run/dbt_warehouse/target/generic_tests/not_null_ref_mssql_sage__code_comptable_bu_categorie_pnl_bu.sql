
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select categorie_pnl_bu
from `evs-datastack-prod`.`prod_reference`.`ref_mssql_sage__code_comptable_bu`
where categorie_pnl_bu is null



  
  
      
    ) dbt_internal_test