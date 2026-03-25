
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select budg_annee
from `evs-datastack-prod`.`prod_reference`.`ref_mssql_sage__pnl_budget`
where budg_annee is null



  
  
      
    ) dbt_internal_test