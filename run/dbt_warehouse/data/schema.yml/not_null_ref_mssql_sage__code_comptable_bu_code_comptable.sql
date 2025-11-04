
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select code_comptable
from `evs-datastack-prod`.`prod_reference`.`ref_mssql_sage__code_comptable_bu`
where code_comptable is null



  
  
      
    ) dbt_internal_test