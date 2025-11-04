
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select designation_compte
from `evs-datastack-prod`.`prod_reference`.`ref_mssql_sage__code_comptable_bu`
where designation_compte is null



  
  
      
    ) dbt_internal_test