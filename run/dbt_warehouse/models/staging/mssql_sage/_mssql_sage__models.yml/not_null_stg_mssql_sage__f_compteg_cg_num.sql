
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cg_num
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_compteg`
where cg_num is null



  
  
      
    ) dbt_internal_test