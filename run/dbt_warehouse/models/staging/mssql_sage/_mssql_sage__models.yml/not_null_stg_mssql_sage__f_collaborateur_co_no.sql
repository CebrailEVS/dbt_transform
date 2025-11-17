
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select co_no
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_collaborateur`
where co_no is null



  
  
      
    ) dbt_internal_test