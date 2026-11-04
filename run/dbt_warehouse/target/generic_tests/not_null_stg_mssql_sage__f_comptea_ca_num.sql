
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ca_num
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_comptea`
where ca_num is null



  
  
      
    ) dbt_internal_test