
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cb_marq
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_ecriturea`
where cb_marq is null



  
  
      
    ) dbt_internal_test