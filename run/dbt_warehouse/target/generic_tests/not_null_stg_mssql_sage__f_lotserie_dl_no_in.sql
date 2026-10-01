
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dl_no_in
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_lotserie`
where dl_no_in is null



  
  
      
    ) dbt_internal_test