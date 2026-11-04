
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select do_date
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docentete`
where do_date is null



  
  
      
    ) dbt_internal_test