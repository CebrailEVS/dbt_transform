
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ls_no_serie
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_lotserie`
where ls_no_serie is null



  
  
      
    ) dbt_internal_test