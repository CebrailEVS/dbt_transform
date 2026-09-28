
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select fa_code_famille
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_famille`
where fa_code_famille is null



  
  
      
    ) dbt_internal_test