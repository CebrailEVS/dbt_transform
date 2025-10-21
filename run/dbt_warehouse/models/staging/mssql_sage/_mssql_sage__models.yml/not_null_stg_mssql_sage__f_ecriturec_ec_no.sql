
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ec_no
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_ecriturec`
where ec_no is null



  
  
      
    ) dbt_internal_test