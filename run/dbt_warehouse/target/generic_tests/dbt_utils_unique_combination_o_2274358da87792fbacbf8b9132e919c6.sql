
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        extracted_at, cb_marq
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_lotserie`
    group by extracted_at, cb_marq
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test