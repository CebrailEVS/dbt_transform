
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        dl_no_in, dl_no_out, ls_no_serie
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_lotserie`
    group by dl_no_in, dl_no_out, ls_no_serie
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test