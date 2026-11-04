
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        ar_ref, no_ref_det, ag_no1, ag_no2, no_operation
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_nomenclat`
    group by ar_ref, no_ref_det, ag_no1, ag_no2, no_operation
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test