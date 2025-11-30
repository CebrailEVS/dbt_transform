
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        export_date, _sdc_source_lineno
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_gcs__stock_theorique`
    group by export_date, _sdc_source_lineno
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test