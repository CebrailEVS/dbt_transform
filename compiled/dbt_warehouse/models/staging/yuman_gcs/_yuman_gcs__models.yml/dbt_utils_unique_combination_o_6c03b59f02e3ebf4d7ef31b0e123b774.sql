





with validation_errors as (

    select
        export_date, _sdc_source_lineno, _sdc_source_file
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_gcs__stock_theorique`
    group by export_date, _sdc_source_lineno, _sdc_source_file
    having count(*) > 1

)

select *
from validation_errors


