





with validation_errors as (

    select
        company_id, device_id, product_id, location_id, location, consumption_date, data_source
    from `evs-datastack-prod`.`prod_marts`.`fct_neshu__consommation`
    group by company_id, device_id, product_id, location_id, location, consumption_date, data_source
    having count(*) > 1

)

select *
from validation_errors


