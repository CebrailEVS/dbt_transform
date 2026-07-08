





with validation_errors as (

    select
        stock_date, depot, reference
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__rupture_depot_yuman`
    group by stock_date, depot, reference
    having count(*) > 1

)

select *
from validation_errors


