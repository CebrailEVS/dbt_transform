





with validation_errors as (

    select
        reference, stock, stock_date
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_yuman`
    group by reference, stock, stock_date
    having count(*) > 1

)

select *
from validation_errors


