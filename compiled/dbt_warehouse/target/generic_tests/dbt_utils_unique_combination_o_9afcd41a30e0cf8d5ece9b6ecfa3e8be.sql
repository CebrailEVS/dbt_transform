





with validation_errors as (

    select
        product_id
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__product`
    group by product_id
    having count(*) > 1

)

select *
from validation_errors


