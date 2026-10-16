





with validation_errors as (

    select
        code, idproduct_type
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__product`
    group by code, idproduct_type
    having count(*) > 1

)

select *
from validation_errors


