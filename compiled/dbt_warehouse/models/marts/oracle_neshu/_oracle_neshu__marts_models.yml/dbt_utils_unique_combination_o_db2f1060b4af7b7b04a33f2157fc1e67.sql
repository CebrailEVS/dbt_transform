





with validation_errors as (

    select
        company_id
    from `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__contract`
    group by company_id
    having count(*) > 1

)

select *
from validation_errors


