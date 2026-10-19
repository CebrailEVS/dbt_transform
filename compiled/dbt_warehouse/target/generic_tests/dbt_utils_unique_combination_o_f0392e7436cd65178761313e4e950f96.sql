





with validation_errors as (

    select
        company_id, product_id
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__classification_article`
    group by company_id, product_id
    having count(*) > 1

)

select *
from validation_errors


