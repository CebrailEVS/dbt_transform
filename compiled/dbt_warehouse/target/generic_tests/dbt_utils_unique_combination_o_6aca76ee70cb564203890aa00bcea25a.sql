





with validation_errors as (

    select
        n_planning, code_article
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__articles_dedup`
    group by n_planning, code_article
    having count(*) > 1

)

select *
from validation_errors


