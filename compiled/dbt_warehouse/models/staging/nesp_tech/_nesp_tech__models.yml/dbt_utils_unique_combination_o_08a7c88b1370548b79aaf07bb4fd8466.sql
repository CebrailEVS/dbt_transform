





with validation_errors as (

    select
        n_planning, code_article, date_intervention
    from `evs-datastack-prod`.`prod_staging`.`stg_nesp_tech__articles`
    group by n_planning, code_article, date_intervention
    having count(*) > 1

)

select *
from validation_errors


