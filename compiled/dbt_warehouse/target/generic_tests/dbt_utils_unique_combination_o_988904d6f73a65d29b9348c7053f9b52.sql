





with validation_errors as (

    select
        n_planning, code_article
    from `evs-datastack-prod`.`prod_marts`.`fct_technique__consommation_article_nespresso`
    group by n_planning, code_article
    having count(*) > 1

)

select *
from validation_errors


