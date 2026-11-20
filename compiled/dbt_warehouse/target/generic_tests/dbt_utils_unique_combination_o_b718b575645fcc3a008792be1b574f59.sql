





with validation_errors as (

    select
        code_article
    from `evs-datastack-prod`.`prod_app_suivi_tech`.`app_suivi_tech__technique_piece_nespresso`
    group by code_article
    having count(*) > 1

)

select *
from validation_errors


