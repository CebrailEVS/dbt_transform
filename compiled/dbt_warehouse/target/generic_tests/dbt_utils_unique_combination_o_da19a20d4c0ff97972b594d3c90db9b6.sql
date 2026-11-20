





with validation_errors as (

    select
        n_planning
    from `evs-datastack-prod`.`prod_app_suivi_tech`.`app_suivi_tech__technique_alerte_conso_aguila`
    group by n_planning
    having count(*) > 1

)

select *
from validation_errors


