





with validation_errors as (

    select
        periode, bad_intervention_id
    from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_rw`
    group by periode, bad_intervention_id
    having count(*) > 1

)

select *
from validation_errors


