





with validation_errors as (

    select
        periode, intervention_id
    from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_modif_intervention`
    group by periode, intervention_id
    having count(*) > 1

)

select *
from validation_errors


