





with validation_errors as (

    select
        snapshot_date, contrat_id_gac, date_d_effet
    from `evs-datastack-prod`.`prod_staging`.`stg_gac__historique_affectation`
    group by snapshot_date, contrat_id_gac, date_d_effet
    having count(*) > 1

)

select *
from validation_errors


