





with validation_errors as (

    select
        immatriculation, date_debut
    from `evs-datastack-prod`.`prod_app_compta`.`app_compta__vehicule_imputation`
    group by immatriculation, date_debut
    having count(*) > 1

)

select *
from validation_errors


