





with validation_errors as (

    select
        contrat_immatriculation_edi
    from `evs-datastack-prod`.`prod_intermediate`.`int_gac__vehicule_code_analytique`
    group by contrat_immatriculation_edi
    having count(*) > 1

)

select *
from validation_errors


