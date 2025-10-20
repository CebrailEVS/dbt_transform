





with validation_errors as (

    select
        ec_no, n_analytique, ea_ligne
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_ecriturea`
    group by ec_no, n_analytique, ea_ligne
    having count(*) > 1

)

select *
from validation_errors


