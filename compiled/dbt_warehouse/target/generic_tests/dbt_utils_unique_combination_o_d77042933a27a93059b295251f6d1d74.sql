





with validation_errors as (

    select
        key_ref_inter, valid_from
    from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__key_facturation`
    group by key_ref_inter, valid_from
    having count(*) > 1

)

select *
from validation_errors


