





with validation_errors as (

    select
        idresources, idlabel
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__v_label_resources`
    group by idresources, idlabel
    having count(*) > 1

)

select *
from validation_errors


