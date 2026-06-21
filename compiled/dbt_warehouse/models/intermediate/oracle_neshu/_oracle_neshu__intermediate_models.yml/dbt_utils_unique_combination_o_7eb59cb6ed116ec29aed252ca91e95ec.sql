





with validation_errors as (

    select
        device_name, device_group
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__valorisation_parc_machines`
    group by device_name, device_group
    having count(*) > 1

)

select *
from validation_errors


