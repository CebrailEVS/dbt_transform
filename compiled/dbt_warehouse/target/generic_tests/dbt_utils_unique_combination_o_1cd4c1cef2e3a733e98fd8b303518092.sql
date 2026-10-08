





with validation_errors as (

    select
        device_name, device_group
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_valorisation_parc_machine`
    group by device_name, device_group
    having count(*) > 1

)

select *
from validation_errors


