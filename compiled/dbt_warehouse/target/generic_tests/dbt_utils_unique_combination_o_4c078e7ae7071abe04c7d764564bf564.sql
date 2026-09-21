





with validation_errors as (

    select
        snapshot_month, device_name, device_group
    from (select * from `evs-datastack-prod`.`snapshots`.`snap_oracle_neshu__valo_parc_machines` where dbt_valid_to IS NULL) dbt_subquery
    group by snapshot_month, device_name, device_group
    having count(*) > 1

)

select *
from validation_errors


