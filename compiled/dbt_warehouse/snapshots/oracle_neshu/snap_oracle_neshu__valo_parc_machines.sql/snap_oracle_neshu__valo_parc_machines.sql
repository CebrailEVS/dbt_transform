



    select
        timestamp(date_trunc(current_date(), month)) as snapshot_month,
        device_name,
        device_group,
        nombre_machines,
        valorisation_totale_machine
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__valorisation_parc_machines`
