



    with source_table as (
        select *
        from `evs-datastack-prod`.`prod_staging`.`stg_yuman__storehouses`
    )

    select
        storehouses_id,
        storehouses_name,       -- TRACKED for changes
        storehouses_address,
        extracted_at
    from source_table
