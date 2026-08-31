



    with source_table as (
        select *
        from `evs-datastack-prod`.`prod_marts`.`dim_neshu__device`
    )

    select
        device_id,
        device_iddevice,
        device_type_id,
        company_id,
        location_id,
        device_code,
        device_name,
        company_code,              -- TRACKED for changes
        company_name,
        device_brand,              -- TRACKED for changes
        device_gamme,
        device_category,
        device_economic_model,      -- TRACKED for changes
        device_location,            -- TRACKED for changes
        is_active,
        last_installation_date,
        created_at,
        updated_at
    from source_table
