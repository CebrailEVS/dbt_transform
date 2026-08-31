



    with source_table as (
        select *
        from `evs-datastack-prod`.`prod_staging`.`stg_yuman__users`
    )

    select
        user_id,
        manager_id,         -- TRACKED
        nomad_id,           -- TRACKED
        user_name,          -- TRACKED
        user_email,
        user_type,          -- TRACKED
        user_phone,
        user_secteur,       -- TRACKED
        is_manager_as_technician, -- TRACKED
        is_active,          -- TRACKED (derived from user_inactif)
        created_at,
        updated_at,
        extracted_at
    from source_table
