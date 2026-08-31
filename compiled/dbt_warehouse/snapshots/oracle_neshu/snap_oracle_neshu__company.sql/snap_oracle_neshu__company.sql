



    with source_table as (

        select *
        from `evs-datastack-prod`.`prod_marts`.`dim_neshu__company`

    )

    select
        -- Identité
        company_id,
        company_type_id,
        company_code,
        company_name,
        company_type,

        -- Organisation / classification
        region,
        sector,
        sector_code,
        activity_sector,
        employee_range,

        -- Modèle économique & relation client
        company_economic_model,
        client_status,
        key_account,

        -- Offres / options
        katiers,
        remote_work,
        proadman,
        gsm,
        badge,
        recycling,

        -- Statut
        is_active,

        -- Localisation
        address1,
        address2,
        city,
        postal_code,
        country,

        -- Métadonnées source
        created_at,
        updated_at

    from source_table
