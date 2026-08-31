



    with source_table as (
        select *
        from `evs-datastack-prod`.`prod_marts`.`dim_lcdp__device`
    )

    select
        -- Identifiants
        device_id,
        device_iddevice,
        device_type_id,
        company_id,
        location_id,

        -- Codes et noms
        device_code,
        device_name,
        company_code,
        company_name,

        -- Caractéristiques machine (labels)
        device_category,
        device_brand,
        device_state,
        device_material_status,
        audit_type,
        typology_da,
        currency_mode,

        -- Types machine (labels)
        fountain_type,
        grinder_type,
        percolator_type,
        type_sp,
        type_dasa,
        model_sp,
        brand_sp,
        badge,

        -- Localisation
        device_location,

        -- Statut
        is_active,

        -- Date de création (immuable, ne déclenche pas de version)
        last_installation_date,
        created_at
    from source_table
