



    select
        device_id,
        device_code,
        device_name,
        serial_number,
        device_type_id,
        parent_device_id,
        model_id,
        model_code,
        model_name,
        model_purchase_unit_price,
        company_id,
        company_code,
        company_name,
        company_type_id,
        owner_company_id,
        owner_company_name,
        owner_company_type_id,
        is_active,
        is_reserved,
        device_category,
        device_brand,
        device_range,
        device_state,
        device_material_status,
        purchase_date,
        purchase_cost,
        last_installation_date,
        created_at
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_nayka__device`

