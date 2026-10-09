



    select
        purchase_order_line_id,
        purchase_order_id,
        purchase_order_number,
        purchase_order_status,      -- TRACKED
        supplier_id,
        line_reference,
        quantity,
        quantity_received,          -- TRACKED
        unit_price,
        creation_date,
        expected_delivery_date,
        line_updated_at,
        extracted_at
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__purchase_orders`
    where purchase_order_line_id is not null

