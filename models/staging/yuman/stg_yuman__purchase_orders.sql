{{
    config(
        materialized = 'table',
        description = 'Staging "flattened" : une ligne = 1 commande + 1 pièce (dépliage du champ json lines).'
    )
}}

with src as (

    select *
    from {{ source('yuman_api', 'yuman_evs_purchase_orders') }}

),

-- Normalisation / typage des champs entête
po_base as (

    select
        safe_cast(id as int64) as purchase_order_id,
        number as purchase_order_number,
        status as purchase_order_status,
        invoice_status as purchase_order_invoice_status,
        title as purchase_order_title,
        description as order_description,
        timestamp(creation_date) as creation_date,
        timestamp(expected_delivery_date) as expected_delivery_date,
        timestamp(created_at) as order_created_at,
        timestamp(updated_at) as order_updated_at,
        safe_cast(vat as float64) as order_vat,
        safe_cast(subtotal as float64) as order_subtotal,
        safe_cast(total as float64) as order_total,
        safe_cast(manager_id as int64) as manager_id,
        safe_cast(quote_id as int64) as quote_id,
        safe_cast(supplier_id as int64) as supplier_id,
        delivery_address,
        _extracted_at as extracted_at,
        lines
    from src
    where id is not null

),

-- Explode json array 'lines' : chaque item devient une ligne
lines_exploded as (

    select
        pb.*,
        item
    from po_base as pb
    cross join unnest(
        -- `lines` est une colonne JSON NATIVE depuis dlt (Meltano rendait une
        -- chaîne, d'où le parse_json d'avant). json_query_array gère le NULL.
        coalesce(json_query_array(lines), [])
    ) as item

),

-- Extraction des champs de chaque item json
line_parsed as (

    select
        purchase_order_id,
        purchase_order_number,
        purchase_order_status,
        purchase_order_invoice_status,
        purchase_order_title,
        order_description,
        creation_date,
        expected_delivery_date,
        order_created_at,
        order_updated_at,
        order_vat,
        order_subtotal,
        order_total,
        manager_id,
        quote_id,
        supplier_id,
        delivery_address,
        extracted_at,

        -- champs ligne
        safe_cast(json_extract_scalar(item, '$.id') as int64) as purchase_order_line_id,
        json_extract_scalar(item, '$.reference') as line_reference,
        json_extract_scalar(item, '$.description') as line_description,
        safe_cast(json_extract_scalar(item, '$.quantity') as float64) as quantity,
        json_extract_scalar(item, '$.unit') as unit,
        safe_cast(json_extract_scalar(item, '$.quantity_received') as float64) as quantity_received,
        safe_cast(json_extract_scalar(item, '$.unit_price') as float64) as unit_price,
        safe_cast(json_extract_scalar(item, '$.vat') as float64) as line_vat,
        safe_cast(json_extract_scalar(item, '$.subtotal') as float64) as line_subtotal,
        safe_cast(json_extract_scalar(item, '$.product_id') as int64) as product_id,
        safe_divide(
            safe_cast(json_extract_scalar(item, '$.subtotal_received.cents') as int64),
            100
        ) as subtotal_received_eur,
        json_extract_scalar(item, '$.subtotal_received.currency_iso') as subtotal_received_currency,
        timestamp(json_extract_scalar(item, '$.created_at')) as line_created_at,
        timestamp(json_extract_scalar(item, '$.updated_at')) as line_updated_at
    from lines_exploded

)

select
    -- entête commande (utile pour jointures et analyses)
    purchase_order_id,
    purchase_order_number,
    purchase_order_status,
    purchase_order_invoice_status,
    purchase_order_title,
    order_description,
    creation_date,
    expected_delivery_date,
    order_created_at,
    order_updated_at,
    order_vat,
    order_subtotal,
    order_total,
    manager_id,
    quote_id,
    supplier_id,
    delivery_address,

    -- champs ligne (une ligne par item)
    purchase_order_line_id,
    line_reference,
    line_description,
    quantity,
    unit,
    quantity_received,
    unit_price,
    line_vat,
    line_subtotal,
    product_id,
    subtotal_received_eur,
    subtotal_received_currency,
    line_created_at,
    line_updated_at,

    -- métadonnées
    extracted_at
from line_parsed
where purchase_order_line_id is not null
