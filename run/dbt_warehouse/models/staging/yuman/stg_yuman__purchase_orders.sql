
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_yuman__purchase_orders`
      
    
    

    
    OPTIONS(
      description="""Purchase Orders d\u00e9pli\u00e9s : une ligne = une commande + une pi\u00e8ce associ\u00e9e"""
    )
    as (
      

with src as (
  select * from `evs-datastack-prod`.`prod_raw`.`yuman_purchase_orders`
),

-- Normalisation / typage des champs entête
po_base as (
  select
    SAFE_CAST(id AS INT64)                            as purchase_order_id,
    number                                           as purchase_order_number,
    status                                           as purchase_order_satus,
    invoice_status                              as purchase_order_invoice_status,
    title                                           as purchase_order_title,
    description                                      as order_description,
    TIMESTAMP(creation_date)                         as creation_date,
    TIMESTAMP(expected_delivery_date)                as expected_delivery_date,
    TIMESTAMP(created_at)                             as order_created_at,
    TIMESTAMP(updated_at)                             as order_updated_at,
    SAFE_CAST(vat AS FLOAT64)                        as order_vat,
    SAFE_CAST(subtotal AS FLOAT64)                   as order_subtotal,
    SAFE_CAST(total AS FLOAT64)                      as order_total,
    SAFE_CAST(manager_id AS INT64)                   as manager_id,
    SAFE_CAST(quote_id AS INT64)                     as quote_id,
    SAFE_CAST(supplier_id AS INT64)                  as supplier_id,
    delivery_address,
    TIMESTAMP(_sdc_extracted_at)                     as extracted_at,
    SAFE_CAST(_sdc_sequence AS INT64)                as sdc_sequence,
    lines
  from src
  where id is not null
),

-- Explode JSON array 'lines' : chaque item devient une ligne
lines_exploded as (
  select
    pb.*,
    item
  from po_base pb,
  unnest(
    -- JSON_EXTRACT_ARRAY ne supporte pas nulls ; SAFE.PARSE_JSON évite les erreurs
    COALESCE(JSON_EXTRACT_ARRAY(SAFE.PARSE_JSON(lines)), []) 
  ) as item
),

-- Extraction des champs de chaque item JSON
line_parsed as (
  select
    purchase_order_id,
    purchase_order_number,
    purchase_order_satus,
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
    sdc_sequence,

    -- champs ligne
    SAFE_CAST(JSON_EXTRACT_SCALAR(item, '$.id') AS INT64)                 as purchase_order_line_id,
    JSON_EXTRACT_SCALAR(item, '$.reference')                             as line_reference,
    JSON_EXTRACT_SCALAR(item, '$.description')                           as line_description,
    SAFE_CAST(JSON_EXTRACT_SCALAR(item, '$.quantity') AS FLOAT64)        as quantity,
    JSON_EXTRACT_SCALAR(item, '$.unit')                                  as unit,
    SAFE_CAST(JSON_EXTRACT_SCALAR(item, '$.quantity_received') AS FLOAT64) as quantity_received,
    SAFE_CAST(JSON_EXTRACT_SCALAR(item, '$.unit_price') AS FLOAT64)      as unit_price,
    SAFE_CAST(JSON_EXTRACT_SCALAR(item, '$.vat') AS FLOAT64)             as line_vat,
    SAFE_CAST(JSON_EXTRACT_SCALAR(item, '$.subtotal') AS FLOAT64)        as line_subtotal,
    SAFE_CAST(JSON_EXTRACT_SCALAR(item, '$.product_id') AS INT64)        as product_id,
    SAFE_DIVIDE(
      SAFE_CAST(JSON_EXTRACT_SCALAR(item, '$.subtotal_received.cents') AS INT64), 
      100
    ) as subtotal_received_eur,
    JSON_EXTRACT_SCALAR(item, '$.subtotal_received.currency_iso')        as subtotal_received_currency,
    TIMESTAMP(JSON_EXTRACT_SCALAR(item, '$.created_at'))                 as line_created_at,
    TIMESTAMP(JSON_EXTRACT_SCALAR(item, '$.updated_at'))                 as line_updated_at
  from lines_exploded
)

select
  -- entête commande (utile pour jointures et analyses)
  purchase_order_id,
  purchase_order_number,
  purchase_order_satus,
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

  -- métadatas
  extracted_at,
  sdc_sequence

from line_parsed
-- si certaines commandes ont lines null on peut garder une ligne (optionnel) :
where purchase_order_line_id is not null
    );
  