{{
    config(
        materialized = 'table',
        partition_by = {"field": "date_system", "data_type": "timestamp"},
        description = 'Table des stocks théoriques depuis les fichiers GCS Oracle Neshu'
    )
}}

select
    cast(id_entity as int64) as id_entity,
    lower(entity_name) as entity_name,
    lower(entity_type) as entity_type,
    cast(date_system as timestamp) as date_system,
    resources_code,
    code_source as product_code,
    code_name as product_name,
    safe.parse_timestamp('%d/%m/%Y %H:%M', date_inventaire) as date_inventaire,
    cast(stock_inventaire as numeric) as stock_inventaire,
    cast(plus as numeric) as plus,
    cast(moins as numeric) as moins,
    cast(stock_at_date as numeric) as stock_at_date,
    cast(dpa as numeric) as dpa,
    cast(pump as numeric) as pump,
    cast(purchase_price as numeric) as purchase_price,
    cast(extracted_at as timestamp) as extracted_at,
    row_count,
    parse_datetime('%Y_%m_%d_%H%M', regexp_extract(_file_name, r'(\d{4}_\d{2}_\d{2}_\d{4})')) as file_datetime
from {{ source('oracle_neshu_gcs', 'ext_gcs_oracle_neshu__stock_theorique') }}
