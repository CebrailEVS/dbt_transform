{{
    config(
        materialized='table',
        partition_by = {"field": "date_system", "data_type": "timestamp"},
        cluster_by = ['id_entity', 'resources_code'],
        description='Table des stocks théoriques depuis les fichiers GCS Oracle Neshu'
    )
}}

WITH raw AS (
    SELECT
        CAST(id_entity AS INT64) AS id_entity,  
        entity_name,
        entity_type,
        CAST(DATE_SYSTEM AS TIMESTAMP) AS date_system,
        resources_code,
        code_source AS product_code,
        code_name AS product_name,
        SAFE.PARSE_TIMESTAMP('%d/%m/%Y %H:%M', DATE_INVENTAIRE) AS date_inventaire,
        CAST(STOCK_INVENTAIRE AS NUMERIC) AS stock_inventaire,
        CAST(PLUS AS NUMERIC) AS plus,
        CAST(MOINS AS NUMERIC) AS moins,
        CAST(STOCK_AT_DATE AS NUMERIC) AS stock_at_date,
        CAST(DPA AS NUMERIC) AS dpa,
        CAST(PUMP AS NUMERIC) AS pump,
        CAST(PURCHASE_PRICE AS NUMERIC) AS purchase_price,
        CAST(EXTRACTION_TIMESTAMP AS TIMESTAMP) AS extraction_timestamp,
        ROW_COUNT,
        file_datetime
    FROM {{ ref('raw_oracle_neshu__stock_theorique') }}
)

SELECT * FROM raw
