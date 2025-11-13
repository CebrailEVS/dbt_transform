{{ config(
    materialized = 'table',
    schema = 'raw'
) }}

SELECT
  *,
  PARSE_DATETIME('%Y_%m_%d_%H%M', REGEXP_EXTRACT(_FILE_NAME, r'(\d{4}_\d{2}_\d{2}_\d{4})')) AS file_datetime
FROM {{ source('oracle_neshu_gcs', 'stock_theorique_ext') }}
