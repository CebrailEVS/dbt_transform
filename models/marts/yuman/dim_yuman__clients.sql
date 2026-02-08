{{
  config(
    materialized='table',
    description='Dimension clients Yuman'
  )
}}

SELECT 
-- Informations Client 
client_id,
partner_name,
client_code,
client_name,
client_category,
client_address,
is_active as client_is_active,
created_at,
updated_at

FROM {{ref('stg_yuman__clients')}} ym
