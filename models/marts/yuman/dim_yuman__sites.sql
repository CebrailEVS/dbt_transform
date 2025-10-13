{{
  config(
    materialized='table',
    description='Dimension sites Yuman'
  )
}}

SELECT 
-- Informations Client 
site_id,
client_id,
agency_id,
site_code,
site_name,
site_address,
site_postal_code,
created_at,
updated_at

FROM {{ref('stg_yuman__sites')}} ym
