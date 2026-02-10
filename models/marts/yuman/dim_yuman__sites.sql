{{ config(
    materialized='table',
    description='Dimension sites Yuman'
) }}

select
    site_id,
    client_id,
    agency_id,
    site_code,
    site_name,
    site_address,
    site_postal_code,
    created_at,
    updated_at

from {{ ref('stg_yuman__sites') }}
