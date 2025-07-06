{{ 
  config(
    materialized='table',
    description='Clients nettoyés et enrichis depuis yuman_clients',
  ) 
}}

with source_data as (
    select * 
    from {{ source('yuman_api', 'yuman_clients') }}
),

base_clients as (
    select
        id as client_id,
        partner_id, 
        code as client_code,
        name as client_name,
        address as client_address,       
        active as is_active,
        SAFE.PARSE_JSON(_embed_fields) as embed_fields,
        TIMESTAMP(created_at) as created_at,
        TIMESTAMP(updated_at) as last_updated,
        TIMESTAMP(_sdc_extracted_at) as extracted_at,
        TIMESTAMP(_sdc_deleted_at) as deleted_at
    from source_data
    where id is not null
),

-- Option 1: Si embed_fields est un tableau JSON
json_parsed as (
    select
        client_id,
        client_code,
        client_name,
        last_updated,
        JSON_EXTRACT_SCALAR(item, '$.name') as field_name,
        JSON_EXTRACT_SCALAR(item, '$.value') as field_value
    from base_clients,
    UNNEST(JSON_EXTRACT_ARRAY(embed_fields)) as item
),

extracted_fields as (
    select
        client_id,
        client_code,
        client_name,
        last_updated,
        field_name,
        field_value
    from json_parsed
    where field_name is not null
),

pivoted as (
    select
        bc.client_id,
        bc.partner_id,
        bc.client_code,
        bc.client_name,
        MAX(IF(ef.field_name = 'CATEGORIE CLIENT EVS', ef.field_value, NULL)) as client_category,
        bc.client_address,
        bc.is_active,
        bc.created_at,
        bc.last_updated,
        bc.extracted_at,
        bc.deleted_at
    from base_clients bc
    left join extracted_fields ef on ef.client_id = bc.client_id
    group by 
        bc.client_id, bc.client_code, bc.client_name, bc.client_address,
        bc.partner_id, bc.is_active,
        bc.created_at, bc.last_updated, bc.extracted_at, bc.deleted_at
),

final as (
    select * from pivoted
    where client_id is not null  -- Assurer que client_id n'est jamais null
)

select 
    client_id,
    partner_id,
    client_code,
    client_name,
    client_category,
    client_address,
    is_active,
    created_at,
    last_updated,
    extracted_at,
    deleted_at
from final