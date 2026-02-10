{{
    config(
        materialized = 'table',
        description = 'Clients nettoyés et enrichis depuis yuman_clients, avec nom du partenaire rattaché.'
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
        safe.parse_json(_embed_fields) as embed_fields,
        timestamp(created_at) as created_at,
        timestamp(updated_at) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
    from source_data
    where id is not null

),

json_parsed as (

    select
        bc.client_id,
        bc.client_code,
        bc.client_name,
        bc.updated_at,
        json_extract_scalar(item, '$.name') as field_name,
        json_extract_scalar(item, '$.value') as field_value
    from base_clients as bc
    cross join unnest(json_extract_array(bc.embed_fields)) as item

),

extracted_fields as (

    select
        client_id,
        client_code,
        client_name,
        updated_at,
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
        max(
            if(
                ef.field_name = 'CATEGORIE CLIENT EVS',
                ef.field_value,
                null
            )
        ) as client_category,
        bc.client_address,
        bc.is_active,
        bc.created_at,
        bc.updated_at,
        bc.extracted_at,
        bc.deleted_at
    from base_clients as bc
    left join extracted_fields as ef
        on bc.client_id = ef.client_id
    group by
        bc.client_id,
        bc.partner_id,
        bc.client_code,
        bc.client_name,
        bc.client_address,
        bc.is_active,
        bc.created_at,
        bc.updated_at,
        bc.extracted_at,
        bc.deleted_at

),

-- Ajout du nom du partenaire via self join
with_partner_name as (

    select
        p.*,
        partner.client_name as partner_name
    from pivoted as p
    left join pivoted as partner
        on p.partner_id = partner.client_id

),

final as (

    select *
    from with_partner_name
    where client_id is not null

)

select
    client_id,
    partner_id,
    partner_name,
    client_code,
    client_name,
    client_category,
    client_address,
    is_active,
    created_at,
    updated_at,
    extracted_at,
    deleted_at
from final
