{{
    config(
        materialized = 'table',
        description = 'Fournisseurs Yuman nettoyés depuis l API'
    )
}}

with source_data as (

    select *
    from {{ source('yuman_api', 'yuman_evs_suppliers') }}

),

cleaned_suppliers as (

    select
        id as supplier_id,
        code as supplier_code,
        name as supplier_name,
        address as supplier_address,
        vat_number as supplier_vat_number,
        active as is_active,
        created_at,
        updated_at,
        _extracted_at as extracted_at
    from source_data

)

select *
from cleaned_suppliers
