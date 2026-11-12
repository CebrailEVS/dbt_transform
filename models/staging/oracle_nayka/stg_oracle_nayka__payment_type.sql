{{
    config(
        materialized='table'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_payment_type') }}
),

cleaned_data as (
    select
        cast(idpayment_type as int64) as idpayment_type,
        code,
        cast(system as int64) as system,  -- noqa: RF04
        cast(is_active as bool) as is_active,
        {{ paris_vers_utc('modification_date') }} as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
