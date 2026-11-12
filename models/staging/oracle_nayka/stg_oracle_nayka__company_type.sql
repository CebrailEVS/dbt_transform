{{
    config(
        materialized='table'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_company_type') }}
),

cleaned_data as (
    select
        cast(idcompany_type as int64) as idcompany_type,
        code,
        code_type,
        {{ paris_vers_utc('modification_date') }} as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
