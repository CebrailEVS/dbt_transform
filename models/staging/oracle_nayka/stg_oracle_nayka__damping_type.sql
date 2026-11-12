{{
    config(
        materialized='table'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_damping_type') }}
),

cleaned_data as (
    select
        cast(iddamping_type as int64) as iddamping_type,
        code,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
