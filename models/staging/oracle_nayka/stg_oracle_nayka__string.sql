{{
    config(
        materialized='table'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_string') }}
),

cleaned_data as (
    select
        cast(idstring as int64) as idstring,
        langage_code,
        text,
        context,
        var,
        comments,
        cast(cast(ihm as int64) as boolean) as is_ihm,
        {{ paris_vers_utc('modification_date') }} as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
