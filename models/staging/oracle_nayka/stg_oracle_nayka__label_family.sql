{{
    config(
        materialized='table'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_label_family') }}
),

cleaned_data as (
    select
        cast(idlabel_family as int64) as idlabel_family,
        cast(idstring as int64) as idstring,
        code,
        cast(exclus as int64) as exclus,
        cast(required as int64) as required,
        cast(system as int64) as system,  -- noqa: RF04
        {{ paris_vers_utc('modification_date') }} as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
