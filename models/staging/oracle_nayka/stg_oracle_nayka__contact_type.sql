{{
    config(
        materialized='table',
        description='Types de contacts Nayka (Oracle contact_type).'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_contact_type') }}
),

cleaned_data as (
    -- idstring renvoie à la table string, non extraite.
    select
        cast(idcontact_type as int64) as idcontact_type,
        code,
        code_type,
        {{ paris_vers_utc('modification_date') }} as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
