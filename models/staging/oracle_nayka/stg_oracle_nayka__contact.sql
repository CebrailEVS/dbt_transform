{{
    config(
        materialized='table'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_contact') }}
),

cleaned_data as (
    select
        cast(idcontact as int64) as idcontact,
        cast(idcompany as int64) as idcompany,
        cast(idcontact_type as int64) as idcontact_type,
        cast(idcivility as int64) as idcivility,
        cast(idlocation as int64) as idlocation,
        code,
        name,
        first_name,
        last_name,
        qualite,
        email,
        mobile,
        phone,
        cast(code_status_record as int64) as code_status_record,
        cast(ismain as int64) as ismain,
        cast(idcontact_modification as int64) as idcontact_modification,
        {{ paris_vers_utc('modification_date') }} as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
