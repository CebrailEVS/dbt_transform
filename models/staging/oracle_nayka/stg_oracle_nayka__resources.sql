{{
    config(
        materialized='table'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_resources') }}
),

cleaned_data as (
    select
        cast(idresources as int64) as idresources,
        cast(idresources_type as int64) as idresources_type,
        cast(idcontact as int64) as idcontact,
        cast(idcompany as int64) as idcompany,
        cast(idcompany_storehouse as int64) as idcompany_storehouse,
        cast(idcontact_creation as int64) as idcontact_creation,
        cast(idcontact_modification as int64) as idcontact_modification,
        code,
        name,
        cast(code_status_record as int64) as code_status_record,
        cast(dtl_hidden as int64) as dtl_hidden,
        cast(base_rate as numeric) as base_rate,
        {{ paris_vers_utc('arrival') }} as arrival,
        {{ paris_vers_utc('creation_date') }} as created_at,
        {{ paris_vers_utc('coalesce(modification_date, creation_date)') }} as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
