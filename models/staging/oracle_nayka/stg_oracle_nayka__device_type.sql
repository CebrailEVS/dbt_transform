{{
    config(
        materialized='table',
        description="Types d'équipements Nayka (Oracle device_type) : machine, télémétrie, TPE, accessoire…"
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_device_type') }}
),

cleaned_data as (
    -- idstring renvoie à la table string, non extraite.
    select
        cast(iddevice_type as int64) as iddevice_type,
        code,
        code_type,
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
