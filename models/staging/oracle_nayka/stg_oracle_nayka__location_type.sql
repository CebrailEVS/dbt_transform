{{
    config(
        materialized='table',
        description="Types d'adresses Nayka (Oracle location_type) : principale, facturation, livraison, emplacement…"
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_location_type') }}
),

cleaned_data as (
    -- idstring renvoie à la table string, non extraite.
    select
        cast(idlocation_type as int64) as idlocation_type,
        code,
        code_type,
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
