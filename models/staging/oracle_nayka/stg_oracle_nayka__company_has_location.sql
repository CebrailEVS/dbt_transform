{{
    config(
        materialized='table',
        description="Rattachement des adresses aux sociétés Nayka (Oracle company_has_location) : une ligne par société × adresse × type d'adresse."
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_company_has_location') }}
),

cleaned_data as (
    select
        cast(idcompany as int64) as idcompany,
        cast(idlocation as int64) as idlocation,
        cast(idlocation_type as int64) as idlocation_type,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
