{{
    config(
        materialized='table',
        description='Étiquettes posées sur les contrats Nayka (Oracle label_has_contract) : une ligne par contrat × étiquette.'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_label_has_contract') }}
),

cleaned_data as (
    select
        cast(idcontract as int64) as idcontract,
        cast(idlabel as int64) as idlabel,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
