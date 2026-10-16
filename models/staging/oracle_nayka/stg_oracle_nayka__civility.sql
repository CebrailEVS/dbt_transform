{{
    config(
        materialized='table',
        description='Civilités des contacts Nayka (Oracle civility) : MME, MLLE, M.'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_civility') }}
),

cleaned_data as (
    -- idstring renvoie à la table string, non extraite.
    select
        cast(idcivility as int64) as idcivility,
        code,
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
