{{
    config(
        materialized='table',
        description='Types de sociétés Nayka (Oracle company_type) : client, fournisseur, dépôt, agence…'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_company_type') }}
),

cleaned_data as (
    -- idstring renvoie à la table string, non extraite.
    select
        cast(idcompany_type as int64) as idcompany_type,
        code,
        code_type,
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
