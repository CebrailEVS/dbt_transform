{{
    config(
        materialized='table',
        description="Devises Nayka (Oracle currency) : référentiel ISO 4217 livré avec l'ERP, seul EUR est actif."
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_currency') }}
),

cleaned_data as (
    -- Retirées : idstring (table string non extraite), comments (notes sur l'affichage des symboles).
    -- Pas de date de modification à la source.
    select
        cast(idcurrency as int64) as idcurrency,
        code,
        symbole,
        cast(isactif as int64) as isactif,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
