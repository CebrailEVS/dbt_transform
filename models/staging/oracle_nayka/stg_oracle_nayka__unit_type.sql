{{
    config(
        materialized='table',
        description="Types d'unités de produit Nayka (Oracle unit_type) : achat, vente, stock, réappro…"
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_unit_type') }}
),

cleaned_data as (
    -- idstring renvoie à la table string, non extraite.
    select
        cast(idunit_type as int64) as idunit_type,
        code,
        code_type,
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
