{{
    config(
        materialized='table',
        description="Unités de conditionnement des produits Nayka (Oracle product_unit) : unité, coefficients de conversion, code-barres."
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_product_unit') }}
),

cleaned_data as (
    -- Non reprises : colonnes jamais renseignées, idstring (table string non extraite), idinstance (constante).
    select
        cast(idproduct_unit as int64) as idproduct_unit,
        cast(idproduct as int64) as idproduct,
        cast(idunit_type as int64) as idunit_type,
        code,
        cast(base_unit as int64) as base_unit,
        coeff_multi,
        coeff_div,
        barcode,
        weight,
        volume,
        cast(isactive as int64) as isactive,
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
