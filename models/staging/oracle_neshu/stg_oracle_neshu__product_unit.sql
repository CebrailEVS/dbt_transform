{{
    config(
        materialized='table',
        description='Unités de conditionnement par produit (base, achat, stock, réception) depuis evs_product_unit. idunit_type=1 = unité d''ACHAT (conditionnement de commande fournisseur), coeff_multi/coeff_div = conversion vers l''unité de base.'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_neshu', 'evs_product_unit') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idproduct_unit as int64) as idproduct_unit,
        cast(idproduct as int64) as idproduct,
        cast(idinstance as int64) as idinstance,
        cast(idunit_type as int64) as idunit_type,
        cast(idstring as int64) as idstring,
        cast(idstandard_unit as int64) as idstandard_unit,
        cast(idcontact_modification as int64) as idcontact_modification,

        -- Colonnes texte
        code,
        barcode,

        -- Flags (passthrough : 1 = oui)
        cast(base_unit as int64) as base_unit,
        cast(isactive as int64) as isactive,

        -- Coefficients de conversion vers l'unité de base
        cast(coeff_multi as numeric) as coeff_multi,
        cast(coeff_div as numeric) as coeff_div,

        -- Caractéristiques physiques
        safe_cast(weight as float64) as weight,
        safe_cast(volume as float64) as volume,
        safe_cast(width as float64) as width,
        safe_cast(height as float64) as height,
        safe_cast(depth as float64) as depth,

        -- Timestamps harmonisés (pas de date de création en source -> created_at null)
        cast(null as timestamp) as created_at,
        timestamp(modification_date) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at

    from source_data
)

select * from cleaned_data
