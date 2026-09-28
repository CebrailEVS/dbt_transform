{{
    config(
        materialized='table',
        description="Lots et numéros de série Sage Nunshen (dbo_f_lotserie) : une ligne par mouvement de lot — l'entrée du lot (dl_no_in) et, une fois sorti, la ligne de sortie (dl_no_out, 0 tant que le lot n'est pas sorti). Sage ne déclare aucune clé unique ; (dl_no_in, dl_no_out, ls_no_serie) l'est en pratique. Sage écrit 1753-01-01 pour une date vide : convertie en NULL. ATTENTION : données incohérentes depuis l'inventaire du 2026-09-27 (lots ouverts ≫ stock) — aucun modèle aval avant correction côté Sage."
    )
}}

with source_data as (
    select *
    from {{ source('mssql_sage', 'dbo_f_lotserie') }}
),

cleaned_data as (
    select
        -- Identifiant technique Sage (PK)
        cb_marq,

        -- Lot et article
        ar_ref,
        ls_no_serie,
        de_no,

        -- Mouvements (lignes de document)
        dl_no_in,
        dl_no_out,
        ls_mvt_stock,

        -- Quantités
        ls_qte,
        ls_qte_restant,
        ls_qte_res,
        ls_lot_epuise,

        -- Dates
        nullif(ls_fabrication, timestamp('1753-01-01')) as ls_fabrication,
        nullif(ls_peremption, timestamp('1753-01-01')) as ls_peremption,

        -- Libre
        nullif(trim(ls_complement), '') as ls_complement,

        -- Metadata
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _extracted_at as extracted_at

    from source_data
)

select *
from cleaned_data
