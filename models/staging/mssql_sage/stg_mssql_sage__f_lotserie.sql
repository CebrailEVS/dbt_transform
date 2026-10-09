{{
    config(
        materialized='table',
        partition_by={
            "field": "extracted_at",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=['ar_ref', 'de_no'],
        description="Photos des lots et numéros de série Sage Nunshen (dbo_f_lotserie). Chargement append : chaque run dlt ajoute une photo complète, identifiée par extracted_at. Toutes les photos sont gardées, la sélection d'une photo par jour se fait en aval. Dans une photo, une ligne par mouvement de lot : l'entrée (dl_no_in) et, une fois sorti, la sortie (dl_no_out, 0 tant que le lot n'est pas sorti). Sage ne déclare aucune clé unique. Sage écrit 1753-01-01 pour une date vide : convertie en NULL."
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
