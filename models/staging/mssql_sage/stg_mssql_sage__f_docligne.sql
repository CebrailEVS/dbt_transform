{{
    config(
        materialized='table',
        partition_by={
            "field": "do_date",
            "data_type": "timestamp",
            "granularity": "day"
        },
        description='Table des documents Sage Nunshen (ventes, achats, stock) nettoyée et transformée depuis dbo_f_docligne. Source désormais en colonnes plates (nouvel extracteur, chargement incrémental) — plus de blob JSON. Filtrer do_domaine = 0 pour ne garder que les ventes.'
    )
}}

with source_data as (
    select *
    from {{ source('mssql_sage', 'dbo_f_docligne') }}
),

cleaned_data as (
    select
        -- Identifiant unique de la ligne
        dl_no, -- PK
        cb_co_no as cbco_no, -- FK pour table collaborateur

        -- Domaine et type Sage (filtrage métier en aval)
        -- DO_Domaine : 0 = Ventes, 1 = Achats/autre, 2 = Stock interne
        do_domaine,
        do_type,

        -- Champs principaux
        ct_num,
        do_piece,
        dl_design,
        ar_ref,

        -- Dates & montants
        do_date,
        dl_qte,
        dl_montant_ht,
        dl_montant_ttc,
        dl_prix_unitaire,
        dl_valorise,

        -- Metadata
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _sdc_extracted_at as extracted_at,
        _sdc_deleted_at as deleted_at
    from source_data
)

select *
from cleaned_data
