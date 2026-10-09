{{
    config(
        materialized='table',
        partition_by={
            "field": "extracted_at",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=['ar_ref', 'de_no'],
        description="Photos du stock Sage Nunshen par article × dépôt (dbo_f_artstock). Chargement append : chaque run dlt ajoute une photo complète, identifiée par extracted_at (une valeur par photo). Toutes les photos sont gardées, y compris un éventuel run manuel en journée ; la sélection d'une photo par jour se fait en aval. Pas de photo le week-end."
    )
}}

with source_data as (
    select *
    from {{ source('mssql_sage', 'dbo_f_artstock') }}
),

cleaned_data as (
    select
        -- Identifiant technique Sage (unique DANS une photo seulement)
        cb_marq,

        -- Grain : photo (extracted_at) × article × dépôt
        ar_ref,
        de_no,
        as_principal,

        -- Quantités (unité de l'article)
        as_qte_sto,
        as_qte_res,
        as_qte_com,
        as_qte_res_cm,
        as_qte_com_cm,
        as_qte_prepa,
        as_qte_a_controler,
        as_qte_mini,
        as_qte_maxi,

        -- Valorisation
        as_mont_sto,

        -- Emplacements
        dp_no_principal,
        dp_no_controle,
        as_mouvemente,

        -- Metadata
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _extracted_at as extracted_at

    from source_data
)

select *
from cleaned_data
