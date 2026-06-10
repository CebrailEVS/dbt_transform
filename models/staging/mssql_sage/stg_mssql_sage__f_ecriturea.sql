{{
    config(
        materialized='table',
        unique_key='cb_marq',
        partition_by={
            "field": "created_at",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=['ec_no'],
        description='Écritures analytiques nettoyées issues du système MSSQL Sage. Déduplication par (ec_no, n_analytique, ea_ligne) — unique key Sage — en gardant le cb_marq le plus récent : Sage crée parfois un nouveau cb_marq sur update au lieu de modifier la ligne existante.'
    )
}}

with source_data as (
    select *
    from {{ source('mssql_sage', 'dbo_f_ecriturea') }}
),

cleaned_data as (
    select
        -- IDs et numéros (colonnes désormais INT64 natifs)
        cb_marq,
        ec_no,
        n_analytique,
        ea_ligne,

        -- Colonnes texte
        ca_num,
        cb_createur,
        cb_creation_user,

        -- Colonnes numériques avec gestion des valeurs nulles
        ea_montant,
        ea_quantite,

        -- Timestamps harmonisés (colonnes désormais TIMESTAMP natifs)
        cb_creation as created_at,
        -- Fallback to cb_creation when cb_modification is null
        coalesce(cb_modification, cb_creation) as updated_at,
        _sdc_extracted_at as extracted_at,
        _sdc_deleted_at as deleted_at

    from source_data
)

select *
from cleaned_data
qualify row_number() over (
    partition by ec_no, n_analytique, ea_ligne
    order by updated_at desc, cb_marq desc
) = 1
