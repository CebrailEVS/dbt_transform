{{
    config(
        materialized='table',
        description='Table des collaborateur Nunshen nettoyée et transformée depuis la table source dbo_f_collaborateur de MSSQL Sage. Source désormais en colonnes plates (nouvel extracteur, overwrite) — plus de blob JSON.'
    )
}}

with source_data as (
    select *
    from {{ source('mssql_sage', 'dbo_f_collaborateur') }}
),

cleaned_data as (
    select
        -- Identifiant technique Sage (PK)
        cb_marq,

        -- Champs principaux
        co_no,
        co_nom,
        co_prenom,
        co_fonction,

        -- Metadata
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _sdc_extracted_at as extracted_at,
        _sdc_deleted_at as deleted_at

    from source_data
)

select *
from cleaned_data
qualify row_number() over (
    partition by co_no
    order by updated_at desc, cb_marq desc
) = 1
