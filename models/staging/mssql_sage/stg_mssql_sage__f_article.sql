{{
    config(
        materialized='table',
        description="Référentiel articles de Sage (dbo_f_article) : une référence par ligne, avec sa désignation. Sert de dimension aux lignes de documents commerciaux. La table source porte 134 colonnes, dont des champs métier propres à EVS (provenance, note olfactive, certifications) volontairement non exposés ici — à ajouter au besoin, un par un."
    )
}}

with source_data as (
    select *
    from {{ source('mssql_sage', 'dbo_f_article') }}
),

cleaned_data as (
    select
        -- Identifiant technique Sage (PK)
        cb_marq,

        -- Clé métier : unique dans Sage (contrainte UKA_F_ARTICLE_AR_Ref)
        ar_ref,
        ar_design,

        -- Classement
        fa_code_famille,
        ar_type,
        ar_nature,
        ar_code_barre,
        ar_substitut,

        -- Valorisation
        ar_prix_ach,
        ar_prix_ven,
        ar_prix_ttc,
        ar_coef,
        ar_cout_std,

        -- Gestion
        ar_suivi_stock,
        ar_sommeil,
        ar_publie,
        ar_nomencl,
        co_no,

        -- Metadata
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _extracted_at as extracted_at

    from source_data
)

select *
from cleaned_data
-- Défensif : le raw est un snapshot complet, donc ar_ref y est déjà unique.
qualify row_number() over (
    partition by ar_ref
    order by updated_at desc, cb_marq desc
) = 1
