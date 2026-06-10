{{
    config(
        materialized='table',
        description='Table des comptes clients Nunshen nettoyée et transformée depuis la table source dbo_f_comptet de MSSQL Sage. Source désormais en colonnes plates (nouvel extracteur, overwrite) — plus de blob JSON.'
    )
}}

with source_data as (
    select *
    from {{ source('mssql_sage', 'dbo_f_comptet') }}
),

cleaned_data as (
    select
        -- Champs principaux
        ct_num,
        ct_intitule,
        ct_type,
        ct_contact,
        ct_adresse,
        ct_complement,
        ct_code_postal as ct_codepostal,
        ct_ville,
        ct_pays,
        ct_siret,
        ct_num_payeur as ct_numpayeur,
        co_no,
        ct_telephone,
        ct_e_mail as ct_email,

        -- Catégorisation métier
        categorisation_niv_1,
        categorisation_niv_2,
        categorisation_niv_3,
        ligne_de_service,
        annee_origine,
        client_perdu,
        typologie,

        -- Metadata
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _sdc_extracted_at as extracted_at,
        _sdc_deleted_at as deleted_at

    from source_data
)

select *
from cleaned_data
