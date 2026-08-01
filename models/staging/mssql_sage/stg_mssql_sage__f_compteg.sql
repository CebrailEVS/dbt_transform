{{
    config(
        materialized='table',
        description="Plan comptable général de Sage (dbo_f_compteg) : un compte général par ligne, avec son intitulé. Sert de dimension aux écritures comptables et de référence au seed ref_mssql_sage__code_comptable_bu, qui n'en couvre que les comptes de classes 6 et 7."
    )
}}

with source_data as (
    select *
    from {{ source('mssql_sage', 'dbo_f_compteg') }}
),

cleaned_data as (
    select
        -- Identifiant technique Sage (PK)
        cb_marq,

        -- Clé métier : unique dans Sage (contrainte UKA_F_COMPTEG_CG_Num)
        cg_num,
        cg_intitule,

        -- Caractérisation du compte
        cg_type,
        cg_classement,
        n_nature,
        ta_code,
        cg_sommeil,
        cg_analytique,
        cg_lettrage,
        cg_tiers,

        -- Metadata
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _extracted_at as extracted_at

    from source_data
)

select *
from cleaned_data
-- Défensif : le raw est un snapshot complet, donc cg_num y est déjà unique. Le
-- qualify protège d'un passage futur en merge, comme sur les autres référentiels.
qualify row_number() over (
    partition by cg_num
    order by updated_at desc, cb_marq desc
) = 1
