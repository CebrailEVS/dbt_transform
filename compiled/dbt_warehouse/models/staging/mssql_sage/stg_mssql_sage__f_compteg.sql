

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_compteg`
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