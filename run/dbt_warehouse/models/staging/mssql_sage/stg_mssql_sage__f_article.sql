
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_article`
      
    
    

    
    OPTIONS(
      description="""R\u00e9f\u00e9rentiel articles \u2014 dimension des lignes de documents commerciaux."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_article`
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
    );
  