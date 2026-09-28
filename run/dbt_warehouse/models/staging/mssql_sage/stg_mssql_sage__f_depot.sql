
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_depot`
      
    
    

    
    OPTIONS(
      description="""D\u00e9p\u00f4ts de stock Nunshen \u2014 dimension du stock et des lignes de documents."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_depot`
),

cleaned_data as (
    select
        -- Identifiant technique Sage (PK)
        cb_marq,

        -- Clé métier
        de_no,
        nullif(trim(de_intitule), '') as de_intitule,
        nullif(trim(de_code), '') as de_code,

        -- Adresse et contact
        nullif(trim(de_adresse), '') as de_adresse,
        nullif(trim(de_complement), '') as de_complement,
        nullif(trim(de_code_postal), '') as de_code_postal,
        nullif(trim(de_ville), '') as de_ville,
        nullif(trim(de_region), '') as de_region,
        nullif(trim(de_pays), '') as de_pays,
        nullif(trim(de_contact), '') as de_contact,
        nullif(trim(de_e_mail), '') as de_e_mail,
        nullif(trim(de_telephone), '') as de_telephone,

        -- Gestion
        de_principal,
        de_cat_compta,
        de_exclure,
        dp_no_defaut,

        -- Metadata
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _extracted_at as extracted_at

    from source_data
)

select *
from cleaned_data
    );
  