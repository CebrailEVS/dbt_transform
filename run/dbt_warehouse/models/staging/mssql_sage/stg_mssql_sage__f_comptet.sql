
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_comptet`
      
    
    

    
    OPTIONS(
      description="""Table des comptes clients Nunshen nettoy\u00e9e et transform\u00e9e depuis la table source dbo_f_comptet de MSSQL Sage"""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_comptet`
),

cleaned_data as (
    select
        -- Identifiant technique Sage (PK)
        cb_marq,

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
qualify row_number() over (
    partition by ct_num
    order by updated_at desc, cb_marq desc
) = 1
    );
  