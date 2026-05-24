
  
    

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
        -- Champs principaux
        json_value(data, '$.CT_Num') as ct_num,
        json_value(data, '$.CT_Intitule') as ct_intitule,
        cast(json_value(data, '$.CT_Type') as int64) as ct_type,
        json_value(data, '$.CT_Contact') as ct_contact,
        json_value(data, '$.CT_Adresse') as ct_adresse,
        json_value(data, '$.CT_Complement') as ct_complement,
        json_value(data, '$.CT_CodePostal') as ct_codepostal,
        json_value(data, '$.CT_Ville') as ct_ville,
        json_value(data, '$.CT_Pays') as ct_pays,
        json_value(data, '$.CT_Siret') as ct_siret,
        json_value(data, '$.CT_NumPayeur') as ct_numpayeur,
        cast(json_value(data, '$.CO_No') as int64) as co_no,
        json_value(data, '$.CT_Telephone') as ct_telephone,
        json_value(data, '$.CT_EMail') as ct_email,

        -- Champs avec espaces
        json_value(data, '$."CATEGORISATION NIV 1"') as categorisation_niv_1,
        json_value(data, '$."CATEGORISATION NIV 2"') as categorisation_niv_2,
        json_value(data, '$."CATEGORISATION NIV 3"') as categorisation_niv_3,
        json_value(data, '$."LIGNE DE SERVICE"') as ligne_de_service,
        json_value(data, '$."ANNEE ORIGINE"') as annee_origine,
        json_value(data, '$."CLIENT PERDU"') as client_perdu,

        -- Champs divers
        json_value(data, '$.TYPOLOGIE') as typologie,

        -- Metadata
        timestamp(json_value(data, '$.cbCreation')) as created_at,
        timestamp(json_value(data, '$.cbModification')) as updated_at,
        _sdc_extracted_at as extracted_at

    from source_data
)

select *
from cleaned_data
    );
  