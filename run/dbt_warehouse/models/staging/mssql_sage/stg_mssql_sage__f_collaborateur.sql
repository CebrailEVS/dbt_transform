
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_collaborateur`
      
    
    

    
    OPTIONS(
      description="""Table des collaborateur Nunshen nettoy\u00e9e et transform\u00e9e depuis la table source dbo_f_collaborateur de MSSQL Sage"""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_collaborateur`
),

cleaned_data as (
    select
        -- Champs principaux
        cast(json_value(data, '$.CO_No') as int64) as co_no,
        json_value(data, '$.CO_Nom') as co_nom,
        json_value(data, '$.CO_Prenom') as co_prenom,
        json_value(data, '$.CO_Fonction') as co_fonction,

        -- Metadata
        timestamp(json_value(data, '$.cbCreation')) as created_at,
        timestamp(json_value(data, '$.cbModification')) as updated_at,
        _sdc_extracted_at as extracted_at

    from source_data
)

select *
from cleaned_data
    );
  