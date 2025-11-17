
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_collaborateur`
      
    
    

    
    OPTIONS(
      description="""Table des collaborateur Nunshen nettoy\u00e9e et transform\u00e9e depuis la table source dbo_f_collaborateur de MSSQL Sage"""
    )
    as (
      

WITH source_data AS (
    SELECT *
    FROM `evs-datastack-prod`.`prod_raw`.`dbo_f_collaborateur`
),

cleaned_data AS (
    SELECT
        -- Champs principaux
        CAST(JSON_VALUE(data, '$.CO_No') as INT64) AS co_no,
        JSON_VALUE(data, '$.CO_Nom') AS co_nom,
        JSON_VALUE(data, '$.CO_Prenom') AS co_prenom,
        JSON_VALUE(data, '$.CO_Fonction') AS co_fonction,

        -- Metadata
        TIMESTAMP(JSON_VALUE(data, '$.cbCreation')) AS created_at,
        TIMESTAMP(JSON_VALUE(data, '$.cbModification')) AS updated_at,
        _sdc_extracted_at AS extracted_at

    FROM source_data
)

SELECT *
FROM cleaned_data
    );
  