-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docligne` as DBT_INTERNAL_DEST
        using (

WITH source_data AS (
    SELECT *
    FROM `evs-datastack-prod`.`prod_raw`.`dbo_f_docligne`
),

cleaned_data AS (
    SELECT
        -- Identifiant unique de la ligne
        CAST(JSON_VALUE(data, '$.DL_No') AS INT64) AS dl_no, -- PK
        CAST(JSON_VALUE(data, '$.cbCO_No') AS INT64) AS cbco_no, -- FK pour table collaborateur

        -- Champs principaux
        JSON_VALUE(data, '$.CT_Num') AS ct_num,
        JSON_VALUE(data, '$.DO_Piece') AS do_piece,
        JSON_VALUE(data, '$.DL_Design') AS dl_design,
        JSON_VALUE(data, '$.AR_Ref') AS ar_ref,

        -- Dates & montants
        TIMESTAMP(JSON_VALUE(data, '$.DO_Date')) AS do_date,
        CAST(JSON_VALUE(data, '$.DL_Qte') AS FLOAT64) AS dl_qte,
        CAST(JSON_VALUE(data, '$.DL_MontantHT') AS FLOAT64) AS dl_montant_ht,
        CAST(JSON_VALUE(data, '$.DL_MontantTTC') AS FLOAT64) AS dl_montant_ttc,
        CAST(JSON_VALUE(data, '$.DL_PrixUnitaire') AS FLOAT64) AS dl_prix_unitaire,

        -- Metadata
        TIMESTAMP(JSON_VALUE(data, '$.cbCreation')) AS created_at,
        TIMESTAMP(JSON_VALUE(data, '$.cbModification')) AS updated_at,
        _sdc_extracted_at AS extracted_at
    FROM source_data
)

SELECT *
FROM cleaned_data



WHERE
    (
        updated_at > (
            SELECT MAX(updated_at)
            FROM `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docligne`
        )
        OR updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    )

        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.dl_no = DBT_INTERNAL_DEST.dl_no))

    
    when matched then update set
        `dl_no` = DBT_INTERNAL_SOURCE.`dl_no`,`cbco_no` = DBT_INTERNAL_SOURCE.`cbco_no`,`ct_num` = DBT_INTERNAL_SOURCE.`ct_num`,`do_piece` = DBT_INTERNAL_SOURCE.`do_piece`,`dl_design` = DBT_INTERNAL_SOURCE.`dl_design`,`ar_ref` = DBT_INTERNAL_SOURCE.`ar_ref`,`do_date` = DBT_INTERNAL_SOURCE.`do_date`,`dl_qte` = DBT_INTERNAL_SOURCE.`dl_qte`,`dl_montant_ht` = DBT_INTERNAL_SOURCE.`dl_montant_ht`,`dl_montant_ttc` = DBT_INTERNAL_SOURCE.`dl_montant_ttc`,`dl_prix_unitaire` = DBT_INTERNAL_SOURCE.`dl_prix_unitaire`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`,`updated_at` = DBT_INTERNAL_SOURCE.`updated_at`,`extracted_at` = DBT_INTERNAL_SOURCE.`extracted_at`
    

    when not matched then insert
        (`dl_no`, `cbco_no`, `ct_num`, `do_piece`, `dl_design`, `ar_ref`, `do_date`, `dl_qte`, `dl_montant_ht`, `dl_montant_ttc`, `dl_prix_unitaire`, `created_at`, `updated_at`, `extracted_at`)
    values
        (`dl_no`, `cbco_no`, `ct_num`, `do_piece`, `dl_design`, `ar_ref`, `do_date`, `dl_qte`, `dl_montant_ht`, `dl_montant_ttc`, `dl_prix_unitaire`, `created_at`, `updated_at`, `extracted_at`)


    