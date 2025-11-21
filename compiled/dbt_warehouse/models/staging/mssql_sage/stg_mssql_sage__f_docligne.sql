

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


