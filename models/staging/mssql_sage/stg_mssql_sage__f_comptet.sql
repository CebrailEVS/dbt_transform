{{
    config(
        materialized='table',
        description='Table des comptes clients Nunshen nettoyée et transformée depuis la table source dbo_f_comptet de MSSQL Sage',
    )
}}

WITH source_data AS (
    SELECT *
    FROM {{ source('mssql_sage', 'dbo_f_comptet') }}
),

cleaned_data AS (
    SELECT
        -- Champs principaux
        JSON_VALUE(data, '$.CT_Num') AS ct_num,
        JSON_VALUE(data, '$.CT_Intitule') AS ct_intitule,
        CAST(JSON_VALUE(data, '$.CT_Type') AS INT64) AS ct_type,
        JSON_VALUE(data, '$.CT_Contact') AS ct_contact,
        JSON_VALUE(data, '$.CT_Adresse') AS ct_adresse,
        JSON_VALUE(data, '$.CT_Complement') AS ct_complement,
        JSON_VALUE(data, '$.CT_CodePostal') AS ct_codepostal,
        JSON_VALUE(data, '$.CT_Ville') AS ct_ville,
        JSON_VALUE(data, '$.CT_Pays') AS ct_pays,
        JSON_VALUE(data, '$.CT_Siret') AS ct_siret,
        JSON_VALUE(data, '$.CT_NumPayeur') AS ct_numpayeur,
        CAST(JSON_VALUE(data, '$.CO_No') AS INT64) AS co_no,
        JSON_VALUE(data, '$.CT_Telephone') AS ct_telephone,
        JSON_VALUE(data, '$.CT_EMail') AS ct_email,

        -- Champs avec espaces
        JSON_VALUE(data, '$."CATEGORISATION NIV 1"') AS categorisation_niv_1,
        JSON_VALUE(data, '$."CATEGORISATION NIV 2"') AS categorisation_niv_2,
        JSON_VALUE(data, '$."CATEGORISATION NIV 3"') AS categorisation_niv_3,
        JSON_VALUE(data, '$."LIGNE DE SERVICE"') AS ligne_de_service,
        JSON_VALUE(data, '$."ANNEE ORIGINE"') AS annee_origine,
        JSON_VALUE(data, '$."CLIENT PERDU"') AS client_perdu,

        -- Champs divers
        JSON_VALUE(data, '$.TYPOLOGIE') AS typologie,

        -- Metadata
        TIMESTAMP(JSON_VALUE(data, '$.cbCreation')) AS created_at,
        TIMESTAMP(JSON_VALUE(data, '$.cbModification')) AS updated_at,
        _sdc_extracted_at AS extracted_at

    FROM source_data
)

SELECT *
FROM cleaned_data
