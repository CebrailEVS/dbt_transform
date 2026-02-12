{{
    config(
        materialized='table',
        unique_key='dl_no',
        partition_by={
            "field": "do_date",
            "data_type": "timestamp",
            "granularity": "day"
        },
        description='Table des ventes Nunshen nettoyée et transformée depuis la table source dbo_f_docligne de MSSQL Sage'
    )
}}

with source_data as (
    select *
    from {{ source('mssql_sage', 'dbo_f_docligne') }}
),

cleaned_data as (
    select
        -- Identifiant unique de la ligne
        cast(json_value(data, '$.DL_No') as int64) as dl_no, -- PK
        cast(json_value(data, '$.cbCO_No') as int64) as cbco_no, -- FK pour table collaborateur

        -- Champs principaux
        json_value(data, '$.CT_Num') as ct_num,
        json_value(data, '$.DO_Piece') as do_piece,
        json_value(data, '$.DL_Design') as dl_design,
        json_value(data, '$.AR_Ref') as ar_ref,

        -- Dates & montants
        timestamp(json_value(data, '$.DO_Date')) as do_date,
        cast(json_value(data, '$.DL_Qte') as float64) as dl_qte,
        cast(json_value(data, '$.DL_MontantHT') as float64) as dl_montant_ht,
        cast(json_value(data, '$.DL_MontantTTC') as float64) as dl_montant_ttc,
        cast(json_value(data, '$.DL_PrixUnitaire') as float64) as dl_prix_unitaire,
        cast(json_value(data, '$.DL_Valorise') as int64) as dl_valorise,

        -- Metadata
        timestamp(json_value(data, '$.cbCreation')) as created_at,
        timestamp(json_value(data, '$.cbModification')) as updated_at,
        _sdc_extracted_at as extracted_at
    from source_data
)

select *
from cleaned_data


{% if is_incremental() %}
where
    (
        updated_at > (
            select max(updated_at)
            from {{ this }}
        )
        or updated_at >= timestamp_sub(current_timestamp(), interval 7 day)
    )
{% endif %}
