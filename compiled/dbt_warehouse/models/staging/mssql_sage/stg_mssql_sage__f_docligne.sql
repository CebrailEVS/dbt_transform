

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_docligne`
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


