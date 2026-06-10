{{
    config(
        materialized='table',
        unique_key='cb_marq',
        partition_by={
            "field": "ec_date",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=['ec_no','cg_num'],
        description='Écritures comptables nettoyées issues du système MSSQL Sage (dbo_f_ecriturec). Déduplication par ec_no (unique key Sage) en gardant le cb_marq le plus récent : Sage crée parfois un nouveau cb_marq sur update au lieu de modifier la ligne existante.'
    )
}}

with source_data as (
    select *
    from {{ source('mssql_sage', 'dbo_f_ecriturec') }}
),

cleaned_data as (
    select
        -- Identifiants
        ec_no,
        ec_no_link,
        cb_marq,

        -- Journal et comptes
        jo_num,
        cg_num,
        ct_num,
        ec_intitule,

        -- Montants et sens
        coalesce(ec_sens, 0) as ec_sens,
        coalesce(ec_montant, 0.0) as ec_montant,
        coalesce(ec_montant_regle, 0.0) as ec_montant_regle,
        ec_devise,
        n_devise,

        -- Dates (colonnes désormais TIMESTAMP natifs ; placeholder Sage 1753-01-01 -> NULL)
        ec_date,
        jm_date,
        ec_jour,
        case when date(ec_echeance) = date '1753-01-01' then null else ec_echeance end as ec_echeance,
        case when date(ec_date_rappro) = date '1753-01-01' then null else ec_date_rappro end as ec_date_rappro,
        case when date(ec_date_regle) = date '1753-01-01' then null else ec_date_regle end as ec_date_regle,

        -- Métadonnées
        cb_createur,
        cb_creation_user,
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _sdc_extracted_at as extracted_at,
        _sdc_deleted_at as deleted_at

    from source_data
)

select *
from cleaned_data
qualify row_number() over (
    partition by ec_no
    order by updated_at desc, cb_marq desc
) = 1
